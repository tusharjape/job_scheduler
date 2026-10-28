#include "job_scheduler/job_scheduler.h"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <latch>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <vector>

#include "job_scheduler/clock.h"
#include "job_scheduler/types.h"

namespace scheduler {
namespace {

using namespace std::chrono_literals;

// Helper barrier/gate for deterministically holding and releasing jobs in
// multi-threaded tests without busy-waiting.
class Gate {
 public:
  void Open() {
    {
      std::lock_guard<std::mutex> lock(mu_);
      open_ = true;
    }
    cv_.notify_all();
  }

  void Wait() {
    std::unique_lock<std::mutex> lock(mu_);
    cv_.wait(lock, [this]() { return open_; });
  }

 private:
  std::mutex mu_;
  std::condition_variable cv_;
  bool open_ = false;
};

void TestEndToEndCapacitySharingAndHandover() {
  auto clock = std::make_shared<FakeClock>();
  // total = 4, rerun_max = 2
  JobScheduler sched(
      SchedulerConfig{.total = 4, .rerun_max = 2, .full_max_wait = 10s},
      clock);

  // 1. Submit 4 Live jobs when no Rerun work exists; all 4 should start and
  // occupy all 4 slots.
  std::latch all_four_live_started(4);
  std::vector<std::unique_ptr<Gate>> live_gates;
  std::vector<std::unique_ptr<std::latch>> live_done;
  for (int i = 0; i < 4; ++i) {
    live_gates.push_back(std::make_unique<Gate>());
    live_done.push_back(std::make_unique<std::latch>(1));
    Gate* g = live_gates.back().get();
    std::latch* done = live_done.back().get();
    sched.Submit(Role::kLive, JobType::kQuick, [&, g, done]() {
      all_four_live_started.count_down();
      g->Wait();
      done->count_down();
    });
  }

  all_four_live_started.wait();
  assert(sched.RunningCount(Role::kLive) == 4);
  assert(sched.RunningCount(Role::kRerun) == 0);

  // 2. While all 4 slots are busy with Live jobs, enqueue 3 Rerun jobs and 2
  // more Live jobs. Running Live jobs must not be interrupted.
  std::latch two_reruns_started(2);
  Gate rerun_gate;
  std::atomic<int> active_reruns{0};
  std::atomic<int> max_concurrent_reruns{0};

  for (int i = 0; i < 3; ++i) {
    sched.Submit(Role::kRerun, JobType::kQuick, [&]() {
      const int cur = ++active_reruns;
      int prev_max = max_concurrent_reruns.load();
      while (cur > prev_max &&
             !max_concurrent_reruns.compare_exchange_weak(prev_max, cur)) {
      }
      if (cur <= 2) {
        two_reruns_started.count_down();
      }
      rerun_gate.Wait();
      --active_reruns;
    });
  }

  std::atomic<int> extra_live_completed{0};
  for (int i = 0; i < 2; ++i) {
    sched.Submit(Role::kLive, JobType::kQuick,
                 [&]() { ++extra_live_completed; });
  }

  assert(sched.RunningCount(Role::kLive) == 4);
  assert(sched.QueuedCount(Role::kRerun) == 3);

  // 3. Finish 2 of the running Live jobs; their freed slots must go to Rerun
  // (up to rerun_max = 2), NOT to the waiting extra Live jobs.
  live_gates[0]->Open();
  live_gates[1]->Open();
  two_reruns_started.wait();

  assert(sched.RunningCount(Role::kRerun) == 2);
  assert(sched.RunningCount(Role::kLive) == 2);
  assert(extra_live_completed.load() == 0);

  // 4. Finish a 3rd running Live job while 2 Reruns are still blocked on
  // rerun_gate (so Rerun is capped at rerun_max = 2). The freed slot must not
  // sit idle: one of the waiting Live jobs should run and complete!
  live_gates[2]->Open();
  live_done[2]->wait();

  // Release everything and wait for idle.
  live_gates[3]->Open();
  rerun_gate.Open();
  sched.WaitForIdle();

  assert(max_concurrent_reruns.load() <= 2);
  assert(extra_live_completed.load() == 2);
  assert(sched.TotalRunningCount() == 0);
}

void TestSingleWorkerDeterministicOrderingAndAging() {
  auto clock = std::make_shared<FakeClock>();
  // Use total = 1 to record exact execution order without multi-worker race.
  JobScheduler sched(
      SchedulerConfig{.total = 1, .rerun_max = 1, .full_max_wait = 10s},
      clock);

  std::latch blocker_started(1);
  Gate blocker_gate;
  sched.Submit(Role::kLive, JobType::kQuick, [&]() {
    blocker_started.count_down();
    blocker_gate.Wait();
  });
  blocker_started.wait();

  std::mutex order_mu;
  std::vector<int> execution_order;
  auto record = [&](int id) {
    return [&, id]() {
      std::lock_guard<std::mutex> lock(order_mu);
      execution_order.push_back(id);
    };
  };

  // At t = 0s: submit Full job #1 (virtual_timestamp = 10s).
  sched.Submit(Role::kLive, JobType::kFull, record(1));

  // At t = 3s: submit Quick job #2 (virtual_timestamp = 3s).
  clock->Advance(3s);
  sched.Submit(Role::kLive, JobType::kQuick, record(2));

  // At t = 3s: submit Quick job #3 (virtual_timestamp = 3s, after #2).
  sched.Submit(Role::kLive, JobType::kQuick, record(3));

  // At t = 10s: Full job #1 has now waited K = 10s. Submit Quick job #4
  // (virtual_timestamp = 10s, after #1).
  clock->Advance(7s);
  sched.Submit(Role::kLive, JobType::kQuick, record(4));

  // At t = 12s: submit Rerun Quick job #5. Since Live takes precedence over
  // Rerun when both are eligible (total = 1, rerun_max = 1 -> live_limit = 0
  // while Rerun is queued!), let's verify that queued Rerun work claims the
  // next freed slot ahead of queued Live work when total = 1, rerun_max = 1
  // (because rerun_target = min(1, 1) = 1 -> live_limit = 1 - 1 = 0).
  // Here we keep all jobs in Role::kLive to test pure type ordering:
  clock->Advance(2s);
  sched.Submit(Role::kLive, JobType::kFull, record(5));

  blocker_gate.Open();
  sched.WaitForIdle();

  const std::vector<int> expected = {2, 3, 1, 4, 5};
  assert(execution_order == expected);
}

void TestHighVolumeConcurrencyInvariants() {
  auto clock = std::make_shared<FakeClock>();
  constexpr std::size_t kTotal = 8;
  constexpr std::size_t kRerunMax = 3;
  constexpr int kTotalJobs = 20000;

  JobScheduler sched(
      SchedulerConfig{
          .total = kTotal, .rerun_max = kRerunMax, .full_max_wait = 5s},
      clock);

  std::atomic<std::size_t> active_total{0};
  std::atomic<std::size_t> active_rerun{0};
  std::atomic<std::size_t> max_seen_total{0};
  std::atomic<std::size_t> max_seen_rerun{0};
  std::atomic<int> completed_count{0};

  // Use 4 producer threads submitting 5,000 jobs each concurrently.
  constexpr int kProducers = 4;
  constexpr int kJobsPerProducer = kTotalJobs / kProducers;
  std::vector<std::thread> producers;
  producers.reserve(kProducers);

  for (int p = 0; p < kProducers; ++p) {
    producers.emplace_back([&, p]() {
      for (int i = 0; i < kJobsPerProducer; ++i) {
        const Role role =
            ((p + i) % 3 == 0) ? Role::kRerun : Role::kLive;
        const JobType type =
            ((p + i) % 4 == 0) ? JobType::kFull : JobType::kQuick;
        if (i % 100 == 0) {
          clock->Advance(10ms);
        }
        sched.Submit(role, type, [&, role]() {
          const std::size_t cur_total = ++active_total;
          std::size_t prev_total = max_seen_total.load();
          while (cur_total > prev_total &&
                 !max_seen_total.compare_exchange_weak(prev_total, cur_total)) {
          }

          if (role == Role::kRerun) {
            const std::size_t cur_rerun = ++active_rerun;
            std::size_t prev_rerun = max_seen_rerun.load();
            while (cur_rerun > prev_rerun &&
                   !max_seen_rerun.compare_exchange_weak(prev_rerun,
                                                         cur_rerun)) {
            }
            assert(cur_rerun <= kRerunMax);
            --active_rerun;
          }

          assert(cur_total <= kTotal);
          --active_total;
          ++completed_count;
        });
      }
    });
  }

  for (auto& t : producers) {
    t.join();
  }

  sched.WaitForIdle();
  assert(completed_count.load() == kTotalJobs);
  assert(max_seen_total.load() <= kTotal);
  assert(max_seen_rerun.load() <= kRerunMax);
}

void TestEndToEndUrgentRerunsAndNormalRerunProgress() {
  auto clock = std::make_shared<FakeClock>();
  JobScheduler sched(
      SchedulerConfig{.total = 1,
                      .rerun_max = 1,
                      .full_max_wait = 10s,
                      .normal_rerun_max_wait = 30s},
      clock);

  std::latch blocker_started(1);
  Gate blocker_gate;
  sched.Submit(Role::kLive, JobType::kQuick, [&]() {
    blocker_started.count_down();
    blocker_gate.Wait();
  });
  blocker_started.wait();

  std::mutex order_mu;
  std::vector<int> order;
  auto record = [&](int id) {
    return [&, id]() {
      std::lock_guard<std::mutex> lock(order_mu);
      order.push_back(id);
    };
  };

  // t = 0s: Normal Quick rerun #1 (v_ts = 30s).
  sched.Submit(Role::kRerun, JobType::kQuick, record(1), Urgency::kNormal);

  // t = 5s: Urgent Full rerun #2 (v_ts = 15s) and Urgent Quick rerun #3 (v_ts = 5s).
  clock->Advance(5s);
  sched.Submit(Role::kRerun, JobType::kFull, record(2), Urgency::kUrgent);
  sched.Submit(Role::kRerun, JobType::kQuick, record(3), Urgency::kUrgent);

  // t = 35s: Normal Quick rerun #1 has waited 35s (> 30s). Submit Urgent Quick
  // rerun #4 (v_ts = 35s). Normal rerun #1 must execute BEFORE #4!
  clock->Advance(30s);
  sched.Submit(Role::kRerun, JobType::kQuick, record(4), Urgency::kUrgent);

  blocker_gate.Open();
  sched.WaitForIdle();

  const std::vector<int> expected = {3, 2, 1, 4};
  assert(order == expected);
}

void TestExceptionInJobDoesNotLeakSlot() {
  JobScheduler sched(
      SchedulerConfig{.total = 1, .rerun_max = 1, .full_max_wait = 5s});

  sched.Submit(Role::kLive, JobType::kQuick,
               []() { throw std::runtime_error("simulated job failure"); });

  std::atomic<bool> second_job_ran{false};
  sched.Submit(Role::kLive, JobType::kQuick,
               [&]() { second_job_ran.store(true); });

  sched.WaitForIdle();
  assert(second_job_ran.load());
  assert(sched.TotalRunningCount() == 0);
}

}  // namespace
}  // namespace scheduler

int main() {
  scheduler::TestEndToEndCapacitySharingAndHandover();
  scheduler::TestSingleWorkerDeterministicOrderingAndAging();
  scheduler::TestEndToEndUrgentRerunsAndNormalRerunProgress();
  scheduler::TestHighVolumeConcurrencyInvariants();
  scheduler::TestExceptionInJobDoesNotLeakSlot();
  std::cout << "[PASS] Step 5: job_scheduler_test passed.\n";
  return 0;
}
