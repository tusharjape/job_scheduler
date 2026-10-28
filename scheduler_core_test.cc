#include "job_scheduler/scheduler_core.h"

#include <atomic>
#include <cassert>
#include <chrono>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <thread>
#include <vector>

#include "job_scheduler/clock.h"
#include "job_scheduler/types.h"

namespace scheduler {
namespace {

using namespace std::chrono_literals;

void TestDynamicSlotSharingAndHandover() {
  auto clock = std::make_shared<FakeClock>();
  SchedulerCore core(
      SchedulerConfig{.total = 3, .rerun_max = 2, .full_max_wait = 10s}, clock);

  // Submit 5 Live jobs while no Rerun work exists.
  for (int i = 0; i < 5; ++i) {
    core.Submit(Role::kLive, JobType::kQuick, []() {});
  }

  // Live can claim all 3 slots when no Rerun work exists.
  auto j1 = core.TryAcquireNext();
  auto j2 = core.TryAcquireNext();
  auto j3 = core.TryAcquireNext();
  assert(j1.has_value() && j1->seq == 1);
  assert(j2.has_value() && j2->seq == 2);
  assert(j3.has_value() && j3->seq == 3);
  assert(!core.TryAcquireNext().has_value());
  assert(core.RunningCount(Role::kLive) == 3);
  assert(core.QueuedCount(Role::kLive) == 2);

  // Now 2 Rerun jobs arrive (one Full, one Quick).
  const auto r_full = core.Submit(Role::kRerun, JobType::kFull, []() {});
  const auto r_quick = core.Submit(Role::kRerun, JobType::kQuick, []() {});

  // As Live jobs finish, freed slots must go to Rerun until Rerun has 2 slots,
  // and within Rerun, Quick runs before Full.
  core.Complete(Role::kLive);
  auto next1 = core.TryAcquireNext();
  assert(next1.has_value());
  assert(next1->role == Role::kRerun);
  assert(next1->seq == r_quick);

  core.Complete(Role::kLive);
  auto next2 = core.TryAcquireNext();
  assert(next2.has_value());
  assert(next2->role == Role::kRerun);
  assert(next2->seq == r_full);

  assert(core.RunningCount(Role::kLive) == 1);
  assert(core.RunningCount(Role::kRerun) == 2);

  // When a Rerun job finishes and no more Rerun work is queued, Live reclaims
  // the freed slot.
  core.Complete(Role::kRerun);
  auto next3 = core.TryAcquireNext();
  assert(next3.has_value());
  assert(next3->role == Role::kLive);
  assert(next3->seq == 4);

  core.Complete(Role::kRerun);
  auto next4 = core.TryAcquireNext();
  assert(next4.has_value());
  assert(next4->role == Role::kLive);
  assert(next4->seq == 5);

  // Clean up remaining running jobs.
  core.Complete(Role::kLive);
  core.Complete(Role::kLive);
  core.Complete(Role::kLive);
  assert(core.TotalRunningCount() == 0);
}

void TestAgingAcrossRolesWithFakeClock() {
  auto clock = std::make_shared<FakeClock>();
  SchedulerCore core(
      SchedulerConfig{.total = 1, .rerun_max = 1, .full_max_wait = 10s}, clock);

  // Occupy the single slot so subsequent submissions wait in queue.
  core.Submit(Role::kLive, JobType::kQuick, []() {});
  auto initial = core.TryAcquireNext();
  assert(initial.has_value() && initial->seq == 1);

  // At t = 0s, submit a Live Full job (virtual_timestamp = 10s).
  const auto full_seq = core.Submit(Role::kLive, JobType::kFull, []() {});

  // At t = 4s, submit a Live Quick job (virtual_timestamp = 4s).
  clock->Advance(4s);
  const auto early_quick_seq =
      core.Submit(Role::kLive, JobType::kQuick, []() {});

  // At t = 11s (after Full has waited 11s > 10s), submit another Live Quick job
  // (virtual_timestamp = 11s).
  clock->Advance(7s);
  const auto late_quick_seq =
      core.Submit(Role::kLive, JobType::kQuick, []() {});

  // Free the slot one by one and verify dispatch order:
  // early_quick (4s) -> full (10s) -> late_quick (11s).
  core.Complete(Role::kLive);
  auto d1 = core.TryAcquireNext();
  assert(d1.has_value() && d1->seq == early_quick_seq);

  core.Complete(Role::kLive);
  auto d2 = core.TryAcquireNext();
  assert(d2.has_value() && d2->seq == full_seq);

  core.Complete(Role::kLive);
  auto d3 = core.TryAcquireNext();
  assert(d3.has_value() && d3->seq == late_quick_seq);

  core.Complete(Role::kLive);
  assert(core.TotalRunningCount() == 0);
}

void TestUrgentAndNormalRerunsInSchedulerCore() {
  auto clock = std::make_shared<FakeClock>();
  SchedulerCore core(
      SchedulerConfig{.total = 2,
                      .rerun_max = 1,
                      .full_max_wait = 10s,
                      .normal_rerun_max_wait = 20s},
      clock);

  // Occupy both slots with Live jobs when no Rerun work exists.
  core.Submit(Role::kLive, JobType::kQuick, []() {});
  core.Submit(Role::kLive, JobType::kQuick, []() {});
  assert(core.TryAcquireNext().has_value());
  assert(core.TryAcquireNext().has_value());

  // At t = 0s: submit a Normal Quick rerun (v_ts = 20s).
  const auto normal_rerun_seq =
      core.Submit(Role::kRerun, JobType::kQuick, []() {}, Urgency::kNormal);

  // At t = 5s: submit an Urgent Quick rerun (v_ts = 5s).
  clock->Advance(5s);
  const auto urgent_early_seq =
      core.Submit(Role::kRerun, JobType::kQuick, []() {}, Urgency::kUrgent);

  // At t = 25s (after Normal Quick rerun has waited 25s > 20s): submit another
  // Urgent Quick rerun (v_ts = 25s).
  clock->Advance(20s);
  const auto urgent_late_seq =
      core.Submit(Role::kRerun, JobType::kQuick, []() {}, Urgency::kUrgent);

  // One Live job completes; freed slot goes to Rerun.
  // Urgent rerun @ 5s starts first.
  core.Complete(Role::kLive);
  auto r1 = core.TryAcquireNext();
  assert(r1.has_value() && r1->seq == urgent_early_seq);

  // When that Urgent rerun completes, the aged Normal rerun (v_ts = 20s) runs
  // BEFORE the newly arrived Urgent rerun @ 25s!
  core.Complete(Role::kRerun);
  auto r2 = core.TryAcquireNext();
  assert(r2.has_value() && r2->seq == normal_rerun_seq);

  // Finally, the late Urgent rerun @ 25s runs.
  core.Complete(Role::kRerun);
  auto r3 = core.TryAcquireNext();
  assert(r3.has_value() && r3->seq == urgent_late_seq);

  core.Complete(Role::kRerun);
  core.Complete(Role::kLive);
  assert(core.TotalRunningCount() == 0);
}

void TestBlockingAcquireWakeupAndShutdown() {
  auto clock = std::make_shared<FakeClock>();
  SchedulerCore core(
      SchedulerConfig{.total = 1, .rerun_max = 1, .full_max_wait = 5s}, clock);

  std::atomic<bool> worker_acquired{false};
  std::atomic<std::uint64_t> acquired_seq{0};

  // Start a worker thread that blocks on AcquireNext() while queue is empty.
  std::thread worker([&]() {
    auto job = core.AcquireNext();
    assert(job.has_value());
    acquired_seq.store(job->seq);
    worker_acquired.store(true);
    job->fn();
    core.Complete(job->role);

    // Next call to AcquireNext() should return nullopt once Shutdown() is
    // called.
    auto after_shutdown = core.AcquireNext();
    assert(!after_shutdown.has_value());
  });

  // Submit a job; worker should wake up without polling and execute it.
  std::atomic<bool> callback_ran{false};
  const auto seq = core.Submit(Role::kLive, JobType::kQuick,
                               [&]() { callback_ran.store(true); });

  core.WaitForIdle();
  assert(worker_acquired.load());
  assert(acquired_seq.load() == seq);
  assert(callback_ran.load());

  core.Shutdown();
  worker.join();

  // Submitting after Shutdown() must throw.
  bool threw_after_shutdown = false;
  try {
    core.Submit(Role::kLive, JobType::kQuick, []() {});
  } catch (const std::runtime_error&) {
    threw_after_shutdown = true;
  }
  assert(threw_after_shutdown);
}

}  // namespace
}  // namespace scheduler

int main() {
  scheduler::TestDynamicSlotSharingAndHandover();
  scheduler::TestAgingAcrossRolesWithFakeClock();
  scheduler::TestUrgentAndNormalRerunsInSchedulerCore();
  scheduler::TestBlockingAcquireWakeupAndShutdown();
  std::cout << "[PASS] Step 4: scheduler_core_test passed.\n";
  return 0;
}
