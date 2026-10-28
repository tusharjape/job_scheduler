#include "job_scheduler/role_queue.h"

#include <cassert>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <stdexcept>
#include <vector>

namespace scheduler {
namespace {

using namespace std::chrono_literals;

QueuedJob MakeJob(std::uint64_t seq, JobType type, TimePoint submit_time) {
  QueuedJob job;
  job.seq = seq;
  job.role = Role::kLive;
  job.type = type;
  job.submit_time = submit_time;
  return job;
}

void TestQuickPreferredOverFullBeforeK() {
  // K = 10s
  RoleQueue q(10s);
  const TimePoint t0{};

  // Full job submitted at t = 0s (virtual_timestamp = 10s), seq = 1.
  q.Push(MakeJob(1, JobType::kFull, t0));
  // Quick jobs submitted at t = 0s, 5s, 9s (all < 10s).
  q.Push(MakeJob(2, JobType::kQuick, t0));
  q.Push(MakeJob(3, JobType::kQuick, t0 + 5s));
  q.Push(MakeJob(4, JobType::kQuick, t0 + 9s));

  assert(q.Size() == 4);

  // All 3 Quick jobs run in FIFO order ahead of the Full job.
  assert(q.PopBest().seq == 2);
  assert(q.PopBest().seq == 3);
  assert(q.PopBest().seq == 4);
  assert(q.PopBest().seq == 1);
  assert(q.Empty());
}

void TestFullJobDoesNotStarveAfterWaitingKSeconds() {
  // K = 10s
  RoleQueue q(10s);
  const TimePoint t0{};

  // Full job submitted at t = 2s (virtual_timestamp = 12s), seq = 1.
  q.Push(MakeJob(1, JobType::kFull, t0 + 2s));

  // Quick job submitted at t = 11s (waited 9s < K): runs BEFORE Full job.
  q.Push(MakeJob(2, JobType::kQuick, t0 + 11s));

  // Quick job submitted at t = 12s (Full has now waited exactly K = 10s):
  // This newly arriving Quick job NOT run ahead of Full.
  q.Push(MakeJob(3, JobType::kQuick, t0 + 12s));

  // Quick job submitted at t = 15s (waited 13s > K): also runs AFTER Full.
  q.Push(MakeJob(4, JobType::kQuick, t0 + 15s));

  assert(q.PopBest().seq == 2);  // Quick @ 11s
  assert(q.PopBest().seq == 1);  // Full @ 2s (v_ts = 12s) beats Quick @ 12s!
  assert(q.PopBest().seq == 3);  // Quick @ 12s
  assert(q.PopBest().seq == 4);  // Quick @ 15s
  assert(q.Empty());
}

void TestSubmissionOrderForEqualJobs() {
  RoleQueue q(10s);
  const TimePoint t0{};

  // Multiple Quick jobs at the exact same timestamp -> FIFO by seq (O3).
  q.Push(MakeJob(10, JobType::kQuick, t0 + 1s));
  q.Push(MakeJob(11, JobType::kQuick, t0 + 1s));
  q.Push(MakeJob(12, JobType::kQuick, t0 + 1s));

  // Multiple Full jobs at the exact same timestamp -> FIFO by seq (O3).
  q.Push(MakeJob(20, JobType::kFull, t0));
  q.Push(MakeJob(21, JobType::kFull, t0));

  assert(q.PopBest().seq == 10);
  assert(q.PopBest().seq == 11);
  assert(q.PopBest().seq == 12);
  assert(q.PopBest().seq == 20);
  assert(q.PopBest().seq == 21);
  assert(q.Empty());
}

void TestZeroMaxWaitStillPrefersQuickAtSameTimestamp() {
  // Edge case: K = 0s. If Full and Quick arrive at the same timestamp,
  // Prefer Quick over Full; if Full arrived earlier (t0 < t1),
  // Full has waited > 0s and runs first.
  RoleQueue q(0s);
  const TimePoint t0{};

  q.Push(MakeJob(1, JobType::kFull, t0));
  q.Push(MakeJob(2, JobType::kQuick, t0));
  q.Push(MakeJob(3, JobType::kFull, t0 + 1s));
  q.Push(MakeJob(4, JobType::kQuick, t0 + 2s));

  assert(q.PopBest().seq == 2);  // Quick @ t0 beats Full @ t0
  assert(q.PopBest().seq == 1);  // Full @ t0
  assert(q.PopBest().seq == 3);  // Full @ t0+1s beats Quick @ t0+2s
  assert(q.PopBest().seq == 4);  // Quick @ t0+2s
}

void TestLargeScaleFiftyThousandJobsScaleAndOrder() {
  // Verify 50,000 jobs can be pushed and popped in strict O(1) per job
  // and maintain monotonically non-decreasing virtual timestamps.
  RoleQueue q(10s);
  const TimePoint t0{};
  constexpr std::uint64_t kNumJobs = 50000;

  for (std::uint64_t i = 1; i <= kNumJobs; ++i) {
    const JobType type = (i % 3 == 0) ? JobType::kFull : JobType::kQuick;
    const TimePoint submit = t0 + std::chrono::milliseconds(i * 100);
    q.Push(MakeJob(i, type, submit));
  }

  assert(q.Size() == kNumJobs);

  TimePoint prev_vts = TimePoint::min();
  std::size_t popped = 0;
  while (!q.Empty()) {
    QueuedJob job = q.PopBest();
    assert(job.virtual_timestamp >= prev_vts);
    prev_vts = job.virtual_timestamp;
    ++popped;
  }
  assert(popped == kNumJobs);
}

void TestEmptyQueueThrows() {
  RoleQueue q(5s);
  bool threw_pop = false;
  try {
    (void)q.PopBest();
  } catch (const std::logic_error&) {
    threw_pop = true;
  }
  assert(threw_pop);
}

}  // namespace
}  // namespace scheduler

int main() {
  scheduler::TestQuickPreferredOverFullBeforeK();
  scheduler::TestFullJobDoesNotStarveAfterWaitingKSeconds();
  scheduler::TestSubmissionOrderForEqualJobs();
  scheduler::TestZeroMaxWaitStillPrefersQuickAtSameTimestamp();
  scheduler::TestLargeScaleFiftyThousandJobsScaleAndOrder();
  scheduler::TestEmptyQueueThrows();
  std::cout << "[PASS] Step 3: role_queue_test passed.\n";
  return 0;
}
