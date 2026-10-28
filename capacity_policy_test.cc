#include "job_scheduler/capacity_policy.h"

#include <cassert>
#include <iostream>
#include <stdexcept>

namespace scheduler {
namespace {

void TestLiveUsesAllSlotsWhenNoRerunWork() {
  // total = 4, rerun_max = 2
  CapacityPolicy cap(4, 2);

  // With 0 rerun jobs waiting, Live can scale all the way to total (4).
  for (std::size_t i = 0; i < 4; ++i) {
    assert(cap.CanRunLive(/*queued_live=*/5, /*queued_rerun=*/0));
    assert(!cap.CanRunRerun(/*queued_rerun=*/0));
    assert(cap.ChooseNextRole(/*queued_live=*/5, /*queued_rerun=*/0) ==
           Role::kLive);
    cap.OnStart(Role::kLive);
  }

  // Once 4 Live jobs are running, no more jobs can start.
  assert(cap.running_live() == 4);
  assert(cap.running_total() == 4);
  assert(!cap.CanRunLive(/*queued_live=*/1, /*queued_rerun=*/0));
  assert(!cap.CanRunRerun(/*queued_rerun=*/1));
  assert(!cap.ChooseNextRole(/*queued_live=*/1, /*queued_rerun=*/1).has_value());
}

void TestLiveYieldsFreedSlotsToRerunUpToRerunMax() {
  // total = 4, rerun_max = 2
  CapacityPolicy cap(4, 2);

  // Start 4 Live jobs when no Rerun work exists.
  for (int i = 0; i < 4; ++i) {
    cap.OnStart(Role::kLive);
  }

  // Now 3 Rerun jobs and 10 Live jobs are waiting.
  // Running Live jobs are not interrupted (running_live == 4).
  // When 1 Live job finishes (running_live = 3, running_rerun = 0):
  // rerun_target = min(2, 0 + 3) = 2 -> live_limit = 4 - 2 = 2.
  // Since running_live (3) >= live_limit (2), Live CANNOT take this freed slot!
  cap.OnComplete(Role::kLive);
  assert(cap.running_live() == 3);
  assert(!cap.CanRunLive(/*queued_live=*/10, /*queued_rerun=*/3));
  assert(cap.CanRunRerun(/*queued_rerun=*/3));
  assert(cap.ChooseNextRole(/*queued_live=*/10, /*queued_rerun=*/3) ==
         Role::kRerun);

  // Rerun claims the first freed slot (running_live = 3, running_rerun = 1).
  cap.OnStart(Role::kRerun);
  assert(cap.running_total() == 4);

  // Second Live job finishes (running_live = 2, running_rerun = 1):
  // rerun_target = min(2, 1 + 2) = 2 -> live_limit = 2.
  // running_live (2) is not < live_limit (2), so Live still cannot start;
  // Rerun claims the second freed slot!
  cap.OnComplete(Role::kLive);
  assert(!cap.CanRunLive(/*queued_live=*/10, /*queued_rerun=*/2));
  assert(cap.CanRunRerun(/*queued_rerun=*/2));
  assert(cap.ChooseNextRole(/*queued_live=*/10, /*queued_rerun=*/2) ==
         Role::kRerun);

  cap.OnStart(Role::kRerun);
  assert(cap.running_live() == 2);
  assert(cap.running_rerun() == 2);

  // Third Live job finishes (running_live = 1, running_rerun = 2):
  // Now Rerun is at rerun_max (2), so even though 1 Rerun job is still queued,
  // 3rd Rerun job is forbidden, and Live can use the slot!
  cap.OnComplete(Role::kLive);
  assert(!cap.CanRunRerun(/*queued_rerun=*/1));
  assert(cap.CanRunLive(/*queued_live=*/10, /*queued_rerun=*/1));
  assert(cap.ChooseNextRole(/*queued_live=*/10, /*queued_rerun=*/1) ==
         Role::kLive);

  cap.OnStart(Role::kLive);
  assert(cap.running_live() == 2);
  assert(cap.running_rerun() == 2);
}

void TestPartialRerunDemandLeavesRemainingCapacityForLive() {
  // total = 4, rerun_max = 2
  CapacityPolicy cap(4, 2);

  // Suppose only 1 Rerun job is running and 0 Rerun jobs are waiting.
  // rerun_target = min(2, 1 + 0) = 1 -> live_limit = 4 - 1 = 3.
  // Live should be allowed to use 3 slots (not restricted to 2!).
  cap.OnStart(Role::kRerun);
  for (int i = 0; i < 3; ++i) {
    assert(cap.CanRunLive(/*queued_live=*/5, /*queued_rerun=*/0));
    cap.OnStart(Role::kLive);
  }
  assert(cap.running_live() == 3);
  assert(cap.running_rerun() == 1);
  assert(cap.running_total() == 4);

  // Once that 1 Rerun job completes and no Rerun jobs are queued, Live gets
  // the 4th slot back (C4).
  cap.OnComplete(Role::kRerun);
  assert(cap.CanRunLive(/*queued_live=*/5, /*queued_rerun=*/0));
  assert(cap.ChooseNextRole(/*queued_live=*/5, /*queued_rerun=*/0) ==
         Role::kLive);
}

void TestLivePrioritizedOverRerunWhenBothEligible() {
  // total = 4, rerun_max = 2
  CapacityPolicy cap(4, 2);

  // Initially 0 running. With 5 Live and 5 Rerun queued:
  // rerun_target = min(2, 0 + 5) = 2 -> live_limit = 4 - 2 = 2.
  // Both CanRunLive and CanRunRerun are true.
  // Live must be prioritized over Rerun for the first 2 slots!
  for (int i = 0; i < 2; ++i) {
    assert(cap.CanRunLive(/*queued_live=*/5, /*queued_rerun=*/5));
    assert(cap.CanRunRerun(/*queued_rerun=*/5));
    assert(cap.ChooseNextRole(/*queued_live=*/5, /*queued_rerun=*/5) ==
           Role::kLive);
    cap.OnStart(Role::kLive);
  }

  // Once 2 Live jobs are running (reaching live_limit = 2), CanRunLive becomes
  // false so the remaining 2 slots go to Rerun.
  for (int i = 0; i < 2; ++i) {
    assert(!cap.CanRunLive(/*queued_live=*/3, /*queued_rerun=*/5));
    assert(cap.CanRunRerun(/*queued_rerun=*/5));
    assert(cap.ChooseNextRole(/*queued_live=*/3, /*queued_rerun=*/5) ==
           Role::kRerun);
    cap.OnStart(Role::kRerun);
  }
  assert(cap.running_live() == 2);
  assert(cap.running_rerun() == 2);
}

void TestZeroRerunMaxBlocksAllReruns() {
  CapacityPolicy cap(3, 0);
  assert(!cap.CanRunRerun(/*queued_rerun=*/5));
  assert(cap.CanRunLive(/*queued_live=*/5, /*queued_rerun=*/5));
  assert(cap.ChooseNextRole(/*queued_live=*/5, /*queued_rerun=*/5) ==
         Role::kLive);
}

void TestInvariantChecks() {
  CapacityPolicy cap(2, 1);

  bool threw_complete_unstarted = false;
  try {
    cap.OnComplete(Role::kLive);
  } catch (const std::logic_error&) {
    threw_complete_unstarted = true;
  }
  assert(threw_complete_unstarted);

  cap.OnStart(Role::kRerun);
  bool threw_exceed_rerun_max = false;
  try {
    cap.OnStart(Role::kRerun);
  } catch (const std::logic_error&) {
    threw_exceed_rerun_max = true;
  }
  assert(threw_exceed_rerun_max);
}

}  // namespace
}  // namespace scheduler

int main() {
  scheduler::TestLiveUsesAllSlotsWhenNoRerunWork();
  scheduler::TestLiveYieldsFreedSlotsToRerunUpToRerunMax();
  scheduler::TestPartialRerunDemandLeavesRemainingCapacityForLive();
  scheduler::TestLivePrioritizedOverRerunWhenBothEligible();
  scheduler::TestZeroRerunMaxBlocksAllReruns();
  scheduler::TestInvariantChecks();
  std::cout << "[PASS] Step 2: capacity_policy_test passed.\n";
  return 0;
}
