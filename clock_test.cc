#include "job_scheduler/clock.h"

#include <cassert>
#include <chrono>
#include <iostream>
#include <stdexcept>
#include <thread>
#include <vector>

#include "job_scheduler/types.h"

namespace scheduler {
namespace {

using namespace std::chrono_literals;

void TestConfigValidation() {
  SchedulerConfig valid{.total = 4, .rerun_max = 2, .full_max_wait = 5s};
  valid.Validate();

  // rerun_max == 0 is valid (e.g., reruns temporarily disabled).
  SchedulerConfig zero_rerun{.total = 4, .rerun_max = 0, .full_max_wait = 0s};
  zero_rerun.Validate();

  // total == 0 must throw.
  bool threw_zero_total = false;
  try {
    SchedulerConfig bad{.total = 0, .rerun_max = 0, .full_max_wait = 1s};
    bad.Validate();
  } catch (const std::invalid_argument&) {
    threw_zero_total = true;
  }
  assert(threw_zero_total);

  // rerun_max > total must throw.
  bool threw_rerun_exceeds_total = false;
  try {
    SchedulerConfig bad{.total = 2, .rerun_max = 3, .full_max_wait = 1s};
    bad.Validate();
  } catch (const std::invalid_argument&) {
    threw_rerun_exceeds_total = true;
  }
  assert(threw_rerun_exceeds_total);

  // negative full_max_wait must throw.
  bool threw_negative_wait = false;
  try {
    SchedulerConfig bad{.total = 2, .rerun_max = 1, .full_max_wait = -1s};
    bad.Validate();
  } catch (const std::invalid_argument&) {
    threw_negative_wait = true;
  }
  assert(threw_negative_wait);
}

void TestFakeClockDeterministicProgression() {
  FakeClock clock;
  const TimePoint t0 = clock.Now();
  assert(t0.time_since_epoch() == 0ns);

  clock.Advance(250ms);
  assert(clock.Now() - t0 == 250ms);

  clock.Advance(3s);
  assert(clock.Now() - t0 == 3250ms);

  const TimePoint custom = TimePoint(100s);
  clock.Set(custom);
  assert(clock.Now() == custom);
}

void TestFakeClockThreadSafety() {
  FakeClock clock;
  constexpr int kThreads = 8;
  constexpr int kIncrementsPerThread = 1000;

  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int i = 0; i < kThreads; ++i) {
    threads.emplace_back([&clock]() {
      for (int j = 0; j < kIncrementsPerThread; ++j) {
        clock.Advance(1ms);
        (void)clock.Now();
      }
    });
  }
  for (auto& t : threads) {
    t.join();
  }

  assert(clock.Now().time_since_epoch() ==
         std::chrono::milliseconds(kThreads * kIncrementsPerThread));
}

void TestSystemClockMonotonicity() {
  SystemClock clock;
  const TimePoint t1 = clock.Now();
  const TimePoint t2 = clock.Now();
  assert(t2 >= t1);
}

}  // namespace
}  // namespace scheduler

int main() {
  scheduler::TestConfigValidation();
  scheduler::TestFakeClockDeterministicProgression();
  scheduler::TestFakeClockThreadSafety();
  scheduler::TestSystemClockMonotonicity();
  std::cout << "[PASS] Step 1: types_and_clock_test passed.\n";
  return 0;
}
