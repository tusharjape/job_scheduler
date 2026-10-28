#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <stdexcept>

namespace scheduler {

using Duration = std::chrono::nanoseconds;
using TimePoint = std::chrono::time_point<std::chrono::steady_clock, Duration>;

enum class Role {
  kLive,
  kRerun,
};

enum class JobType {
  kQuick,
  kFull,
};

struct SchedulerConfig {
  std::size_t total = 1;
  std::size_t rerun_max = 1;
  Duration full_max_wait = std::chrono::seconds(10);

  void Validate() const {
    if (total == 0) {
      throw std::invalid_argument("total must be >= 1");
    }
    if (rerun_max > total) {
      throw std::invalid_argument("rerun_max must be <= total");
    }
    if (full_max_wait < Duration::zero()) {
      throw std::invalid_argument("full_max_wait must be >= 0");
    }
  }
};

struct QueuedJob {
  std::uint64_t seq = 0;
  Role role = Role::kLive;
  JobType type = JobType::kQuick;
  TimePoint submit_time{};
  TimePoint virtual_timestamp{};
  std::function<void()> fn;
};

}  // namespace scheduler
