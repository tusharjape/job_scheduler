#pragma once

#include <chrono>
#include <mutex>

#include "scheduler/types.h"

namespace scheduler {

class Clock {
 public:
  virtual ~Clock() = default;
  virtual TimePoint Now() const = 0;
};

class SystemClock final : public Clock {
 public:
  TimePoint Now() const override {
    return std::chrono::time_point_cast<Duration>(
        std::chrono::steady_clock::now());
  }
};

class FakeClock final : public Clock {
 public:
  explicit FakeClock(TimePoint initial = TimePoint{}) : now_(initial) {}

  TimePoint Now() const override {
    std::lock_guard<std::mutex> lock(mu_);
    return now_;
  }

  void Advance(Duration delta) {
    std::lock_guard<std::mutex> lock(mu_);
    now_ += delta;
  }

  void Set(TimePoint t) {
    std::lock_guard<std::mutex> lock(mu_);
    now_ = t;
  }

 private:
  mutable std::mutex mu_;
  TimePoint now_;
};

}  // namespace scheduler
