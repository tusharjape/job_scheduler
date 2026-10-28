#pragma once

#include <cstddef>
#include <optional>

#include "job_scheduler/types.h"

namespace scheduler {

// Tracks active slots across Live and Rerun roles.
// Not internally synchronized.
class CapacityPolicy {
 public:
  explicit CapacityPolicy(std::size_t total, std::size_t rerun_max);

  // A waiting Rerun job may start iff total running < total_ and
  // running_rerun_ < rerun_max_.
  bool CanRunRerun(std::size_t queued_rerun) const noexcept;

  // A waiting Live job may start iff total running < total_ and
  // starting a Live job leaves enough capacity for active + waiting Rerun work
  // (up to rerun_max_).
  bool CanRunLive(std::size_t queued_live,
                  std::size_t queued_rerun) const noexcept;

  // Returns the next Role that should claim an available slot, or std::nullopt
  // if neither role is eligible to start a job right now.
  // Live takes precedence when both roles are eligible to run; when Live is at
  // or above its dynamic limit (leaving room for Rerun up to rerun_max_),
  // CanRunLive is false and Rerun is selected.
  std::optional<Role> ChooseNextRole(std::size_t queued_live,
                                     std::size_t queued_rerun) const noexcept;

  void OnStart(Role role);
  void OnComplete(Role role);

  std::size_t total() const noexcept { return total_; }
  std::size_t rerun_max() const noexcept { return rerun_max_; }
  std::size_t running_live() const noexcept { return running_live_; }
  std::size_t running_rerun() const noexcept { return running_rerun_; }
  std::size_t running_total() const noexcept {
    return running_live_ + running_rerun_;
  }

 private:
  std::size_t total_;
  std::size_t rerun_max_;
  std::size_t running_live_ = 0;
  std::size_t running_rerun_ = 0;
};

}  // namespace scheduler
