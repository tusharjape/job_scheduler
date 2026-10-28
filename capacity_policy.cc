#include "job_scheduler/capacity_policy.h"

#include <algorithm>
#include <stdexcept>

namespace scheduler {

CapacityPolicy::CapacityPolicy(std::size_t total, std::size_t rerun_max)
    : total_(total), rerun_max_(rerun_max) {
  if (total_ == 0) {
    throw std::invalid_argument("total must be >= 1");
  }
  if (rerun_max_ > total_) {
    throw std::invalid_argument("rerun_max must be <= total");
  }
}

bool CapacityPolicy::CanRunRerun(std::size_t queued_rerun) const noexcept {
  if (queued_rerun == 0) {
    return false;
  }
  if (running_total() >= total_) {
    return false;
  }
  return running_rerun_ < rerun_max_;
}

bool CapacityPolicy::CanRunLive(std::size_t queued_live,
                                std::size_t queued_rerun) const noexcept {
  if (queued_live == 0) {
    return false;
  }
  if (running_total() >= total_) {
    return false;
  }
  // Rerun needs min(rerun_max_, running_rerun_ + queued_rerun) slots.
  // Live may occupy at most (total_ - rerun_target) slots when starting new
  // work.
  const std::size_t rerun_demand = running_rerun_ + queued_rerun;
  const std::size_t rerun_target = std::min(rerun_max_, rerun_demand);
  const std::size_t live_limit = total_ - rerun_target;
  return running_live_ < live_limit;
}

std::optional<Role> CapacityPolicy::ChooseNextRole(
    std::size_t queued_live, std::size_t queued_rerun) const noexcept {
  if (CanRunLive(queued_live, queued_rerun)) {
    return Role::kLive;
  }
  if (CanRunRerun(queued_rerun)) {
    return Role::kRerun;
  }
  return std::nullopt;
}

void CapacityPolicy::OnStart(Role role) {
  if (running_total() >= total_) {
    throw std::logic_error("Cannot start job: total capacity already full");
  }
  if (role == Role::kRerun) {
    if (running_rerun_ >= rerun_max_) {
      throw std::logic_error("Cannot start rerun job: rerun_max reached");
    }
    ++running_rerun_;
  } else {
    ++running_live_;
  }
}

void CapacityPolicy::OnComplete(Role role) {
  if (role == Role::kRerun) {
    if (running_rerun_ == 0) {
      throw std::logic_error("Cannot complete rerun job: none running");
    }
    --running_rerun_;
  } else {
    if (running_live_ == 0) {
      throw std::logic_error("Cannot complete live job: none running");
    }
    --running_live_;
  }
}

}  // namespace scheduler
