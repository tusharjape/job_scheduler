#include "job_scheduler/scheduler_core.h"

#include <stdexcept>
#include <utility>

namespace scheduler {

SchedulerCore::SchedulerCore(SchedulerConfig config,
                             std::shared_ptr<Clock> clock)
    : clock_(clock ? std::move(clock) : std::make_shared<SystemClock>()),
      capacity_([&config]() {
        config.Validate();
        return CapacityPolicy(config.total, config.rerun_max);
      }()),
      live_queue_(config.full_max_wait, Duration::zero()),
      rerun_queue_(config.full_max_wait, config.normal_rerun_max_wait) {}

std::uint64_t SchedulerCore::Submit(Role role, JobType type,
                                    std::function<void()> fn,
                                    Urgency urgency) {
  if (!fn) {
    throw std::invalid_argument("Job callback fn must not be empty");
  }

  std::uint64_t assigned_seq = 0;
  {
    std::lock_guard<std::mutex> lock(mu_);
    if (shutdown_) {
      throw std::runtime_error("Cannot submit job after Shutdown()");
    }
    assigned_seq = next_seq_++;
    // Urgency only applies to Rerun jobs; normalize Live jobs to kNormal.
    const Urgency effective_urgency =
        (role == Role::kRerun) ? urgency : Urgency::kNormal;
    QueuedJob job{
        .seq = assigned_seq,
        .role = role,
        .type = type,
        .urgency = effective_urgency,
        .submit_time = clock_->Now(),
        .virtual_timestamp = TimePoint{},
        .fn = std::move(fn),
    };
    if (role == Role::kLive) {
      live_queue_.Push(std::move(job));
    } else {
      rerun_queue_.Push(std::move(job));
    }
  }
  work_cv_.notify_one();
  return assigned_seq;
}

std::optional<QueuedJob> SchedulerCore::PopNextEligibleLocked() {
  const std::optional<Role> next_role =
      capacity_.ChooseNextRole(live_queue_.Size(), rerun_queue_.Size());
  if (!next_role.has_value()) {
    return std::nullopt;
  }

  capacity_.OnStart(*next_role);
  if (*next_role == Role::kLive) {
    return live_queue_.PopBest();
  } else {
    return rerun_queue_.PopBest();
  }
}

std::optional<QueuedJob> SchedulerCore::AcquireNext() {
  std::unique_lock<std::mutex> lock(mu_);
  while (true) {
    if (auto job = PopNextEligibleLocked()) {
      return job;
    }
    if (shutdown_ && live_queue_.Empty() && rerun_queue_.Empty()) {
      return std::nullopt;
    }
    // If shutdown_ was requested while jobs are still queued, but capacity is
    // currently full because other workers are still finishing their jobs,
    // wait until a running worker calls Complete() and frees a slot.
    work_cv_.wait(lock);
  }
}

std::optional<QueuedJob> SchedulerCore::TryAcquireNext() {
  std::lock_guard<std::mutex> lock(mu_);
  return PopNextEligibleLocked();
}

void SchedulerCore::Complete(Role role) {
  bool became_idle = false;
  {
    std::lock_guard<std::mutex> lock(mu_);
    capacity_.OnComplete(role);
    became_idle = (capacity_.running_total() == 0 && live_queue_.Empty() &&
                   rerun_queue_.Empty());
  }
  work_cv_.notify_one();
  if (became_idle) {
    idle_cv_.notify_all();
  }
}

void SchedulerCore::WaitForIdle() {
  std::unique_lock<std::mutex> lock(mu_);
  idle_cv_.wait(lock, [this]() {
    return capacity_.running_total() == 0 && live_queue_.Empty() &&
           rerun_queue_.Empty();
  });
}

void SchedulerCore::Shutdown() {
  {
    std::lock_guard<std::mutex> lock(mu_);
    shutdown_ = true;
  }
  work_cv_.notify_all();
  idle_cv_.notify_all();
}

std::size_t SchedulerCore::QueuedCount(Role role) const {
  std::lock_guard<std::mutex> lock(mu_);
  return (role == Role::kLive) ? live_queue_.Size() : rerun_queue_.Size();
}

std::size_t SchedulerCore::RunningCount(Role role) const {
  std::lock_guard<std::mutex> lock(mu_);
  return (role == Role::kLive) ? capacity_.running_live()
                               : capacity_.running_rerun();
}

std::size_t SchedulerCore::TotalRunningCount() const {
  std::lock_guard<std::mutex> lock(mu_);
  return capacity_.running_total();
}

}  // namespace scheduler
