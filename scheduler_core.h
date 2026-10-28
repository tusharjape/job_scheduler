#pragma once

#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>

#include "job_scheduler/capacity_policy.h"
#include "job_scheduler/clock.h"
#include "job_scheduler/role_queue.h"
#include "job_scheduler/types.h"

namespace scheduler {

// Thread-safe monitor coordinating job submissions, role capacity limits, and
// per-role ordering queues.
class SchedulerCore {
 public:
  explicit SchedulerCore(SchedulerConfig config,
                         std::shared_ptr<Clock> clock =
                             std::make_shared<SystemClock>());

  SchedulerCore(const SchedulerCore&) = delete;
  SchedulerCore& operator=(const SchedulerCore&) = delete;

  // Enqueues a new job in O(1) time and wakes one waiting worker if any.
  // Returns the monotonically increasing sequence number assigned to the job.
  // Throws std::runtime_error if Shutdown() has already been called.
  std::uint64_t Submit(Role role, JobType type, std::function<void()> fn,
                       Urgency urgency = Urgency::kNormal);

  // Blocks until either a job is eligible to run under the capacity policy or
  // Shutdown() has been called and no more work will be dispatched.
  // Atomically claims a slot and pops the highest-priority eligible job in O(1)
  // time. Returns std::nullopt once shut down and drained.
  std::optional<QueuedJob> AcquireNext();

  // Non-blocking variant of AcquireNext() for deterministic state-machine
  // testing: returns the next eligible job (and claims its slot) if one can run
  // immediately, or std::nullopt otherwise.
  std::optional<QueuedJob> TryAcquireNext();

  // Releases the slot held by a completed job of `role` and notifies waiting
  // workers/waiters.
  void Complete(Role role);

  // Blocks until all queued jobs have been dispatched and all running jobs have
  // completed.
  void WaitForIdle();

  // Initiates shutdown: rejects new submissions and wakes all blocked workers
  // once existing queued work has been drained.
  void Shutdown();

  // Snapshot state accessors for observability and testing.
  std::size_t QueuedCount(Role role) const;
  std::size_t RunningCount(Role role) const;
  std::size_t TotalRunningCount() const;

 private:
  std::optional<QueuedJob> PopNextEligibleLocked();

  mutable std::mutex mu_;
  std::condition_variable work_cv_;
  std::condition_variable idle_cv_;
  std::shared_ptr<Clock> clock_;
  CapacityPolicy capacity_;
  RoleQueue live_queue_;
  RoleQueue rerun_queue_;
  std::uint64_t next_seq_ = 1;
  bool shutdown_ = false;
};

}  // namespace scheduler
