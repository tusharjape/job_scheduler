#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <thread>
#include <vector>

#include "job_scheduler/clock.h"
#include "job_scheduler/scheduler_core.h"
#include "job_scheduler/types.h"

namespace scheduler {

// Thread-safe job scheduler managing a fixed pool of `config.total` worker
// threads.
class JobScheduler {
 public:
  explicit JobScheduler(SchedulerConfig config,
                        std::shared_ptr<Clock> clock =
                            std::make_shared<SystemClock>());

  // Initiates shutdown and joins all worker threads.
  ~JobScheduler();

  JobScheduler(const JobScheduler&) = delete;
  JobScheduler& operator=(const JobScheduler&) = delete;

  // Submits a job with the given role, type, and optional urgency to the
  // scheduler. Returns the monotonically increasing sequence ID assigned to the
  // job.
  std::uint64_t Submit(Role role, JobType type, std::function<void()> fn,
                       Urgency urgency = Urgency::kNormal);

  // Blocks until all currently queued and running jobs have completed.
  void WaitForIdle();

  // Rejects future submissions, drains remaining queued work, and joins all
  // worker threads. Safe to call multiple times.
  void Shutdown();

  std::size_t QueuedCount(Role role) const { return core_.QueuedCount(role); }
  std::size_t RunningCount(Role role) const { return core_.RunningCount(role); }
  std::size_t TotalRunningCount() const { return core_.TotalRunningCount(); }

 private:
  void WorkerLoop();

  SchedulerCore core_;
  std::vector<std::jthread> workers_;
};

}  // namespace scheduler
