#include "job_scheduler/job_scheduler.h"

#include <utility>

namespace scheduler {

namespace {

// Ensures SchedulerCore::Complete(role) is always called when a job finishes,
// even if the user-supplied callback throws an exception.
struct SlotGuard {
  SchedulerCore& core;
  Role role;
  ~SlotGuard() { core.Complete(role); }
};

}  // namespace

JobScheduler::JobScheduler(SchedulerConfig config, std::shared_ptr<Clock> clock)
    : core_(config, std::move(clock)) {
  workers_.reserve(config.total);
  for (std::size_t i = 0; i < config.total; ++i) {
    workers_.emplace_back([this]() { WorkerLoop(); });
  }
}

JobScheduler::~JobScheduler() { Shutdown(); }

std::uint64_t JobScheduler::Submit(Role role, JobType type,
                                   std::function<void()> fn,
                                   Urgency urgency) {
  return core_.Submit(role, type, std::move(fn), urgency);
}

void JobScheduler::WaitForIdle() { core_.WaitForIdle(); }

void JobScheduler::Shutdown() {
  core_.Shutdown();
  for (auto& worker : workers_) {
    if (worker.joinable()) {
      worker.join();
    }
  }
}

void JobScheduler::WorkerLoop() {
  while (auto job = core_.AcquireNext()) {
    SlotGuard guard{core_, job->role};
    try {
      job->fn();
    } catch (...) {
      // Swallow unhandled exceptions from job callbacks so worker threads
      // remain alive and the capacity slot is cleanly released by SlotGuard.
    }
  }
}

}  // namespace scheduler
