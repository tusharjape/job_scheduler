#pragma once

#include <array>
#include <cstddef>
#include <deque>

#include "job_scheduler/types.h"

namespace scheduler {

// Per-role O(1) priority queue ordering jobs by urgency, type, and submission
// time:
//   - Quick jobs are preferred over Full jobs.
//   - Once a Full job has waited full_max_wait_, newly arriving Quick jobs
//     (with the same urgency) do not run ahead of it.
//   - For Rerun queues (when normal_rerun_max_wait_ > 0), Urgent reruns start
//     sooner than Normal reruns, but once a Normal rerun has waited
//     normal_rerun_max_wait_, newly arriving Urgent reruns (of the same type)
//     do not run ahead of it.
//   - Jobs that are otherwise equal run in submission order (seq).
//   - Push and PopBest are both strict O(1) time using four monotonic FIFOs
//     indexed by (Urgency, JobType).
class RoleQueue {
 public:
  explicit RoleQueue(Duration full_max_wait,
                     Duration normal_rerun_max_wait = Duration::zero());

  // Computes job.virtual_timestamp and appends to the appropriate FIFO in O(1).
  void Push(QueuedJob job);

  // Peeks at the next job that would be popped in O(1).
  // Precondition: !Empty()
  const QueuedJob& PeekBest() const;

  // Removes and returns the highest-priority job in O(1).
  // Precondition: !Empty()
  QueuedJob PopBest();

  bool Empty() const noexcept;

  std::size_t Size() const noexcept;

  // Exposed for unit testing the comparator directly.
  bool ComesBefore(const QueuedJob& a, const QueuedJob& b) const noexcept;

 private:
  static constexpr std::size_t kNumBuckets = 4;

  static std::size_t BucketIndex(Urgency urgency, JobType type) noexcept;
  std::size_t BestBucketIndex() const;

  Duration full_max_wait_;
  Duration normal_rerun_max_wait_;
  std::array<std::deque<QueuedJob>, kNumBuckets> buckets_;
};

}  // namespace scheduler
