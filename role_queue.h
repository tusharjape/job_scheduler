#pragma once

#include <cstddef>
#include <deque>

#include "job_scheduler/types.h"

namespace scheduler {

// Per-role priority queue that implements the following ordering rules:
//   1. Quick jobs are preferred over Full jobs.
//   2. Once a Full job has waited K seconds (full_max_wait_), newly arriving
//       Quick jobs (submitted at t >= submit_time + K) do not run ahead of it.
//   3. Jobs that are otherwise equal run in submission order (seq).
//   4. Push and PopBest are both strict O(1) time using two monotonic FIFOs.
class RoleQueue {
 public:
  explicit RoleQueue(Duration full_max_wait);

  // Computes job.virtual_timestamp and appends to the appropriate FIFO in O(1).
  void Push(QueuedJob job);

  // Peeks at the next job that would be popped in O(1).
  // Precondition: !Empty()
  const QueuedJob& PeekBest() const;

  // Removes and returns the highest-priority job in O(1).
  // Precondition: !Empty()
  QueuedJob PopBest();

  bool Empty() const noexcept {
    return quick_queue_.empty() && full_queue_.empty();
  }

  std::size_t Size() const noexcept {
    return quick_queue_.size() + full_queue_.size();
  }

  // Exposed for unit testing the comparator directly.
  bool ComesBefore(const QueuedJob& a, const QueuedJob& b) const noexcept;

 private:
  Duration full_max_wait_;
  std::deque<QueuedJob> quick_queue_;
  std::deque<QueuedJob> full_queue_;
};

}  // namespace scheduler
