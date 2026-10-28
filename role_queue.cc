#include "job_scheduler/role_queue.h"

#include <stdexcept>
#include <utility>

namespace scheduler {

RoleQueue::RoleQueue(Duration full_max_wait) : full_max_wait_(full_max_wait) {
  if (full_max_wait_ < Duration::zero()) {
    throw std::invalid_argument("full_max_wait must be >= 0");
  }
}

void RoleQueue::Push(QueuedJob job) {
  if (job.type == JobType::kQuick) {
    job.virtual_timestamp = job.submit_time;
    quick_queue_.push_back(std::move(job));
  } else {
    job.virtual_timestamp = job.submit_time + full_max_wait_;
    full_queue_.push_back(std::move(job));
  }
}

bool RoleQueue::ComesBefore(const QueuedJob& a,
                            const QueuedJob& b) const noexcept {
  if (a.virtual_timestamp != b.virtual_timestamp) {
    return a.virtual_timestamp < b.virtual_timestamp;
  }
  // When virtual timestamps are equal:
  // - If full_max_wait_ == 0, Quick and Full submitted at the same instant t
  //   share virtual_timestamp == t; Quick will be preferred over Full.
  // - If full_max_wait_ > 0, a Full job submitted at t_F has
  //   virtual_timestamp = t_F + K. A Quick job with the same virtual_timestamp
  //   was submitted at t_Q = t_F + K (i.e., after the Full job has already
  //   waited K seconds). The newly arriving Quick job must NOT run
  //   ahead of the Full job, which is naturally satisfied by a.seq < b.seq
  //   (since the Full job was submitted earlier and has a smaller seq).
  if (full_max_wait_ == Duration::zero() && a.type != b.type) {
    return a.type == JobType::kQuick;
  }
  return a.seq < b.seq;
}

const QueuedJob& RoleQueue::PeekBest() const {
  if (Empty()) {
    throw std::logic_error("Cannot peek from an empty RoleQueue");
  }
  if (quick_queue_.empty()) {
    return full_queue_.front();
  }
  if (full_queue_.empty()) {
    return quick_queue_.front();
  }
  const QueuedJob& best_quick = quick_queue_.front();
  const QueuedJob& best_full = full_queue_.front();
  return ComesBefore(best_quick, best_full) ? best_quick : best_full;
}

QueuedJob RoleQueue::PopBest() {
  if (Empty()) {
    throw std::logic_error("Cannot pop from an empty RoleQueue");
  }
  bool pick_quick = false;
  if (full_queue_.empty()) {
    pick_quick = true;
  } else if (!quick_queue_.empty()) {
    pick_quick = ComesBefore(quick_queue_.front(), full_queue_.front());
  }

  if (pick_quick) {
    QueuedJob out = std::move(quick_queue_.front());
    quick_queue_.pop_front();
    return out;
  } else {
    QueuedJob out = std::move(full_queue_.front());
    full_queue_.pop_front();
    return out;
  }
}

}  // namespace scheduler
