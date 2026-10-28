#include "job_scheduler/role_queue.h"

#include <stdexcept>
#include <utility>

namespace scheduler {

RoleQueue::RoleQueue(Duration full_max_wait, Duration normal_rerun_max_wait)
    : full_max_wait_(full_max_wait),
      normal_rerun_max_wait_(normal_rerun_max_wait) {
  if (full_max_wait_ < Duration::zero()) {
    throw std::invalid_argument("full_max_wait must be >= 0");
  }
  if (normal_rerun_max_wait_ < Duration::zero()) {
    throw std::invalid_argument("normal_rerun_max_wait must be >= 0");
  }
}

std::size_t RoleQueue::BucketIndex(Urgency urgency, JobType type) noexcept {
  const std::size_t urgency_bit = (urgency == Urgency::kNormal) ? 2 : 0;
  const std::size_t type_bit = (type == JobType::kFull) ? 1 : 0;
  return urgency_bit + type_bit;
}

void RoleQueue::Push(QueuedJob job) {
  Duration offset = Duration::zero();
  if (job.type == JobType::kFull) {
    offset += full_max_wait_;
  }
  if (job.role == Role::kRerun && job.urgency == Urgency::kNormal) {
    offset += normal_rerun_max_wait_;
  }
  job.virtual_timestamp = job.submit_time + offset;
  const std::size_t idx = BucketIndex(job.urgency, job.type);
  buckets_[idx].push_back(std::move(job));
}

bool RoleQueue::ComesBefore(const QueuedJob& a,
                            const QueuedJob& b) const noexcept {
  if (a.virtual_timestamp != b.virtual_timestamp) {
    return a.virtual_timestamp < b.virtual_timestamp;
  }
  // When virtual timestamps are equal:
  // - If normal_rerun_max_wait_ == 0 and a/b have different urgency (for Rerun
  //   jobs), Urgent is still preferred over Normal at the exact same timestamp.
  // - If full_max_wait_ == 0 and a/b have different job types, Quick is still
  //   preferred over Full at the exact same timestamp.
  // - Otherwise, when the corresponding wait threshold (> 0) produced equal
  //   virtual timestamps, the older job has already waited its full anti-
  //   starvation threshold and wins via a.seq < b.seq.
  if (normal_rerun_max_wait_ == Duration::zero() && a.urgency != b.urgency) {
    return a.urgency == Urgency::kUrgent;
  }
  if (full_max_wait_ == Duration::zero() && a.type != b.type) {
    return a.type == JobType::kQuick;
  }
  return a.seq < b.seq;
}

std::size_t RoleQueue::BestBucketIndex() const {
  std::size_t best_idx = kNumBuckets;
  for (std::size_t i = 0; i < kNumBuckets; ++i) {
    if (buckets_[i].empty()) {
      continue;
    }
    if (best_idx == kNumBuckets ||
        ComesBefore(buckets_[i].front(), buckets_[best_idx].front())) {
      best_idx = i;
    }
  }
  if (best_idx == kNumBuckets) {
    throw std::logic_error("RoleQueue is empty");
  }
  return best_idx;
}

const QueuedJob& RoleQueue::PeekBest() const {
  return buckets_[BestBucketIndex()].front();
}

QueuedJob RoleQueue::PopBest() {
  const std::size_t best_idx = BestBucketIndex();
  QueuedJob out = std::move(buckets_[best_idx].front());
  buckets_[best_idx].pop_front();
  return out;
}

bool RoleQueue::Empty() const noexcept {
  for (const auto& bucket : buckets_) {
    if (!bucket.empty()) {
      return false;
    }
  }
  return true;
}

std::size_t RoleQueue::Size() const noexcept {
  std::size_t total = 0;
  for (const auto& bucket : buckets_) {
    total += bucket.size();
  }
  return total;
}

}  // namespace scheduler
