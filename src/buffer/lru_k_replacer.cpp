//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKNode::LRUKNode(size_t k, frame_id_t fid): k_(k), fid_(fid) {};

auto LRUKNode::Evictable () -> bool {
  return is_evictable_;
}

auto LRUKNode::KthDistance(size_t ts) -> size_t {
  if (history_.size() < k_) {
    return std::numeric_limits<size_t>::max();
  }
  return ts - history_.back();
}

auto LRUKNode::EarlistTimeStamp() -> size_t {
  return history_.front();
}

void LRUKNode::Access(size_t ts) {
  if (history_.size() >= k_) {
    history_.pop_back();
  }
  history_.push_front(ts);
}

void LRUKNode::SetEvictable(bool set_evictable) {
  is_evictable_ = set_evictable;
}

void LRUKNode::Evict() {
  history_.clear();
  is_evictable_ = false;
}

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  for (size_t fid = 0; fid < replacer_size_; fid++) {
    node_store_.insert({fid, LRUKNode(k, fid)});
  }
}

auto LRUKReplacer::Evict() -> std::optional<frame_id_t> { 
  frame_id_t victim = 0;
  size_t largest = 0;
  size_t earliest = 0;
  for (auto iter = node_store_.begin(); iter != node_store_.end(); iter++) {
    if (!iter->second.Evictable()) continue;
    auto fid = iter->first;
    auto node = iter->second;
    auto kd = node.KthDistance(current_timestamp_);
    if (kd > largest) {
      largest = kd;
      victim = fid;
      earliest = node.EarlistTimeStamp();
    } else if (kd == largest && node.EarlistTimeStamp() < earliest) {
      victim = fid;
      earliest = node.EarlistTimeStamp();
    }
  }
  if (victim != 0) {
    node_store_.at(victim).Evict();
    curr_size_ -= 1;
    return victim;
  };
  return std::nullopt; 
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  if (frame_id < 0 || (size_t)frame_id >= replacer_size_) {
    throw std::invalid_argument("Invalid frame id.");
  }
  auto &node = node_store_.at(frame_id);
  node.Access(current_timestamp_++);
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  if (frame_id < 0 || (size_t)frame_id >= replacer_size_) {
    throw std::invalid_argument("Invalid frame id.");
  }
  auto &node = node_store_.at(frame_id);
  if (node.Evictable() == set_evictable) {
    return;
  }
  node.SetEvictable(set_evictable);
  if (set_evictable) {
    curr_size_ += 1;
  } else {
    curr_size_ -= 1;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  if (frame_id < 0 || (size_t)frame_id >= replacer_size_) return;
  auto &node = node_store_.at(frame_id);
  if (!node.Evictable()) {
    throw std::invalid_argument("Frame is not evictable.");
  }
  node.Evict();
  curr_size_ -= 1;
}

auto LRUKReplacer::Size() -> size_t {
  return curr_size_;
}

}  // namespace bustub
