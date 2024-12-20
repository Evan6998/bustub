#include "primer/hyperloglog.h"

namespace bustub {

template <typename KeyType>
HyperLogLog<KeyType>::HyperLogLog(int16_t n_bits) : cardinality_(0) {
  b_ = n_bits > 0 ? n_bits : 0;
  buckets_.resize(1 << b_, 0);
}

template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeBinary(const hash_t &hash) const -> std::bitset<BITSET_CAPACITY> {
  std::bitset<BITSET_CAPACITY> bin{hash};
  return bin;
}

template <typename KeyType>
auto HyperLogLog<KeyType>::PositionOfLeftmostOne(const std::bitset<BITSET_CAPACITY> &bset) const -> uint64_t {
  for (auto i = BITSET_CAPACITY - b_ - 1; i > 0; --i) {
    if (bset.test(i)) {
      return BITSET_CAPACITY - b_ - i;
    }
  }
  return BITSET_CAPACITY - b_ + 1;
}

template <typename KeyType>
auto HyperLogLog<KeyType>::AddElem(KeyType val) -> void {
  auto hash_val = CalculateHash(val);
  auto bin = ComputeBinary(hash_val);
  auto pos = PositionOfLeftmostOne(bin);
  auto bucket = (bin >> (BITSET_CAPACITY - b_)).to_ulong();
  buckets_[bucket] = std::max(buckets_[bucket], pos);
}

template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeCardinality() -> void {
  double sum = 0.0;
  for (auto &bucket : buckets_) {
    sum += std::pow(2, -static_cast<double>(bucket));
  }
  auto alpha_mm = CONSTANT * buckets_.size() * buckets_.size();
  cardinality_ = std::floor(alpha_mm / sum);
}

template class HyperLogLog<int64_t>;
template class HyperLogLog<std::string>;

}  // namespace bustub
