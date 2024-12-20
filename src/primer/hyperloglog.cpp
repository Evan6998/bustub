#include "primer/hyperloglog.h"

namespace bustub {

template <typename KeyType>
HyperLogLog<KeyType>::HyperLogLog(int16_t n_bits) : cardinality_(0) {
  b_ = n_bits > 0 ? n_bits : 0;
  buckets.resize(1 << b_, 0);
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
  auto hashVal = CalculateHash(val);
  auto bin = ComputeBinary(hashVal);
  auto pos = PositionOfLeftmostOne(bin);
  auto bucket = (bin >> (BITSET_CAPACITY - b_)).to_ulong();
  buckets[bucket] = std::max(buckets[bucket], pos);
}

template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeCardinality() -> void {
  double sum = 0.0;
  for (auto &bucket : buckets) {
    sum += std::pow(2, -static_cast<double>(bucket));
  }
  auto alphaMM = CONSTANT * buckets.size() * buckets.size();
  cardinality_ = std::floor(alphaMM / sum);
}

template class HyperLogLog<int64_t>;
template class HyperLogLog<std::string>;

}  // namespace bustub
