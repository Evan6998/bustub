#include "primer/hyperloglog_presto.h"

namespace bustub {

template <typename KeyType>
HyperLogLogPresto<KeyType>::HyperLogLogPresto(int16_t n_leading_bits) : cardinality_(0) {
  b_ = n_leading_bits > 0 ? n_leading_bits : 0;
  dense_bucket_.resize(1 << b_);
}

template <typename KeyType>
auto HyperLogLogPresto<KeyType>::CountOfRightMostContiguousZeros(const std::bitset<BITSET_CAPACITY> &bset) const
    -> uint64_t {
  for (size_t i = 0; i < bset.size() - b_; i++) {
    if (bset.test(i)) {
      return i;
    }
  }
  return bset.size() - b_;
}

template <typename KeyType>
auto HyperLogLogPresto<KeyType>::AddElem(KeyType val) -> void {
  /** @TODO(student) Implement this function! */
  auto hashVal = CalculateHash(val);
  std::bitset<BITSET_CAPACITY> bin{hashVal};
  auto rightMostZeros = CountOfRightMostContiguousZeros(bin);
  std::bitset<DENSE_BUCKET_SIZE> dense{rightMostZeros & DENSE_MASK};
  std::bitset<OVERFLOW_BUCKET_SIZE> overflow{(rightMostZeros >> DENSE_BUCKET_SIZE) & OVERFLOW_MASK};
  auto bucket = (bin >> (BITSET_CAPACITY - b_)).to_ulong();

  if (rightMostZeros > (overflow_bucket_[bucket].to_ulong() << DENSE_BUCKET_SIZE) + dense_bucket_[bucket].to_ulong()) {
    dense_bucket_[bucket] = dense;
    overflow_bucket_[bucket] = overflow;
  }
}

template <typename T>
auto HyperLogLogPresto<T>::ComputeCardinality() -> void {
  /** @TODO(student) Implement this function! */
  double sum = 0.0;
  for (size_t i = 0; i < (1 << b_); i++) {
    sum += std::pow(2, -(static_cast<double>(dense_bucket_[i].to_ulong()) +
                         static_cast<double>(overflow_bucket_[i].to_ulong() << DENSE_BUCKET_SIZE)));
  }
  auto alphaMM = CONSTANT * dense_bucket_.size() * dense_bucket_.size();
  cardinality_ = std::floor(alphaMM / sum);
}

template class HyperLogLogPresto<int64_t>;
template class HyperLogLogPresto<std::string>;
}  // namespace bustub
