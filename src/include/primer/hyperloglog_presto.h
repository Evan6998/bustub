#pragma once

#include <bitset>
#include <memory>
#include <mutex>  // NOLINT
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"

/** @brief Dense bucket size. */
#define DENSE_BUCKET_SIZE 4
/** @brief Dense bucket mask. */
#define DENSE_MASK 0xF
/** @brief Overflow bucket size. */
#define OVERFLOW_BUCKET_SIZE 3
/** @brief Overflow bucket mask. */
#define OVERFLOW_MASK 0x7
/** @brief Capacity of the bitset stream. */
#define BITSET_CAPACITY 64

/** @brief Total bucket size. */
#define TOTAL_BUCKET_SIZE (DENSE_BUCKET_SIZE + OVERFLOW_BUCKET_SIZE)

namespace bustub {

template <typename KeyType>
class HyperLogLogPresto {
  /**
   * INSTRUCTIONS: Testing framework will use the GetDenseBucket and GetOverflow function,
   * hence SHOULD NOT be deleted. It's essential to use the dense_bucket_
   * data structure.
   */

  /** @brief Constant for HLL. */
  static constexpr double CONSTANT = 0.79402;

 public:
  /** @brief Disabling default constructor. */
  HyperLogLogPresto() = delete;

  /** @brief Parameterized constructor. */
  explicit HyperLogLogPresto(int16_t n_leading_bits);

  /** @brief Returns the dense_bucket_ data structure. */
  auto GetDenseBucket() const -> std::vector<std::bitset<DENSE_BUCKET_SIZE>> { return dense_bucket_; }

  /** @brief Returns overflow bucket of a specific given index. */
  auto GetOverflowBucketofIndex(uint16_t idx) { return overflow_bucket_[idx]; }

  /** @brief Retusn the cardinality of the set. */
  auto GetCardinality() const -> uint64_t { return cardinality_; }

  /** @brief Element is added for HLL calculation. */
  auto AddElem(KeyType val) -> void;

  /** @brief Function to compute cardinality. */
  auto ComputeCardinality() -> void;

 private:
  /**
   * @brief Function that computes the count of the rightmost contiguous zeros(LSB).
   *
   * @param[in] bset - binary values of a given bitset
   * @returns rightmost contiguous zeros of given binary set
   */
  auto CountOfRightMostContiguousZeros(const std::bitset<BITSET_CAPACITY> &bset) const -> uint64_t;

  /** @brief Calculate Hash.
   *
   * @param[in] val
   *
   * @returns hash value
   */
  inline auto CalculateHash(KeyType val) -> hash_t {
    Value val_obj;
    if constexpr (std::is_same<KeyType, std::string>::value) {
      val_obj = Value(VARCHAR, val);
      return bustub::HashUtil::HashValue(&val_obj);
    }
    if constexpr (std::is_same<KeyType, int64_t>::value) {
      return static_cast<hash_t>(val);
    }
    return 0;
  }

  /** @brief Structure holding dense buckets (or also known as registers). */
  std::vector<std::bitset<DENSE_BUCKET_SIZE>> dense_bucket_;

  /** @brief Structure holding overflow buckets. */
  std::unordered_map<uint16_t, std::bitset<OVERFLOW_BUCKET_SIZE>> overflow_bucket_;

  /** @brief Storing cardinality value */
  uint64_t cardinality_;

  // TODO(student) - can add more data structures as required
  /** @brief Number of initial bits in a binary representation of a hash value. */
  uint16_t b_;
};

}  // namespace bustub
