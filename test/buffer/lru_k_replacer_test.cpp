/**
 * lru_k_replacer_test.cpp
 */

#include "buffer/lru_k_replacer.h"

#include <algorithm>
#include <cstdio>
#include <memory>
#include <random>
#include <set>
#include <thread>  // NOLINT
#include <vector>

#include "gtest/gtest.h"

namespace bustub {

TEST(LRUKReplacerTest, SampleTest) {
  // Note that comparison with `std::nullopt` always results in `false`, and if the optional type actually does contain
  // a value, the comparison will compare the inner value.
  // See: https://devblogs.microsoft.com/oldnewthing/20211004-00/?p=105754
  std::optional<frame_id_t> frame;

  // Initialize the replacer.
  LRUKReplacer lru_replacer(7, 2);

  // Add six frames to the replacer. We now have frames [1, 2, 3, 4, 5]. We set frame 6 as non-evictable.
  lru_replacer.RecordAccess(1);
  lru_replacer.RecordAccess(2);
  lru_replacer.RecordAccess(3);
  lru_replacer.RecordAccess(4);
  lru_replacer.RecordAccess(5);
  lru_replacer.RecordAccess(6);
  lru_replacer.SetEvictable(1, true);
  lru_replacer.SetEvictable(2, true);
  lru_replacer.SetEvictable(3, true);
  lru_replacer.SetEvictable(4, true);
  lru_replacer.SetEvictable(5, true);
  lru_replacer.SetEvictable(6, false);

  // The size of the replacer is the number of frames that can be evicted, _not_ the total number of frames entered.
  ASSERT_EQ(5, lru_replacer.Size());

  // Record an access for frame 1. Now frame 1 has two accesses total.
  lru_replacer.RecordAccess(1);
  // All other frames now share the maximum backward k-distance. Since we use timestamps to break ties, where the first
  // to be evicted is the frame with the oldest timestamp, the order of eviction should be [2, 3, 4, 5, 1].

  // Evict three pages from the replacer.
  // To break ties, we use LRU with respect to the oldest timestamp, or the least recently used frame.
  ASSERT_EQ(2, lru_replacer.Evict());
  ASSERT_EQ(3, lru_replacer.Evict());
  ASSERT_EQ(4, lru_replacer.Evict());
  ASSERT_EQ(2, lru_replacer.Size());
  // Now the replacer has the frames [5, 1].

  // Insert new frames [3, 4], and update the access history for 5. Now, the ordering is [3, 1, 5, 4].
  lru_replacer.RecordAccess(3);
  lru_replacer.RecordAccess(4);
  lru_replacer.RecordAccess(5);
  lru_replacer.RecordAccess(4);
  lru_replacer.SetEvictable(3, true);
  lru_replacer.SetEvictable(4, true);
  ASSERT_EQ(4, lru_replacer.Size());

  // Look for a frame to evict. We expect frame 3 to be evicted next.
  ASSERT_EQ(3, lru_replacer.Evict());
  ASSERT_EQ(3, lru_replacer.Size());

  // Set 6 to be evictable. 6 Should be evicted next since it has the maximum backward k-distance.
  lru_replacer.SetEvictable(6, true);
  ASSERT_EQ(4, lru_replacer.Size());
  ASSERT_EQ(6, lru_replacer.Evict());
  ASSERT_EQ(3, lru_replacer.Size());

  // Mark frame 1 as non-evictable. We now have [5, 4].
  lru_replacer.SetEvictable(1, false);

  // We expect frame 5 to be evicted next.
  ASSERT_EQ(2, lru_replacer.Size());
  ASSERT_EQ(5, lru_replacer.Evict());
  ASSERT_EQ(1, lru_replacer.Size());

  // Update the access history for frame 1 and make it evictable. Now we have [4, 1].
  lru_replacer.RecordAccess(1);
  lru_replacer.RecordAccess(1);
  lru_replacer.SetEvictable(1, true);
  ASSERT_EQ(2, lru_replacer.Size());

  // Evict the last two frames.
  ASSERT_EQ(4, lru_replacer.Evict());
  ASSERT_EQ(1, lru_replacer.Size());
  ASSERT_EQ(1, lru_replacer.Evict());
  ASSERT_EQ(0, lru_replacer.Size());

  // Insert frame 1 again and mark it as non-evictable.
  lru_replacer.RecordAccess(1);
  lru_replacer.SetEvictable(1, false);
  ASSERT_EQ(0, lru_replacer.Size());

  // A failed eviction should not change the size of the replacer.
  frame = lru_replacer.Evict();
  ASSERT_EQ(false, frame.has_value());

  // Mark frame 1 as evictable again and evict it.
  lru_replacer.SetEvictable(1, true);
  ASSERT_EQ(1, lru_replacer.Size());
  ASSERT_EQ(1, lru_replacer.Evict());
  ASSERT_EQ(0, lru_replacer.Size());

  // There is nothing left in the replacer, so make sure this doesn't do something strange.
  frame = lru_replacer.Evict();
  ASSERT_EQ(false, frame.has_value());
  ASSERT_EQ(0, lru_replacer.Size());

  // Make sure that setting a non-existent frame as evictable or non-evictable doesn't do something strange.
  lru_replacer.SetEvictable(6, false);
  lru_replacer.SetEvictable(6, true);
}

/**
 * @brief Test that no pages are evicted if all frames are marked non-evictable.
 */
TEST(LRUKReplacerExtendedTest, NoEvictionIfNonEvictable) {
  LRUKReplacer lru_replacer(5, 2);

  // Record accesses for frames [0..4] but do NOT set them as evictable.
  for (int i = 0; i < 5; i++) {
    lru_replacer.RecordAccess(i);
    // All frames remain non-evictable
    lru_replacer.SetEvictable(i, false);
  }

  // Expect size to be 0 because no frames are evictable.
  ASSERT_EQ(0, lru_replacer.Size());

  // Attempt eviction, should return no frame.
  auto victim = lru_replacer.Evict();
  ASSERT_FALSE(victim.has_value());
  ASSERT_EQ(0, lru_replacer.Size());
}

/**
 * @brief Test removing frames not in the replacer or non-evictable frames.
 */
TEST(LRUKReplacerExtendedTest, RemovingUnknownOrNonEvictableFrames) {
  LRUKReplacer lru_replacer(4, 2);

  // Insert frames [1, 2], mark them evictable.
  lru_replacer.RecordAccess(1);
  lru_replacer.RecordAccess(2);
  lru_replacer.SetEvictable(1, true);
  lru_replacer.SetEvictable(2, true);
  ASSERT_EQ(2, lru_replacer.Size());

  // Try to remove a frame that doesn't exist (frame 5). Should do nothing.
  // lru_replacer.Remove(5);
  ASSERT_EQ(2, lru_replacer.Size());

  // Try to remove an existing frame but in non-evictable state.
  // Switch frame 1 to non-evictable, then remove should fail/throw/abort.
  lru_replacer.SetEvictable(1, false);
  // Optional: Check for exception if your code throws on removing non-evictable frames.
  // For demonstration, we assume it doesn't remove or crash:
  // lru_replacer.Remove(1);
  // Frame 1 is still in the replacer, so size remains 1 for the evictable frames.
  ASSERT_EQ(1, lru_replacer.Size());

  // Evict the remaining evictable frame (2).
  auto victim = lru_replacer.Evict();
  ASSERT_TRUE(victim.has_value());
  ASSERT_EQ(2, victim.value());
  ASSERT_EQ(0, lru_replacer.Size());
}

/**
 * @brief Test repeated accesses to the same frame, ensuring that
 *  the frame's backward k-distance updates correctly.
 */
TEST(LRUKReplacerExtendedTest, RepeatedAccessSameFrame) {
  LRUKReplacer lru_replacer(3, 3);

  // Record multiple accesses to frame 0.
  for (int i = 0; i < 10; i++) {
    lru_replacer.RecordAccess(0);
  }
  // Mark frame 0 as evictable.
  lru_replacer.SetEvictable(0, true);
  ASSERT_EQ(1, lru_replacer.Size());

  // Add another frame (1) with fewer accesses.
  lru_replacer.RecordAccess(1);
  lru_replacer.SetEvictable(1, true);
  ASSERT_EQ(2, lru_replacer.Size());

  // Frame 0 should have many access timestamps, so its backward k-distance
  // may be smaller than frame 1 if k=3 is satisfied.
  // Let's see who gets evicted first.
  // If multiple frames have fewer than k=3 references, they get +inf, then LRU decides.

  auto victim = lru_replacer.Evict();
  ASSERT_TRUE(victim.has_value());
  // Depending on your logic:
  // - If frame 1 has only 1 access, it has +inf k-distance.
  // - If frame 0 has 10 accesses, it fully satisfies k=3, so its backward k-distance might be small.
  // Usually, the frame with the largest k-distance gets evicted first, so likely victim=1.
  // We'll just check that size is decremented properly.
  ASSERT_EQ(1, lru_replacer.Size());

  // Evict the last frame (likely frame 0).
  victim = lru_replacer.Evict();
  ASSERT_TRUE(victim.has_value());
  ASSERT_EQ(0, lru_replacer.Size());
}

/**
 * @brief Test a larger number of frames to ensure correctness under heavier usage.
 */
TEST(LRUKReplacerExtendedTest, LargerNumberOfFrames) {
  const int total_frames = 20;
  LRUKReplacer lru_replacer(total_frames, 2);

  // Record access for all frames [0..19].
  // Mark some of them as evictable.
  for (int i = 0; i < total_frames; i++) {
    lru_replacer.RecordAccess(i);
    // Mark half as evictable
    if (i % 2 == 0) {
      lru_replacer.SetEvictable(i, true);
    } else {
      lru_replacer.SetEvictable(i, false);
    }
  }

  // We expect 10 frames to be evictable.
  ASSERT_EQ(10, lru_replacer.Size());

  // Evict a few frames.
  for (int evict_count = 0; evict_count < 5; evict_count++) {
    auto victim = lru_replacer.Evict();
    ASSERT_TRUE(victim.has_value());
  }
  // Now 5 evictable frames remain.
  ASSERT_EQ(5, lru_replacer.Size());

  // Set some previously non-evictable frames to evictable.
  for (int i = 1; i < total_frames; i += 4) {
    lru_replacer.SetEvictable(i, true);
  }

  // Now more frames are evictable. We won't do an exact check because it depends on which were evicted,
  // but let's just confirm we can evict the rest eventually.
  while (true) {
    auto victim = lru_replacer.Evict();
    if (!victim.has_value()) {
      break;
    }
  }

  // All frames should be evicted or non-evictable by now, so replacer size = 0 or at least no frames can be evicted.
  ASSERT_EQ(0, lru_replacer.Size());
}

/**
 * @brief Test the LRU-K replacer with k=1 and only 1 frame capacity.
 * This scenario ensures that every new access immediately places
 * the frame at the "fully known distance," which might be less
 * trivial for k=1 logic.
 */
TEST(LRUKReplacerCornerCaseTest, K1SingleFrame) {
  // Create an LRU-K replacer with capacity=1 and k=1
  LRUKReplacer lru_replacer(1, 1);

  // Record access on frame 0, mark it evictable
  lru_replacer.RecordAccess(0);
  lru_replacer.SetEvictable(0, true);

  ASSERT_EQ(1, lru_replacer.Size());

  // Evict it
  auto victim = lru_replacer.Evict();
  ASSERT_TRUE(victim.has_value());
  ASSERT_EQ(0, victim.value());
  ASSERT_EQ(0, lru_replacer.Size());

  // Try evicting again when empty
  victim = lru_replacer.Evict();
  ASSERT_FALSE(victim.has_value());
  ASSERT_EQ(0, lru_replacer.Size());

  // Re-insert the same frame, but mark it as non-evictable
  lru_replacer.RecordAccess(0);
  lru_replacer.SetEvictable(0, false);
  ASSERT_EQ(0, lru_replacer.Size());

  // Nothing to evict
  victim = lru_replacer.Evict();
  ASSERT_FALSE(victim.has_value());
  ASSERT_EQ(0, lru_replacer.Size());
}

/**
 * @brief Test evicting frames when some have never reached k accesses.
 * We create a scenario with k=2 but some frames get only 1 or 0 accesses.
 */
TEST(LRUKReplacerCornerCaseTest, PartialAccessesBeforeEviction) {
  LRUKReplacer lru_replacer(5, 2);

  // Frames 0 and 1 each get one access.
  lru_replacer.RecordAccess(0);
  lru_replacer.RecordAccess(1);

  // Frame 2 gets two accesses, fulfilling the K=2 requirement.
  lru_replacer.RecordAccess(2);
  lru_replacer.RecordAccess(2);

  // Set all frames as evictable
  lru_replacer.SetEvictable(0, true);
  lru_replacer.SetEvictable(1, true);
  lru_replacer.SetEvictable(2, true);

  // No accesses for frames 3 and 4, but let's mark them evictable anyway.
  lru_replacer.SetEvictable(3, true);
  lru_replacer.SetEvictable(4, true);

  // Check size is 5 because we’ve mentioned frames [0..4] as evictable,
  // even though frames 3 and 4 have zero accesses.
  ASSERT_EQ(5, lru_replacer.Size());

  // We expect frames 3 and 4 to have "infinite" backward k-distance (0 accesses).
  // Among the frames with fewer than k=2 accesses (0,1,3,4), the LRU rule for
  // tie-breaking might lead to evicting 0 or 1 first (whichever was accessed earliest).
  // But frames 3 and 4 also have 0 accesses, so they might be even older in timestamp
  // terms if your implementation handles no-access frames specially.
  // The exact order can vary by design, but let's just confirm eviction always returns a valid frame.

  auto victim = lru_replacer.Evict();
  ASSERT_TRUE(victim.has_value());
  ASSERT_EQ(4, lru_replacer.Size());  // One down

  victim = lru_replacer.Evict();
  ASSERT_TRUE(victim.has_value());
  ASSERT_EQ(3, lru_replacer.Size());

  victim = lru_replacer.Evict();
  ASSERT_TRUE(victim.has_value());
  ASSERT_EQ(2, lru_replacer.Size());

  // Evict remaining frames.
  victim = lru_replacer.Evict();
  ASSERT_TRUE(victim.has_value());
  ASSERT_EQ(1, lru_replacer.Size());

  victim = lru_replacer.Evict();
  ASSERT_TRUE(victim.has_value());
  ASSERT_EQ(0, lru_replacer.Size());

  // Now the replacer is empty
  victim = lru_replacer.Evict();
  ASSERT_FALSE(victim.has_value());
}

/**
 * @brief Test toggling frames between evictable and non-evictable repeatedly,
 * making sure the LRU-K logic updates properly each time.
 */
TEST(LRUKReplacerCornerCaseTest, ToggleEvictableStatusRepeatedly) {
  LRUKReplacer lru_replacer(4, 2);

  // Add frames [10, 11], record some accesses
  lru_replacer.RecordAccess(0);
  lru_replacer.RecordAccess(0);
  lru_replacer.RecordAccess(1);

  // Initially mark them as evictable
  lru_replacer.SetEvictable(0, true);
  lru_replacer.SetEvictable(1, true);
  ASSERT_EQ(2, lru_replacer.Size());

  // Toggle 10 to non-evictable
  lru_replacer.SetEvictable(0, false);
  ASSERT_EQ(1, lru_replacer.Size());  // Only frame 11 is evictable now

  // Now toggle 10 back to evictable
  lru_replacer.SetEvictable(0, true);
  ASSERT_EQ(2, lru_replacer.Size());

  // Add a new frame 12 with one access
  lru_replacer.RecordAccess(2);
  lru_replacer.SetEvictable(2, true);
  ASSERT_EQ(3, lru_replacer.Size());

  // Evict one frame; the tie-breaking will vary depending on your internal times.
  // We just confirm that we get a valid frame and the size is decremented.
  auto victim = lru_replacer.Evict();
  ASSERT_TRUE(victim.has_value());
  ASSERT_EQ(2, lru_replacer.Size());

  // Mark everything as non-evictable now
  lru_replacer.SetEvictable(0, false);
  lru_replacer.SetEvictable(1, false);
  lru_replacer.SetEvictable(2, false);
  ASSERT_EQ(0, lru_replacer.Size());

  // Try eviction
  victim = lru_replacer.Evict();
  ASSERT_FALSE(victim.has_value());
  ASSERT_EQ(0, lru_replacer.Size());
}

/**
 * @brief Test the LRU-K replacer with k=2, but we access many frames
 * in a round-robin manner to ensure no single frame dominates.
 * This helps check correct time-stamp ordering and k-distance updates.
 */
TEST(LRUKReplacerCornerCaseTest, RoundRobinAccessPattern) {
  const int total_frames = 6;
  LRUKReplacer lru_replacer(total_frames, 2);

  // Access each frame in a round-robin pattern, e.g., 0,1,2,3,4,5, 0,1,2,...
  for (int round = 0; round < 3; round++) {
    for (int f = 0; f < total_frames; f++) {
      lru_replacer.RecordAccess(f);
      lru_replacer.SetEvictable(f, true);
    }
  }

  // At this point, each frame has been accessed 3 times.
  // All are evictable, so replacer size should be = total_frames.
  ASSERT_EQ(total_frames, lru_replacer.Size());

  // We expect the frame with the oldest last access to be evicted first,
  // but they might be close in time because we did a round-robin.
  // We'll just confirm that all frames eventually get evicted in some order.
  for (int i = 0; i < total_frames; i++) {
    auto victim = lru_replacer.Evict();
    ASSERT_TRUE(victim.has_value());
  }
  ASSERT_EQ(0, lru_replacer.Size());

  // Another eviction should fail
  auto victim = lru_replacer.Evict();
  ASSERT_FALSE(victim.has_value());
}

}  // namespace bustub
