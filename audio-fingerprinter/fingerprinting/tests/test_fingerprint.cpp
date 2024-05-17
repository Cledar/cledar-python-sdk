#include <gtest/gtest.h>

#include "fingerprint.h"

TEST(AdditionTest, TwoPlusTwoEqualsFour) {
  fingerprint_t hash;
  EXPECT_NE(hash.ts, 4) << "Default ts of hash should not be equal to 4";
}