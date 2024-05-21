#include <gtest/gtest.h>

#include "fingerprint.h"

TEST(JSONTest, BasicTest) {
  fingerprint_t fingerprint{
      .hash_1 = "hash1", .hash_2 = "hash2", .ts = 33, .offset = 6};

  nlohmann::json basic_info;
  basic_info["org_ts"] = 0;
  basic_info["channel"] = "channel";
  basic_info["station_id"] = "station_id";

  std::string json_fingerprint = dump_fingerprint(fingerprint, basic_info);
  std::string expected_json_fingerprint =
      "{\"channel\":\"channel\",\"hash_1\":\"hash1\",\"hash_2\":\"hash2\","
      "\"offset\":6,\"org_ts\":0,\"station_id\":\"station_id\",\"ts\":33}";

  EXPECT_EQ(json_fingerprint, expected_json_fingerprint);
}