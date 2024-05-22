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

TEST(PeaksToFingerprintTest, BasicTest) {
  std::vector<fingerprint_t> fingerprints;
  std::array<peak_t, 6> peaks;
  for (int i = 1; i <= 6; i++) {
    peaks[i - 1].freq = i * i;
    peaks[i - 1].time = i * i;
  }

  peaks_to_fingerprints(peaks, fingerprints, 0, {.freq = 1, .time = 0});

  std::vector<fingerprint_t> expected_fingerprints;
  expected_fingerprints.push_back(
      {.hash_1 = "1:1:1", .hash_2 = "0:0:1", .ts = 0, .offset = 1});
  expected_fingerprints.push_back(
      {.hash_1 = "1:4:3", .hash_2 = "0:13:3", .ts = 0, .offset = 4});
  expected_fingerprints.push_back(
      {.hash_1 = "4:9:5", .hash_2 = "13:21:5", .ts = 0, .offset = 9});
  expected_fingerprints.push_back(
      {.hash_1 = "9:16:7", .hash_2 = "21:27:7", .ts = 1, .offset = 16});
  expected_fingerprints.push_back(
      {.hash_1 = "16:25:9", .hash_2 = "27:32:9", .ts = 2, .offset = 25});
  expected_fingerprints.push_back(
      {.hash_1 = "25:36:11", .hash_2 = "32:35:11", .ts = 3, .offset = 36});

  EXPECT_EQ(fingerprints.size(), expected_fingerprints.size());
  for (size_t i = 0; i < fingerprints.size(); i++) {
    EXPECT_EQ(fingerprints[i].hash_1, expected_fingerprints[i].hash_1);
    EXPECT_EQ(fingerprints[i].hash_2, expected_fingerprints[i].hash_2);
    EXPECT_EQ(fingerprints[i].ts, expected_fingerprints[i].ts);
    EXPECT_EQ(fingerprints[i].offset, expected_fingerprints[i].offset);
  }
}

TEST(PeaksToFingerprintTest, FreqThreshold) {
  std::vector<fingerprint_t> fingerprints;
  std::array<peak_t, 10> peaks;
  for (int i = 1; i <= 10; i++) {
    peaks[i - 1].freq = 1 - (i % 3);
    peaks[i - 1].time = i * i;
  }

  peaks_to_fingerprints(peaks, fingerprints, 0, {.freq = 1, .time = 0});

  std::vector<fingerprint_t> expected_fingerprints;
  expected_fingerprints.push_back(
      {.hash_1 = "1:1:9", .hash_2 = "0:0:9", .ts = 0, .offset = 9});
  expected_fingerprints.push_back(
      {.hash_1 = "1:1:27", .hash_2 = "0:0:27", .ts = 3, .offset = 36});
  expected_fingerprints.push_back(
      {.hash_1 = "1:1:45", .hash_2 = "0:0:45", .ts = 7, .offset = 81});

  EXPECT_EQ(fingerprints.size(), expected_fingerprints.size());
  for (size_t i = 0; i < fingerprints.size(); i++) {
    EXPECT_EQ(fingerprints[i].hash_1, expected_fingerprints[i].hash_1);
    EXPECT_EQ(fingerprints[i].hash_2, expected_fingerprints[i].hash_2);
    EXPECT_EQ(fingerprints[i].ts, expected_fingerprints[i].ts);
    EXPECT_EQ(fingerprints[i].offset, expected_fingerprints[i].offset);
  }
}

TEST(PeaksToFingerprintTest, Hash2) {
  std::vector<fingerprint_t> fingerprints;
  std::array<peak_t, 6> peaks;
  peaks[0] = {.freq = 100, .time = 4};
  peaks[1] = {.freq = 200, .time = 4};
  peaks[2] = {.freq = 400, .time = 4};
  peaks[3] = {.freq = 800, .time = 4};
  peaks[4] = {.freq = 1600, .time = 4};
  peaks[5] = {.freq = 3200, .time = 4};

  peaks_to_fingerprints(peaks, fingerprints, 0, {.freq = 1, .time = 2});

  std::vector<fingerprint_t> expected_fingerprints;
  expected_fingerprints.push_back(
      {.hash_1 = "1:100:2", .hash_2 = "0:46:2", .ts = 0, .offset = 4});
  expected_fingerprints.push_back(
      {.hash_1 = "100:200:0", .hash_2 = "46:52:0", .ts = 0, .offset = 4});
  expected_fingerprints.push_back(
      {.hash_1 = "200:400:0", .hash_2 = "52:59:0", .ts = 0, .offset = 4});
  expected_fingerprints.push_back(
      {.hash_1 = "400:800:0", .hash_2 = "59:66:0", .ts = 0, .offset = 4});
  expected_fingerprints.push_back(
      {.hash_1 = "800:1600:0", .hash_2 = "66:73:0", .ts = 0, .offset = 4});
  expected_fingerprints.push_back(
      {.hash_1 = "1600:3200:0", .hash_2 = "73:80:0", .ts = 0, .offset = 4});

  EXPECT_EQ(fingerprints.size(), expected_fingerprints.size());
  for (size_t i = 0; i < fingerprints.size(); i++) {
    EXPECT_EQ(fingerprints[i].hash_1, expected_fingerprints[i].hash_1);
    EXPECT_EQ(fingerprints[i].hash_2, expected_fingerprints[i].hash_2);
    EXPECT_EQ(fingerprints[i].ts, expected_fingerprints[i].ts);
    EXPECT_EQ(fingerprints[i].offset, expected_fingerprints[i].offset);
  }
}

// TODO(kkrol): Uncomment when bug fixed
// TEST(PeaksToFingerprintTest, LastPeakWrongFreq) {
//   std::vector<fingerprint_t> fingerprints;
//   std::array<peak_t, 10> peaks;
//   for (int i = 0; i < 10; i++) {
//     peaks[i].freq = i + 1;
//     peaks[i].time = i;
//   }

//   peaks_to_fingerprints(peaks, fingerprints, 0, {.freq = 0, .time = 0});
//   EXPECT_EQ(fingerprints[0].hash_2, "0:0:0");

//   fingerprints.clear();
//   peaks_to_fingerprints(peaks, fingerprints, 0, {.freq = -1, .time = 0});
//   EXPECT_EQ(fingerprints[0].hash_2, "0:0:0");
// }
