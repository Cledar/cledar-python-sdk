#define SPDLOG_ACTIVE_LEVEL SPDLOG_LEVEL_TRACE
#include <gtest/gtest.h>

#include <boost/program_options.hpp>

#include "../utils/config.h"

namespace po = boost::program_options;

TEST(ConfigTest, MinimalisticConfig) {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
  // NOLINTBEGIN(clang-diagnostic-writable-strings)
  char *argv[] = {"program_name", "--kafka-address", "kafka-address",
                  "--audio-source", "audio-source"};
  // NOLINTEND(clang-diagnostic-writable-strings)
#pragma GCC diagnostic pop
  int argc = sizeof(argv) / sizeof(char *);
  StreamFingerprinterConfig config(argc, argv);

  EXPECT_EQ(config.kafka_address(), "kafka-address");
  EXPECT_EQ(config.audio_source(), "audio-source");
  EXPECT_EQ(config.channel(), "audio-source");
  EXPECT_EQ(config.station_id(), "audio-source");
  EXPECT_EQ(config.fingerprint_topic(), "fingerprint");
  EXPECT_EQ(config.sframe_size(), 8192);
  EXPECT_EQ(config.buffer_read(), 81920);
  EXPECT_EQ(config.ts(), 0);
  EXPECT_EQ(config.step_size(), 4096);
  EXPECT_EQ(config.offsets().size(), 2);
  EXPECT_EQ(config.offsets()[0], 0);
  EXPECT_EQ(config.offsets()[1], 2048);
  EXPECT_EQ(spdlog::get_level(), spdlog::level::debug);
}

TEST(ConfigTest, MissingKafkaAddress) {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
  // NOLINTBEGIN(clang-diagnostic-writable-strings)
  char *argv[] = {"program_name", "--audio-source", "audio-source"};
  // NOLINTEND(clang-diagnostic-writable-strings)
#pragma GCC diagnostic pop
  int argc = sizeof(argv) / sizeof(char *);
  EXPECT_THROW(StreamFingerprinterConfig config(argc, argv),
               po::invalid_option_value);
}

TEST(ConfigTest, MissingAudioSource) {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
  // NOLINTBEGIN(clang-diagnostic-writable-strings)
  char *argv[] = {"program_name", "--kafka-address", "kafka-address"};
  // NOLINTEND(clang-diagnostic-writable-strings)
#pragma GCC diagnostic pop
  int argc = sizeof(argv) / sizeof(char *);
  EXPECT_THROW(StreamFingerprinterConfig config(argc, argv),
               po::invalid_option_value);
}

TEST(ConfigTest, PositionalAudioSource) {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
  // NOLINTBEGIN(clang-diagnostic-writable-strings)
  char *argv[] = {"program_name", "--kafka-address", "kafka-address",
                  "audio-source"};
  // NOLINTEND(clang-diagnostic-writable-strings)
#pragma GCC diagnostic pop
  int argc = sizeof(argv) / sizeof(char *);
  StreamFingerprinterConfig config(argc, argv);
  EXPECT_EQ(config.audio_source(), "audio-source");
}

TEST(ConfigTest, SetAllOptions) {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
  // NOLINTBEGIN(clang-diagnostic-writable-strings)
  char *argv[] = {"program_name",
                  "--kafka-address",
                  "kafka-address",
                  "--audio-source",
                  "audio-source",
                  "--channel",
                  "channel",
                  "--station-id",
                  "station-id",
                  "--fingerprint-topic",
                  "fingerprint-topic",
                  "--sframe-size",
                  "234",
                  "--buffer-size",
                  "1000",
                  "--offset",
                  "300",
                  "400",
                  "450",
                  "--step-size",
                  "500",
                  "--start-ts",
                  "600",
                  "--spdlog-level",
                  "warn"};
  // NOLINTEND(clang-diagnostic-writable-strings)
#pragma GCC diagnostic pop
  int argc = sizeof(argv) / sizeof(char *);
  StreamFingerprinterConfig config(argc, argv);

  EXPECT_EQ(config.kafka_address(), "kafka-address");
  EXPECT_EQ(config.audio_source(), "audio-source");
  EXPECT_EQ(config.channel(), "channel");
  EXPECT_EQ(config.station_id(), "station-id");
  EXPECT_EQ(config.fingerprint_topic(), "fingerprint-topic");
  EXPECT_EQ(config.sframe_size(), 234);
  EXPECT_EQ(config.buffer_read(), 1000);
  EXPECT_EQ(config.ts(), 600);
  EXPECT_EQ(config.step_size(), 500);
  EXPECT_EQ(config.offsets().size(), 3);
  EXPECT_EQ(config.offsets()[0], 300);
  EXPECT_EQ(config.offsets()[1], 400);
  EXPECT_EQ(config.offsets()[2], 450);
  EXPECT_EQ(spdlog::get_level(), spdlog::level::warn);
}

TEST(ConfigTest, BufferNotMultiple) {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
  // NOLINTBEGIN(clang-diagnostic-writable-strings)
  char *argv[] = {"program_name",   "--kafka-address", "kafka-address",
                  "--audio-source", "audio-source",    "--buffer-size",
                  "1001",           "--step-size",     "500"};
  // NOLINTEND(clang-diagnostic-writable-strings)
#pragma GCC diagnostic pop
  int argc = sizeof(argv) / sizeof(char *);
  EXPECT_THROW(StreamFingerprinterConfig config(argc, argv),
               po::invalid_option_value);
}

TEST(ConfigTest, UnknownOption) {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
  // NOLINTBEGIN(clang-diagnostic-writable-strings)
  char *argv[] = {"program_name",   "--kafka-address", "kafka-address",
                  "--audio-source", "audio-source",    "--test"};
  // NOLINTEND(clang-diagnostic-writable-strings)
#pragma GCC diagnostic pop
  int argc = sizeof(argv) / sizeof(char *);
  EXPECT_THROW(StreamFingerprinterConfig config(argc, argv),
               po::unknown_option);
}

TEST(ConfigTest, CustomDefaults) {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
  // NOLINTBEGIN(clang-diagnostic-writable-strings)
  char *argv[] = {
      "program_name", "--kafka-address", "kafka-address", "--audio-source",
      "audio-source", "--sframe-size",   "128",           "--step-size",
      "80",           "--spdlog-level",  "misspell"};
  // NOLINTEND(clang-diagnostic-writable-strings)
#pragma GCC diagnostic pop
  int argc = sizeof(argv) / sizeof(char *);
  StreamFingerprinterConfig config(argc, argv);
  EXPECT_EQ(config.buffer_read(), 1280);
  EXPECT_EQ(config.offsets().size(), 2);
  EXPECT_EQ(config.offsets()[0], 0);
  EXPECT_EQ(config.offsets()[1], 40);
  EXPECT_EQ(spdlog::get_level(), spdlog::level::debug);
}