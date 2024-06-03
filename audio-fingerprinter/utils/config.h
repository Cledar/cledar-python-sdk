#ifndef AUDIO_FINGERPRINTER_UTILS_CONFIG_H_
#define AUDIO_FINGERPRINTER_UTILS_CONFIG_H_
#define FMT_HEADER_ONLY
#include <spdlog/fmt/ranges.h>
#include <spdlog/spdlog.h>

#include <boost/program_options.hpp>

#include "fingerprint.h"

constexpr const char *FINGERPRINTER_DEFAULT_OUTPUT_TOPIC = "fingerprints";
constexpr const char *PROSUMER_DEFAULT_OUTPUT_TOPIC =
    "ambient-audio-fingerprints";
constexpr const char *DEFAULT_INPUT_TOPIC = "ambient-audio-fingerprints-raw";
constexpr const char *DEFAULT_PEAKS_JSON_KEY = "channel1";

namespace po = boost::program_options;

void set_default_logging_level() {
#ifdef _DEBUG
  spdlog::set_level(spdlog::level::debug);
#else
  spdlog::set_level(spdlog::level::info);
#endif  // _DEBUG
}

class CommonConfig {
 protected:
  std::string kafka_address_;

  po::options_description get_description() {
    po::options_description desc("Allowed options");

    // clang-format off
    desc.add_options()(
        "kafka-address", po::value<std::string>(&kafka_address_),
          "Kafka broker address")(
        "spdlog-level", po::value<std::string>(),
          "spdlog logging threshold level");
    // clang-format on
    return desc;
  }

  void check_arguments(po::variables_map &vm) {
    if (vm.count("spdlog-level") == 0) {
      set_default_logging_level();
      SPDLOG_INFO("Log level set to default - {}",
                  spdlog::level::to_string_view(spdlog::get_level()));
    } else {
      spdlog::set_level(
          spdlog::level::from_str(vm["spdlog-level"].as<std::string>()));
      if (spdlog::get_level() == spdlog::level::off &&
          vm["spdlog-level"].as<std::string>() != "off") {
        set_default_logging_level();
        SPDLOG_INFO("Unknown log level {}; set to default - {}",
                    vm["spdlog-level"].as<std::string>(),
                    spdlog::level::to_string_view(spdlog::get_level()));
      } else {
        SPDLOG_INFO("Log level set to {}",
                    spdlog::level::to_string_view(spdlog::get_level()));
      }
    }

    if (vm.count("kafka-address") == 0) {
      SPDLOG_ERROR("Kafka broker address is required");
      throw po::invalid_option_value("Kafka broker address is required");
    }
  }

 public:
  const std::string &kafka_address() const { return kafka_address_; }
};

class StreamFingerprinterConfig : public CommonConfig {
 protected:
  std::string audio_source_, station_id_, channel_, fingerprint_topic_;
  size_t sframe_size_;
  int buffer_read_, step_size_, ts_;
  std::vector<int> offsets_;
  po::options_description get_description() {
    po::options_description desc = CommonConfig::get_description();
    // clang-format off
    desc.add_options()(
        "audio-source", po::value<std::string>(&audio_source_),
          "Audio source file")(
        "sframe-size",
          po::value<size_t>(&sframe_size_)->default_value(SFRAME_IN_SIZE),
          "Number of samples in each fft frame")(
        "buffer-size", po::value<int>(&buffer_read_),
          "Read size to buffer (default 10x sframe size)")(
        "offset", po::value<std::vector<int>>(&offsets_)->multitoken(),
          "Offsets to add to the vector")(
        "step-size",
          po::value<int>(&step_size_)->default_value(SFRAME_IN_SIZE / 2),
          "Distance between two sframes")(
        "start-ts", po::value<int>(&ts_)->default_value(0),
          "Initial timestamp")(
        "channel", po::value<std::string>(&channel_), "Name of that channel")(
        "station-id", po::value<std::string>(&station_id_), "Station ID")(
        "fingerprint-topic",
          po::value<std::string>(&fingerprint_topic_)
              ->default_value(FINGERPRINTER_DEFAULT_OUTPUT_TOPIC),
          "Topic on Kafka to send fingerprints to");
    // clang-format on
    return desc;
  }

  void check_arguments(po::variables_map &vm) {
    CommonConfig::check_arguments(vm);
    if (vm.count("audio-source") == 0) {
      SPDLOG_ERROR("Audio source is required");
      throw po::invalid_option_value("Audio source is required");
    }
    if (vm.count("channel") == 0) {
      SPDLOG_DEBUG("Channel not provided. Using audio source as channel");
      channel_ = audio_source_;
    }
    if (vm.count("station-id") == 0) {
      SPDLOG_DEBUG("Station ID not provided. Using audio source as station ID");
      station_id_ = audio_source_;
    }
    if (vm.count("buffer-size") == 0) {
      SPDLOG_DEBUG("Buffer size not provided. Using {}x sframe size",
                   READ_BUFFER_MULT);
      buffer_read_ = static_cast<int>(sframe_size_) * READ_BUFFER_MULT;
    }
    if (buffer_read_ % step_size_ != 0) {
      SPDLOG_ERROR("Buffer read must be multiple of step size");
      throw po::invalid_option_value(
          "Buffer read must be multiple of step size");
    }
    if (offsets_.empty()) {
      SPDLOG_DEBUG("Offsets not provided. Using 0 and step size / 2");
      offsets_ = {0, step_size_ / 2};
    }

    std::string offsets_str(offsets_.begin(), offsets_.end());
    SPDLOG_INFO(
        "Config:\n Kafka Address: {}\n Audio Source: {}\n sframe size: {}\n "
        "buffer size: {}\n step size: {}\n Station ID: {}\n Channel: {}\n "
        "Topic for Fingerprints: {}\n Initial Timestamp: {}\n Offsets: {}",
        kafka_address_, audio_source_, sframe_size_, buffer_read_, step_size_,
        station_id_, channel_, fingerprint_topic_, ts_, offsets_str);
  }

 public:
  const std::string &audio_source() const { return audio_source_; }
  const std::string &channel() const { return channel_; }
  const std::string &station_id() const { return station_id_; }
  const std::string &fingerprint_topic() const { return fingerprint_topic_; }
  size_t sframe_size() const { return sframe_size_; }
  int buffer_read() const { return buffer_read_; }
  int ts() const { return ts_; }
  int step_size() const { return step_size_; }
  const std::vector<int> &offsets() const { return offsets_; }

  StreamFingerprinterConfig(int argc, char *argv[]) {  // NOLINT

    po::options_description desc = get_description();
    po::positional_options_description positionalOptions;
    positionalOptions.add("audio-source", 1);

    po::variables_map vm;
    po::store(po::command_line_parser(argc, argv)
                  .options(desc)
                  .positional(positionalOptions)
                  .run(),
              vm);
    po::notify(vm);

    check_arguments(vm);
  }
};

class ProsumerConfig : public CommonConfig {
 protected:
  std::string output_topic_, input_topic_, consumer_group_, peaks_key_;
  po::options_description get_description() {
    po::options_description desc = CommonConfig::get_description();
    // clang-format off
    desc.add_options()(
        "output-topic",
          po::value<std::string>(&output_topic_)
              ->default_value(PROSUMER_DEFAULT_OUTPUT_TOPIC),
          "Topic on Kafka to send parsed fingerprints to")(
        "input-topic",
          po::value<std::string>(&input_topic_)
              ->default_value(DEFAULT_INPUT_TOPIC),
          "Topic on Kafka from which to read raw fingerprints")(
        "consumer-group", po::value<std::string>(&consumer_group_),
          "Name of consumer group of a given consumer")(
        "peaks-key",
          po::value<std::string>(&peaks_key_)
              ->default_value(DEFAULT_PEAKS_JSON_KEY),
          "Name of the key in the JSON payload that contains peaks");
    // clang-format on
    return desc;
  }

  void check_arguments(po::variables_map &vm) {
    CommonConfig::check_arguments(vm);
    if (vm.count("consumer-group") == 0) {
      SPDLOG_ERROR("Consumer group name is required");
      throw po::invalid_option_value("Consumer group name is required");
    }
  }

 public:
  inline const std::string &output_topic() const { return output_topic_; }
  inline const std::string &input_topic() const { return input_topic_; }
  inline const std::string &consumer_group() const { return consumer_group_; }
  inline const std::string &peaks_key() const { return peaks_key_; }

  ProsumerConfig(int argc, char *argv[]) {  // NOLINT
    po::options_description desc = get_description();
    po::variables_map vm;
    po::store(po::command_line_parser(argc, argv).options(desc).run(), vm);
    po::notify(vm);

    check_arguments(vm);

    SPDLOG_INFO(
        "Config:\n Kafka Address: {}\n Output Topic: {}\n Input Topic: {}\n"
        "Consumer Group: {}",
        kafka_address_, output_topic_, input_topic_, consumer_group_);
  }
};

#endif  // AUDIO_FINGERPRINTER_UTILS_CONFIG_H_