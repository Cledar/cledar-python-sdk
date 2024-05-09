#define SPDLOG_ACTIVE_LEVEL SPDLOG_LEVEL_TRACE

#include <librdkafka/rdkafkacpp.h>
#include <spdlog/fmt/ranges.h>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <boost/program_options.hpp>
#include <iostream>
#include <string>

#include "fingerprint.h"
#include "utils/kafka_workers.h"

constexpr const char *DEFAULT_OUTPUT_TOPIC = "fingerprint";
const std::string FFMPEG_CMD("ffmpeg ");
const std::string INPUT_FLAG("-i ");
const std::string OUTPUT_FORMAT(" -f s16le -acodec pcm_s16le -ac 1 pipe:1");
#ifdef _DEBUG
const std::string FFMPEG_VERBOSITY("");
#else
const std::string FFMPEG_VERBOSITY(" -loglevel warning");
#endif

namespace po = boost::program_options;

class StreamFingerprinterConfig {
 protected:
  std::string kafka_address_, audio_source_, station_id_, channel_,
      fingerprint_topic_;
  size_t sframe_size_;
  int buffer_read_, step_size_, ts_;
  std::vector<int> offsets_;

 public:
  const std::string &kafka_address() const { return kafka_address_; }
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
    po::options_description desc("Allowed options");
    // clang-format off
    desc.add_options()(
        "kafka-address", po::value<std::string>(&kafka_address_),
          "Kafka broker address")(
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
              ->default_value(DEFAULT_OUTPUT_TOPIC),
          "Topic on Kafka to send fingerprints to");
    // clang-format on
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

  void check_arguments(po::variables_map &vm) {
    if (vm.count("kafka-address") == 0) {
      SPDLOG_ERROR("Kafka broker address is required");
      throw po::invalid_option_value(
          "Kafka broker address is required");  // TODO(kkrol): Make it optional
                                                // but add serious log if not
                                                // provided
    }
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

    SPDLOG_INFO(
        "Config:\n Kafka Address: {}\n Audio Source: {}\n sframe size: {}\n "
        "buffer size: {}\n step size: {}\n Station ID: {}\n Channel: {}\n "
        "Topic for Fingerprints: {}\n Initial Timestamp: {}\n Offsets: {}",
        kafka_address_, audio_source_, sframe_size_, buffer_read_, step_size_,
        station_id_, channel_, fingerprint_topic_, ts_, offsets_);
  }
};

class OverlappedStreamReader {
 private:
  bool requires_shift_ = false;

 protected:
  size_t carry_over_;
  int buffer_read_;
  std::string command_;
  std::unique_ptr<FILE, decltype(&pclose)> pipe_;

 public:
  OverlappedStreamReader(const std::string &audio_source, size_t sf_size,
                         int buf_read)
      : carry_over_(sf_size - 1),
        buffer_read_(buf_read),
        command_(FFMPEG_CMD + INPUT_FLAG + audio_source + FFMPEG_VERBOSITY +
                 OUTPUT_FORMAT),
        pipe_(popen(command_.c_str(), "r"), &pclose) {
    if (!pipe_) {
      SPDLOG_CRITICAL("Error opening ffmpeg pipe");
      throw std::ios_base::failure("Error opening ffmpeg pipe");
    }
  }

  std::vector<sample_t> init_buffer() {
    std::vector<sample_t> sample_buffer(buffer_read_ + carry_over_);
    size_t n_samples_read =
        fread(sample_buffer.data(), sizeof(sample_t), carry_over_, pipe_.get());
    if (n_samples_read != carry_over_) {
      SPDLOG_WARN("Initial read too small. {} samples read; expected {}",
                  n_samples_read, carry_over_);
    }
    return sample_buffer;
  }

  std::span<sample_t> read_samples(std::vector<sample_t> &sample_buffer) {
    if (requires_shift_) {
      std::shift_left(sample_buffer.begin(), sample_buffer.end(), buffer_read_);
    }
    size_t n_samples_read = fread(&sample_buffer[carry_over_], sizeof(sample_t),
                                  buffer_read_, pipe_.get());
    requires_shift_ = true;
    if (n_samples_read == 0) {
      if (feof(pipe_.get())) {
        SPDLOG_INFO("ffmpeg reached EOF - shutting down");
      } else if (ferror(pipe_.get())) {
        SPDLOG_CRITICAL("Failed reading from ffmpeg pipe");
        throw std::ios_base::failure("Failed reading from ffmpeg pipe");
      }
    }
    return std::span(sample_buffer).first(n_samples_read + carry_over_);
  }
};

namespace {
void set_logging_level() {  // TODO(kkrol): loading log levels from argv or
                            // environment var
#ifdef _DEBUG
  spdlog::set_level(spdlog::level::debug);
#else
  spdlog::set_level(spdlog::level::info);
#endif
}
}  // namespace

int main(int argc, char *argv[]) {
  set_logging_level();
  StreamFingerprinterConfig config(argc, argv);
  KafkaProducer producer(config.kafka_address());

  std::vector<fingerprint_t> fingerprints;
  fingerprints.reserve(PFRAME_CELLS * MAGIC_100);
  std::vector<Fingerprinter> fingerprinters;
  fingerprinters.reserve(config.offsets().size());
  for (size_t i = 0; i < config.offsets().size(); i++) {
    fingerprinters.emplace_back(config.sframe_size(), config.step_size(),
                                config.ts());
  }
  OverlappedStreamReader reader(config.audio_source(), config.sframe_size(),
                                config.buffer_read());
  std::vector<sample_t> sample_buffer = reader.init_buffer();

  while (true) {
    std::span<sample_t> samples = reader.read_samples(sample_buffer);
    if (samples.size() < config.sframe_size()) {
      break;
    }
    for (size_t i = 0; i < config.offsets().size(); i++) {
      fingerprints.clear();
      fingerprinters[i].get_fingerprints(samples.subspan(config.offsets()[i]),
                                         fingerprints);
      for (auto &fingerprint : fingerprints) {
        producer.produce(
            dump_fingerprint(fingerprint, config.ts(), config.channel(),
                             config.station_id()),
            config.station_id(), config.fingerprint_topic());
      }
    }
  }

  producer.flush();

  return 0;
}