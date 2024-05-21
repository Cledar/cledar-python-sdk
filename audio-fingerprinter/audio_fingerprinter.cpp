#define SPDLOG_ACTIVE_LEVEL SPDLOG_LEVEL_TRACE

#include <librdkafka/rdkafkacpp.h>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <boost/program_options.hpp>
#include <iostream>
#include <string>

#include "fingerprint.h"
#include "utils/config.h"
#include "utils/kafka_workers.h"

const std::string FFMPEG_CMD("ffmpeg ");
const std::string INPUT_FLAG("-i ");
const std::string OUTPUT_FORMAT(" -f s16le -acodec pcm_s16le -ac 1 pipe:1");
#ifdef _DEBUG
const std::string FFMPEG_VERBOSITY("");
#else
const std::string FFMPEG_VERBOSITY(" -loglevel warning");
#endif

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

int main(int argc, char *argv[]) {
  set_default_logging_level();
  StreamFingerprinterConfig config(argc, argv);
  nlohmann::json basic_info;
  basic_info["org_ts"] = config.ts();
  basic_info["channel"] = config.channel();
  basic_info["station_id"] = config.station_id();
  KafkaTopicProducer producer(config.kafka_address(),
                              config.fingerprint_topic());

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
        producer.produce(dump_fingerprint(fingerprint, basic_info),
                         config.station_id());
      }
    }
  }

  producer.flush();

  return 0;
}