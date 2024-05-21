#define SPDLOG_ACTIVE_LEVEL SPDLOG_LEVEL_TRACE

#include <spdlog/spdlog.h>

#include <algorithm>
#include <boost/program_options.hpp>
#include <csignal>
#include <iostream>
#include <nlohmann/json.hpp>
#include <string>

#include "fingerprint.h"
#include "utils/config.h"
#include "utils/kafka_workers.h"

namespace {
volatile std::sig_atomic_t signal_status;
void signal_handler(int signal) { signal_status = signal; }
}  // namespace

std::span<peak_t> retrieve_peaks(nlohmann::json &payload_parsed,
                                 const std::string &peaks_key,
                                 std::span<peak_t> peaks_buff) {
  std::vector<int> compressed_peaks;
  try {
    compressed_peaks = payload_parsed.at(peaks_key).get<std::vector<int>>();
  } catch (const nlohmann::json::out_of_range &e) {
    SPDLOG_WARN("The key {} is not stored in the object: [Exception ID:{}] {}",
                peaks_key, e.id, e.what());
  } catch (const nlohmann::json::type_error &e) {
    SPDLOG_WARN("Received JSON value is not an object: [Exception ID:{}] {}",
                e.id, e.what());
  }
  if (compressed_peaks.size() > peaks_buff.size()) {
    SPDLOG_WARN("Too many peaks in the payload, dropping excess.");
  }
  size_t n_peaks_out = std::min(compressed_peaks.size(), peaks_buff.size());
  int cumulative_time = 0;
  for (size_t peaks_it = 0; peaks_it < n_peaks_out; peaks_it++) {
    cumulative_time += compressed_peaks[peaks_it] / 2048;
    peaks_buff[peaks_it].time = cumulative_time;
    peaks_buff[peaks_it].freq = abs(compressed_peaks[peaks_it] % 2048);
  }

  (void)payload_parsed.erase(peaks_key);
  return peaks_buff.first(n_peaks_out);
}

int main(int argc, char *argv[]) {
  std::signal(SIGINT, signal_handler);
  ProsumerConfig config(argc, argv);
  KafkaTopicProducer producer(config.kafka_address(), config.output_topic());
  KafkaConsumer consumer(config.kafka_address(), config.consumer_group(),
                         config.input_topic());

  std::array<peak_t, PFRAME_CELLS * MAGIC_100>
      peaksbuff;  // TODO(kkrol): Reason for such size?
  std::vector<fingerprint_t> fingerprints;
  fingerprints.reserve(PFRAME_CELLS * MAGIC_100);

  while (!signal_status) {
    std::unique_ptr<RdKafka::Message> message = consumer.get_message();

    if (message) {
      auto payload = std::string(static_cast<const char *>(message->payload()),
                                 message->len());
      nlohmann::json payload_parsed = nlohmann::json::parse(payload);
      std::span<peak_t> payload_peaks =
          retrieve_peaks(payload_parsed, config.peaks_key(), peaksbuff);
      fingerprints.clear();
      (void)peaks_to_fingerprints(payload_peaks, fingerprints, 0, {1, 0});
      for (auto &fingerprint : fingerprints) {
        producer.produce(dump_fingerprint(fingerprint, payload_parsed),
                         *message->key());
      }
      break;
    }
  }

  SPDLOG_INFO("Prosumer interrupted - gracefuly shutting down");

  producer.flush();

  return 0;
}