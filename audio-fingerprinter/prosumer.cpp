#include <algorithm>
#include <boost/program_options.hpp>
#include <csignal>
#include <iostream>
#include <string>

#include "fingerprint.h"
#include "utils/kafka_workers.h"

constexpr const char *DEFAULT_OUTPUT_TOPIC = "ambient-audio-fingerprints";
constexpr const char *DEFAULT_INPUT_TOPIC = "ambient-audio-fingerprints-raw";

namespace {
volatile std::sig_atomic_t signal_status;
void signal_handler(int signal) { signal_status = signal; }
}  // namespace

namespace po = boost::program_options;

class ProsumerConfig {
 protected:
  std::string kafka_address_, output_topic_, input_topic_, consumer_group_;

 public:
  inline const std::string &kafka_address() const { return kafka_address_; }
  inline const std::string &output_topic() const { return output_topic_; }
  inline const std::string &input_topic() const { return input_topic_; }
  inline const std::string &consumer_group() const { return consumer_group_; }
  ProsumerConfig(int argc, char *argv[]) {  // NOLINT
    po::options_description desc("Allowed options");
    // clang-format off
    desc.add_options()(
        "kafka-address", po::value<std::string>(&kafka_address_),
          "Kafka broker address")(
        "output-topic",
          po::value<std::string>(&output_topic_)
              ->default_value(DEFAULT_OUTPUT_TOPIC),
          "Topic on Kafka to send parsed fingerprints to")(
        "input-topic",
          po::value<std::string>(&input_topic_)
              ->default_value(DEFAULT_INPUT_TOPIC),
          "Topic on Kafka from which to read raw fingerprints")(
        "consumer-group", po::value<std::string>(&consumer_group_),
          "Name of consumer group of a given consumer");
    // clang-format on
    po::variables_map vm;
    po::store(po::command_line_parser(argc, argv).options(desc).run(), vm);
    po::notify(vm);

    check_arguments(vm);
  }

  void check_arguments(po::variables_map &vm) {
    if (vm.count("kafka-address") == 0) {
      throw po::invalid_option_value("Kafka broker address is required");
    }
    if (vm.count("consumer-group") == 0) {
      throw po::invalid_option_value("Consumer group name is required");
    }

    // TODO(kkrol): spdlog::INFO/DEBUG arguments
  }
};

int main(int argc, char *argv[]) {
  std::signal(SIGINT, signal_handler);
  ProsumerConfig config(argc, argv);
  KafkaProducer producer(config.kafka_address());
  KafkaConsumer consumer(config.kafka_address(), config.consumer_group(),
                         config.input_topic());

  std::vector<fingerprint_t> fingerprints;
  fingerprints.reserve(PFRAME_CELLS * MAGIC_100);

  while (!signal_status) {
    std::unique_ptr<RdKafka::Message> message = consumer.get_message();

    if (message) {
      auto payload = std::string(static_cast<const char *>(message->payload()),
                                 message->len());
      producer.produce(payload, *message->key(), config.output_topic());
    }
  }

  // TODO(kkrol): Add logging

  producer.flush();

  return 0;
}