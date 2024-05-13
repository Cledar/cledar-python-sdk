
#ifndef KAFKA_WORKERS_H
#define KAFKA_WORKERS_H

#include <librdkafka/rdkafkacpp.h>
// #include <spdlog/spdlog.h>

#include <iostream>
#include <memory>

#define PRODUCER_FLUSH_TIMEOUT_MS 5000  // TODO(kkrol): Why 5s?
#define AWAIT_MSG_TIMEOUT_MS 1000       // TODO(kkrol): Why 1s?

class KafkaProducer {
 private:
  std::unique_ptr<RdKafka::Producer> producer_;

 public:
  explicit KafkaProducer(const std::string &brokers) {
    std::string errstr;
    auto conf = std::unique_ptr<RdKafka::Conf>(
        RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
    conf->set("bootstrap.servers", brokers, errstr);

    producer_ = std::unique_ptr<RdKafka::Producer>(
        RdKafka::Producer::create(conf.get(), errstr));
    if (!producer_) {
      throw std::ios_base::failure("Failed to create producer: " + errstr);
    }
  }

  void produce(std::string message, const std::string key,
               const std::string &topic) {
    // TODO(kkrol): Add producer_->poll(0) in regular intervals, even if no
    // produce is run for some time
    RdKafka::ErrorCode resp = producer_->produce(
        topic, RdKafka::Topic::PARTITION_UA, RdKafka::Producer::RK_MSG_COPY,
        message.data(), message.size(), key.data(), key.size(),
        0,  // TODO(kkrol): 0 timestamp?
        nullptr, nullptr);

    if (resp != RdKafka::ERR_NO_ERROR) {
      // spdlog::warn("Failed to produce message: {}", RdKafka::err2str(resp));
    }
  }
  void flush() { producer_->flush(PRODUCER_FLUSH_TIMEOUT_MS); }
};

class KafkaConsumer {
 private:
  std::unique_ptr<RdKafka::KafkaConsumer> consumer_;

 public:
  explicit KafkaConsumer(const std::string &brokers,
                         const std::string &group_id,
                         const std::string &topic) {
    std::string errstr;
    auto conf = std::unique_ptr<RdKafka::Conf>(
        RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
    conf->set("bootstrap.servers", brokers, errstr);
    conf->set("group.id", group_id, errstr);  // TODO(kkrol): Error handling

    consumer_ = std::unique_ptr<RdKafka::KafkaConsumer>(
        RdKafka::KafkaConsumer::create(conf.get(), errstr));
    if (!consumer_) {
      throw std::ios_base::failure("Failed to create consumer: " + errstr);
    }

    consumer_->subscribe({topic});  // TODO(kkrol): Error handling
  }
  ~KafkaConsumer() { consumer_->close(); }

  std::unique_ptr<RdKafka::Message> get_message() {
    auto message = std::unique_ptr<RdKafka::Message>(
        consumer_->consume(AWAIT_MSG_TIMEOUT_MS));
    if (message->err() == RdKafka::ERR_NO_ERROR) {
      return message;
    }
    return nullptr;
  }
};

#endif  // KAFKA_WORKERS_H