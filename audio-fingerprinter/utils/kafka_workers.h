
#ifndef KAFKA_WORKERS_H
#define KAFKA_WORKERS_H

#include <librdkafka/rdkafkacpp.h>
// #include <spdlog/spdlog.h>

#include <iostream>
#include <memory>

#define PRODUCER_FLUSH_TIMEOUT_MS 5000  // TODO(kkrol): Why 5s?
#define AWAIT_MSG_TIMEOUT_MS 1000       // TODO(kkrol): Why 1s?

/**
 * @brief A class that represents a Kafka producer.
 *
 * The KafkaProducer class is responsible for producing messages to a Kafka
 * topic. It uses the RdKafka library to interact with Kafka.
 */
class KafkaProducer {
 private:
  std::unique_ptr<RdKafka::Producer> producer_;

 public:
  /**
   * @brief Constructs a KafkaProducer object with the specified brokers.
   *
   * @param brokers The string of broker addresses in the format "host:port".
   *                Multiple brokers can be provided by separating them with
   *                commas (e.g., "host1:port1,host2:port2").
   **/
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

  /**
   * @brief Produces a message to a Kafka topic.
   *
   * This function sends a message to the specified Kafka topic using the
   * configured producer.
   *
   * @param message The message to be sent.
   * @param key The key associated with the message.
   * @param topic The Kafka topic to which the message will be sent.
   */
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
  /**
   * Flushes the Kafka producer.
   * This function ensures that all pending messages are sent to the Kafka
   * broker before returning. It blocks until all messages are successfully sent
   * or the specified timeout is reached.
   */
  void flush() { producer_->flush(PRODUCER_FLUSH_TIMEOUT_MS); }
};

/**
 * @brief A class that represents a Kafka consumer.
 *
 * The KafkaConsumer class is responsible for consuming messages from a Kafka
 * topic. It uses the RdKafka library to interact with Kafka.
 */
class KafkaConsumer {
 private:
  std::unique_ptr<RdKafka::KafkaConsumer> consumer_;

 public:
  /**
   * @brief Constructs a KafkaConsumer object.
   *
   * This constructor initializes a KafkaConsumer object with the specified
   * brokers, group ID, and topic. It creates a Kafka consumer and subscribes to
   * the specified topic.
   *
   * @param brokers The list of Kafka brokers to connect to.
   * @param group_id The ID of the consumer group.
   * @param topic The topic to subscribe to.
   * @throws std::ios_base::failure if the consumer fails to be created.
   */
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
  /**
   * @brief Destructor for the KafkaConsumer class.
   *
   * This destructor closes the Kafka consumer.
   */
  ~KafkaConsumer() { consumer_->close(); }

  /**
   * @brief Retrieves a message from the Kafka consumer.
   *
   * This function retrieves a message from the Kafka consumer and returns it as
   * a unique pointer to a `RdKafka::Message` object. If there are no messages
   * available or an error occurs, a null pointer is returned.
   *
   * @return A unique pointer to a `RdKafka::Message` object if a message is
   * successfully retrieved, or nullptr otherwise.
   */
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