#ifndef FINGERPRINT_H
#define FINGERPRINT_H

#include <fstream>
#include <memory>
#include <optional>
#include <span>
#include <vector>

#include "peaks.h"
#include "spectrogram.h"

typedef struct {
  std::string hash_1;
  std::string hash_2;
  int org_ts;
  int ts;
  int offset;
  std::string channel;
  std::string station_id;
} fingerprint_t;

class FingerprintLogger {
 protected:
  std::ofstream fingerprint_sink_;

 public:
  explicit FingerprintLogger(std::string fingerprint_file);
  void log_fingerprint(fingerprint_t& hash);
  void log_fingerprints(std::vector<fingerprint_t>& fingerprints);
};

class Fingerprinter {
 private:
  bool initialized_ = false;

 protected:
  int sframe_size_;
  int step_size_;

  std::string station_id_;
  std::string channel_;
  int ts_;

  int last_freq_ = 0;
  int last_t_ = 0;

  std::vector<spec_t> spectrogram_;
  peaks_answer_t peaks_ans_{};

  SpectrogramCalculator spectrogram_calculator_;
  PeakExtractor peak_extractor_;

  std::optional<FingerprintLogger> logger_ = std::nullopt;

  void peaks_to_fingerprints(
      std::span<peak_t> peaks,
      std::vector<fingerprint_t>&
          fingerprints);  // TODO(kkrol): Separate peaks_to_fingerprints and
                          // samples_to_fingerprints to 2 classes

 public:
  Fingerprinter(int sframe_size, int step_size, std::string channel,
                std::string station_id, int ts);

  void get_fingerprints(std::span<const sample_t> samples,
                        std::vector<fingerprint_t>& fingerprints);
};

std::string dump_fingerprint(const fingerprint_t& hash);
#endif  // FINGERPRINT_H
