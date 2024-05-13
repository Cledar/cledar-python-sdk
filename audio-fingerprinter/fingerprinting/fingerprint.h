#ifndef FINGERPRINT_H
#define FINGERPRINT_H

#include <fstream>
#include <memory>
#include <nlohmann/json.hpp>
#include <optional>
#include <span>
#include <vector>

#include "peaks.h"
#include "spectrogram.h"

typedef struct {
  std::string hash_1;
  std::string hash_2;
  int ts;
  int offset;
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
 protected:
  int sframe_size_;
  int step_size_;

  int ts_;

  std::optional<peak_t> last_peak_ = std::nullopt;

  std::vector<spec_t> spectrogram_;
  peaks_answer_t peaks_ans_{};

  SpectrogramCalculator spectrogram_calculator_;
  PeakExtractor peak_extractor_;

  std::optional<FingerprintLogger> logger_ = std::nullopt;

 public:
  Fingerprinter(int sframe_size, int step_size, int ts);

  void get_fingerprints(std::span<const sample_t> samples,
                        std::vector<fingerprint_t>& fingerprints);
};

peak_t peaks_to_fingerprints(std::span<peak_t> peaks,
                             std::vector<fingerprint_t>& fingerprints,
                             int start_time, peak_t last_peak);

std::string dump_fingerprint(const fingerprint_t& hash,
                             nlohmann::json basic_info);
#endif  // FINGERPRINT_H
