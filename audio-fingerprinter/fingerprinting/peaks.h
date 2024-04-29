#ifndef PEAKS_H
#define PEAKS_H

#include <memory>
#include <optional>
#include <span>

#include "common.h"
#include "legacy_peaks.h"  // # TODO(kkrol): Rewrite legacy dependency

typedef struct {
  int pframes_read;
  size_t peaks_count;
  std::array<peak_t, PFRAME_CELLS * MAGIC_100>
      peaksbuff;  // TODO(kkrol): Reason for such size?
} peaks_answer_t;

class PeakLogger {
 protected:
  std::unique_ptr<FILE, decltype(&fclose)> peaks_sink_;

 public:
  explicit PeakLogger(std::string file);
  void log_peaks(std::span<peak_t> peaks_ans);
};

class PeakExtractor {
 protected:
  peaks_gen_t pgen_{};
  std::optional<PeakLogger> logger_ = std::nullopt;

 public:
  PeakExtractor();
  void get_peaks(std::span<spec_t> spectrogram, peaks_answer_t& peaks_ans);
};

#endif  // PEAKS_H