#ifndef PEAKS_H
#define PEAKS_H

#include <memory>
#include <optional>
#include <span>

#include "common.h"
#include "legacy_peaks.h"  // # TODO(kkrol): Rewrite legacy dependency

/**
 * @brief Struct representing the answer containing peak information.
 *
 * This struct is used to store information about peaks in an audio signal.
 * It contains the number of frames read, the count of peaks since
 * fingerprinting started, and an array to store the peak values.
 */
typedef struct {
  int pframes_read;
  size_t peaks_count;
  std::array<peak_t, PFRAME_CELLS * MAGIC_100>
      peaksbuff;  // TODO(kkrol): Reason for such size?
} peaks_answer_t;

/**
 * @brief A class for logging peaks to a file.
 *
 * The `PeakLogger` class provides functionality to log peaks to a file.
 * It takes a file path as input and logs the peaks to that file.
 */
class PeakLogger {
 protected:
  std::unique_ptr<FILE, decltype(&fclose)> peaks_sink_;

 public:
  /**
   * @brief Constructs a `PeakLogger` object with the specified file path.
   * @param file The file path to log the peaks to.
   */
  explicit PeakLogger(std::string file);

  /**
   * @brief Logs the given peaks to the file.
   * @param peaks_ans A span of `peak_t` objects representing the peaks to be
   * logged.
   */
  void log_peaks(std::span<peak_t> peaks_ans);
};

/**
 * @brief The `PeakExtractor` class is responsible for extracting peaks from a
 * spectrogram.
 *
 * It uses a `peaks_gen_t` object to generate peaks and a `PeakLogger` object
 * for optional logging.
 */
class PeakExtractor {
 protected:
  /**
   * @brief The `peaks_gen_t` class is used to buffer frames in order to
   * calculate peaks.
   *
   * It provides functionality to store and process audio frames for peak
   * calculation.
   */
  peaks_gen_t pgen_{};
  std::optional<PeakLogger> logger_ = std::nullopt;

 public:
  /**
   * @brief Constructs a new `PeakExtractor` object.
   */
  PeakExtractor();

  /**
   * @brief Extracts peaks from the given spectrogram.
   *
   * @param spectrogram The input spectrogram.
   * @param peaks_ans The output peaks.
   */
  void get_peaks(std::span<spec_t> spectrogram, peaks_answer_t& peaks_ans);
};

#endif  // PEAKS_H