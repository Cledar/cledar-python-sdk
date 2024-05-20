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

/**
 * @brief Struct representing a fingerprint.
 *
 * This struct contains the hash values, timestamp, and offset of a fingerprint.
 */
typedef struct {
  /**
   * @brief String consisting of the frequency of the previous peak, the
   * frequency of the current peak, and the number of frames between them. All
   * values are separated by a colon (':').
   */
  std::string hash_1;
  /**
   * @brief The same value as `hash_1`, but with log applied to the frequency
   * values.
   */
  std::string hash_2;
  /**
   * @brief Timestamp of the fingerprint in seconds rounded down.
   */
  int ts;
  /**
   * @brief Number of frames since the start of fingerprinting.
   *
   * Each frame is approximately `PROBABLY_SFRAME_TIME_OFFSET` seconds long.
   */
  int offset;
} fingerprint_t;

/**
 * @brief A class for logging audio fingerprints to a file.
 *
 * The `FingerprintLogger` class provides functionality to log audio
 * fingerprints to a specified file. It allows logging individual fingerprints
 * as well as a collection of fingerprints.
 */
class FingerprintLogger {
 protected:
  std::ofstream fingerprint_sink_;

 public:
  /**
   * @brief Constructs a `FingerprintLogger` object with the specified
   * fingerprint file.
   * @param fingerprint_file The path to the file where the fingerprints will be
   * logged.
   */
  explicit FingerprintLogger(std::string fingerprint_file);

  /**
   * @brief Logs a single fingerprint to the fingerprint file.
   * @param hash The fingerprint to be logged.
   */
  void log_fingerprint(fingerprint_t &hash);

  /**
   * @brief Logs a collection of fingerprints to the fingerprint file.
   * @param fingerprints The collection of fingerprints to be logged.
   */
  void log_fingerprints(std::vector<fingerprint_t> &fingerprints);
};

/**
 * @brief Class responsible for generating audio fingerprints from samples.
 *
 * The `Fingerprinter` class is used to generate audio fingerprints from a given
 * set of audio samples. It calculates the spectrogram of the audio samples,
 * extracts peaks from the spectrogram, and converts the peaks into
 * fingerprints. The fingerprints can be used for audio identification and
 * matching.
 */
class Fingerprinter {
 protected:
  int sframe_size_;
  int step_size_;

  int ts_;

  /**
   * @brief An optional value that may contain last peak if one was processed.
   *
   * This optional value is used to store the last peak found during audio
   * fingerprinting. It is initialized as empty (std::nullopt) and will be
   * assigned a value on the first use.
   *
   * @tparam peak_t The type of the peak object.
   */
  std::optional<peak_t> last_peak_ = std::nullopt;

  std::vector<spec_t> spectrogram_;
  peaks_answer_t peaks_ans_{};

  SpectrogramCalculator spectrogram_calculator_;
  PeakExtractor peak_extractor_;

  std::optional<FingerprintLogger> logger_ = std::nullopt;

 public:
  /**
   * @brief Constructs a `Fingerprinter` object.
   *
   * @param sframe_size The size of the analysis frames in samples.
   * @param step_size The step size between consecutive analysis frames in
   * samples.
   * @param ts The timestamp of the audio data.
   */
  Fingerprinter(int sframe_size, int step_size, int ts);

  /**
   * @brief Generates fingerprints from the given audio samples.
   *
   * This function takes a span of audio samples and generates fingerprints
   * based on the samples. The fingerprints are stored in the provided vector.
   *
   * @param samples The audio samples to generate fingerprints from.
   * @param fingerprints The vector to store the generated fingerprints.
   */
  void get_fingerprints(std::span<const sample_t> samples,
                        std::vector<fingerprint_t> &fingerprints);
};

/**
 * Converts a list of peaks into fingerprints and stores them in the provided
 * vector.
 *
 * @param peaks The list of peaks to convert.
 * @param fingerprints The vector to store the converted fingerprints.
 * @param start_time The start time for the fingerprints.
 * @param last_peak The last peak processed before the first peak in the list.
 */
peak_t peaks_to_fingerprints(std::span<peak_t> peaks,
                             std::vector<fingerprint_t> &fingerprints,
                             int start_time, peak_t last_peak);

/**
 * @brief Generates a JSON string representation of a fingerprint.
 *
 * @param hash The fingerprint hash to be dumped.
 * @param basic_info The basic shared information about the fingerprint, i.e.
 *      - org_ts -      The timestamp of the beginning of fingerprinting.
 *      - channel -     The channel associated with the fingerprint.
 *      - station_id -  The station ID associated with the fingerprint.
 *
 * @return A JSON string representation of the fingerprint.
 */
std::string dump_fingerprint(const fingerprint_t &hash,
                             nlohmann::json basic_info);
#endif  // FINGERPRINT_H
