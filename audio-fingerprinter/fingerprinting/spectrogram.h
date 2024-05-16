#ifndef SPECTROGRAM_H
#define SPECTROGRAM_H

#include <memory>
#include <optional>
#include <span>
#include <vector>

#include "common.h"
#include "fftw3.h"

/**
 * @brief A class for logging spectrogram data to a file.
 */
class SpectrogramLogger {
 protected:
  std::unique_ptr<FILE, decltype(&fclose)> spectrogram_sink_;

 public:
  /**
   * @brief Constructs a `SpectrogramLogger` object with the specified file.
   * @param file The file path to log the spectrogram data.
   */
  explicit SpectrogramLogger(std::string file);

  /**
   * @brief Logs the given spectrogram data to the file.
   * @param spectrogram A span containing the spectrogram data.
   */
  void log_spectrogram(std::span<spec_t> spectrogram);
};

/**
 * @brief Calculates the spectrogram of an audio signal.
 *
 * The `SpectrogramCalculator` class is responsible for calculating the
 * spectrogram of an audio signal. It takes a frame size as input and generates
 * a spectrogram by applying a window function to the audio samples and
 * performing a Fast Fourier Transform (FFT) on each frame.
 */
class SpectrogramCalculator {
 protected:
  int sframe_size_;
  size_t fftw_out_size_;
  std::vector<spec_t> sframe_;
  std::vector<spec_t> window_han_;
  fftwf_complex* fftw_out_;
  fftwf_plan fftw_plan_;
  std::optional<SpectrogramLogger> logger_ = std::nullopt;

 public:
  /**
   * @brief Calculates the spectrogram of an audio signal.
   *
   * The `SpectrogramCalculator` class is responsible for calculating the
   * spectrogram of an audio signal.
   */
  explicit SpectrogramCalculator(int frame_size);

  ~SpectrogramCalculator();
  SpectrogramCalculator(const SpectrogramCalculator& other) = default;
  SpectrogramCalculator& operator=(const SpectrogramCalculator& other) =
      default;
  SpectrogramCalculator(SpectrogramCalculator&& other) = default;
  SpectrogramCalculator& operator=(SpectrogramCalculator&& other) = default;

  /**
   * @brief Calculates the spectrogram of the given audio samples.
   *
   * This function takes a span of audio samples and calculates the spectrogram,
   * which is a visual representation of the frequencies present in the audio
   * signal over time. The resulting spectrogram is stored in the provided span.
   *
   * @param samples The input audio samples.
   * @param spectrogram The span to store the calculated spectrogram.
   */
  void get_spectrogram(std::span<const sample_t> samples,
                       std::span<spec_t> spectrogram);
};

#endif  // SPECTROGRAM_H