#ifndef SPECTROGRAM_H
#define SPECTROGRAM_H

#include <memory>
#include <optional>
#include <span>
#include <vector>

#include "common.h"
#include "fftw3.h"

class SpectrogramLogger {
 protected:
  std::unique_ptr<FILE, decltype(&fclose)> spectrogram_sink_;

 public:
  explicit SpectrogramLogger(std::string file);

  void log_spectrogram(std::span<spec_t> spectrogram);
};

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
  explicit SpectrogramCalculator(int frame_size);

  ~SpectrogramCalculator();
  SpectrogramCalculator(const SpectrogramCalculator& other) = default;
  SpectrogramCalculator& operator=(const SpectrogramCalculator& other) =
      default;
  SpectrogramCalculator(SpectrogramCalculator&& other) = default;
  SpectrogramCalculator& operator=(SpectrogramCalculator&& other) = default;

  void get_spectrogram(std::span<const sample_t> samples,
                       std::span<spec_t> spectrogram);
};

#endif  // SPECTROGRAM_H