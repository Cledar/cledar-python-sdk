#include "spectrogram.h"

#include <cmath>
#include <iostream>

SpectrogramLogger::SpectrogramLogger(std::string file)
    : spectrogram_sink_(fopen(file.c_str(), "wb+"), &fclose) {}

void SpectrogramLogger::log_spectrogram(std::span<spec_t> spectrogram) {
  if (!spectrogram_sink_) {
    std::cerr << "Spectrogram file is not open"
              << std::endl;  // TODO(kkrol): Change to logging lib
  } else {
    if (fwrite(spectrogram.data(), sizeof(spec_t), spectrogram.size(),
               spectrogram_sink_.get()) < spectrogram.size()) {
      std::cerr << "Error writing to spectrogram file"
                << std::endl;  // TODO(kkrol): Change to logging lib
    }
  }
}

SpectrogramCalculator::SpectrogramCalculator(int frame_size)
    : sframe_size_(frame_size),
      fftw_out_size_(sframe_size_ / 2 + 1),
      sframe_(sframe_size_),
      window_han_(sframe_size_),
      fftw_out_(fftwf_alloc_complex(fftw_out_size_)),
      fftw_plan_(fftwf_plan_dft_r2c_1d(sframe_size_, sframe_.data(), fftw_out_,
                                       FFTW_MEASURE)) {
  // originally FFTW_ESTIMATE, MEASURE should initialize longer, but run faster

  if (!fftw_out_ || !fftw_plan_) {
    throw std::bad_alloc();
  }

  for (int i = 0; i < sframe_size_; i++) {
    window_han_[i] =
        MAGIC_0_5 - MAGIC_0_5 * cos(MAGIC_2_ * M_PI * i / (sframe_size_ - 1));
  }

#ifdef _DEBUG
  logger_.emplace("new_out/specbytes");
#endif  // _DEBUG
}

SpectrogramCalculator::~SpectrogramCalculator() {
  fftwf_free(fftw_out_);
  fftwf_destroy_plan(fftw_plan_);
}

void SpectrogramCalculator::get_spectrogram(std::span<const sample_t> samples,
                                            std::span<spec_t> spectrogram) {
  for (int i = 0; i < sframe_size_; i++) {
    sframe_[i] = static_cast<spec_t>(samples[i]) * window_han_[i];
  }
  fftwf_execute(fftw_plan_);
  auto fftw_span = std::span(fftw_out_, fftw_out_size_);
  for (size_t i = 0; i < fftw_span.size(); i++) {
    spec_t real = fftw_span[i][0];
    spec_t im = fftw_span[i][1];
    spec_t norm = real * real + im * im;
    spectrogram[i] = norm;
  }
  spec_t norm_sum = 0.0;
  for (size_t i = 0; i < fftw_out_size_; i++) {
    norm_sum += spectrogram[i];
  }
  spec_t threshold = norm_sum * MAGIC_THRESHOLD;
  for (size_t i = 0; i < fftw_out_size_; i++) {
    if (spectrogram[i] < threshold) {
      spectrogram[i] = 0;
    }
    if (i > fftw_out_size_ / 4) {  // TODO(kkrol): Why? Could we do less
                                   // processing if we didn't do this?
      spectrogram[i] = 0;
    }
  }
  if (logger_) {
    logger_.value().log_spectrogram(spectrogram);
  }
}
