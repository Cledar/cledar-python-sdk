#include "peaks.h"

#include <spdlog/spdlog.h>

#include <iostream>

namespace {
int pframe_to_time(int pframe) { return pframe * PFRAME_WIDTH; }
}  // namespace

PeakLogger::PeakLogger(std::string file)
    : peaks_sink_(fopen(file.c_str(), "wb+")) {}

void PeakLogger::log_peaks(std::span<peak_t> peaks_ans) {
  if (!peaks_sink_) {
    SPDLOG_ERROR("Peak file is not open");
  } else {
    if (fwrite(peaks_ans.data(), sizeof(peak_t), peaks_ans.size(),
               peaks_sink_.get()) < peaks_ans.size()) {
      SPDLOG_WARN("Error writing to peak file");
    }
  }
}

PeakExtractor::PeakExtractor() {
  peaks_init(&pgen_);
#ifdef _DEBUG
  logger_.emplace("new_out/peakbytes");
#endif  // _DEBUG
}

void PeakExtractor::get_peaks(std::span<spec_t> spectrogram,
                              peaks_answer_t &peaks_ans) {
  size_t readp = 0;
  if (peaks_consume_sframe(&pgen_, spectrogram.data(),
                           peaks_ans.peaksbuff.data() + peaks_ans.peaks_count,
                           &readp)) {  // TODO(kkrol): Use span instead
    for (auto &peak :
         std::span(peaks_ans.peaksbuff).subspan(peaks_ans.peaks_count, readp)) {
      peak.time += pframe_to_time(peaks_ans.pframes_read);
    }
    if (logger_) {
      logger_.value().log_peaks(
          std::span(peaks_ans.peaksbuff).subspan(peaks_ans.peaks_count, readp));
    }
    peaks_ans.peaks_count += readp;
    peaks_ans.pframes_read++;
  }
}