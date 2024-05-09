#include "fingerprint.h"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <cmath>
#include <iostream>
#include <nlohmann/json.hpp>

FingerprintLogger::FingerprintLogger(std::string fingerprint_file)
    : fingerprint_sink_(fingerprint_file) {
  fingerprint_sink_ << "hash_1,hash_2,org_ts,ts,offset,channel,station_id\n";
}

void FingerprintLogger::log_fingerprint(fingerprint_t &hash) {
  if (fingerprint_sink_.is_open()) {
    fingerprint_sink_ << hash.hash_1 << "," << hash.hash_2 << "," << 10 << ","
                      << hash.ts << "," << hash.offset << ",10,kim"
                      << std::endl;
    if (fingerprint_sink_.fail()) {
      SPDLOG_ERROR("Error writing to fingerprint file");
    }
  } else {
    SPDLOG_WARN("Fingerprint file is not open");
  }
}

void FingerprintLogger::log_fingerprints(
    std::vector<fingerprint_t> &fingerprints) {
  for (auto &hash : fingerprints) {
    log_fingerprint(hash);
  }
}

Fingerprinter::Fingerprinter(int sframe_size, int step_size, int ts)
    : sframe_size_(sframe_size),
      step_size_(step_size),
      ts_(ts),
      spectrogram_(sframe_size_ / 2 + 1),
      spectrogram_calculator_(sframe_size_),
      peak_extractor_() {
#ifdef _DEBUG
  logger_.emplace("new_out/fingerprint.csv");
#endif  // _DEBUG
}

peak_t peaks_to_fingerprints(std::span<peak_t> peaks,
                             std::vector<fingerprint_t> &fingerprints,
                             int start_time, peak_t last_peak) {
  for (auto &peak : peaks) {
    if (peak.freq <= 0) continue;
    int t = peak.time - last_peak.time;
    auto hash_1 = std::to_string(last_peak.freq) + ":" +
                  std::to_string(peak.freq) + ":" + std::to_string(t);
    auto hash_2 =
        std::to_string(static_cast<int>(log(last_peak.freq) * MAGIC_10)) + ":" +
        std::to_string(static_cast<int>(log(peak.freq) * MAGIC_10)) + ":" +
        std::to_string(t);
    fingerprints.emplace_back(fingerprint_t{
        hash_1, hash_2,
        start_time + static_cast<int>(PROBABLY_SFRAME_TIME_OFFSET * peak.time),
        peak.time});
    last_peak = peak;
  }
  return last_peak;
}

void Fingerprinter::get_fingerprints(std::span<const sample_t> samples,
                                     std::vector<fingerprint_t> &fingerprints) {
  for (size_t samples_it = 0; samples_it + sframe_size_ <= samples.size();
       samples_it += step_size_) {
    spectrogram_calculator_.get_spectrogram(
        samples.subspan(samples_it, sframe_size_), spectrogram_);
    peak_extractor_.get_peaks(spectrogram_, peaks_ans_);
  }
  auto peaks_read =
      std::span(peaks_ans_.peaksbuff).first(peaks_ans_.peaks_count);
  std::sort(peaks_read.begin(), peaks_read.end(),
            [](const peak_t &a, const peak_t &b) {
              if (a.time == b.time) {
                return a.freq < b.freq;
              }
              return a.time < b.time;
            });

  if (!peaks_read.empty()) {
    if (!last_peak_) {
      last_peak_ = {std::max(peaks_read[0].freq, 0),
                    std::max(peaks_read[0].time, 0)};
      peaks_read = peaks_read.subspan(1);
    }
    last_peak_ = peaks_to_fingerprints(peaks_read, fingerprints, ts_,
                                       last_peak_.value());
  }
  peaks_ans_.peaks_count = 0;
  // last_t_ -= pframe_to_time(peaks_ans_.pframes_read);
  // peaks_ans_.pframes_read = 0;  // TODO(kkrol): reset so that it can run
  // infinitly or make it LL, now overflow after ~1 year; resetting destroys
  // offset column in fingerprint
  if (logger_) {
    logger_.value().log_fingerprints(fingerprints);
  }
}

std::string dump_fingerprint(const fingerprint_t &hash, int org_ts,
                             const std::string &channel,
                             const std::string &station_id) {
  nlohmann::json json_hash;
  json_hash["hash_1"] = hash.hash_1;
  json_hash["hash_2"] = hash.hash_2;
  json_hash["org_ts"] = org_ts;
  json_hash["ts"] = hash.ts;
  json_hash["offset"] = hash.offset;
  json_hash["channel"] = channel;
  json_hash["station_id"] = station_id;

  return json_hash.dump();
}
