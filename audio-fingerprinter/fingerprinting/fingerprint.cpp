#include "fingerprint.h"

#include <algorithm>
#include <iostream>
#include <nlohmann/json.hpp>

FingerprintLogger::FingerprintLogger(std::string fingerprint_file)
    : fingerprint_sink_(fingerprint_file) {
  fingerprint_sink_ << "hash_1,hash_2,org_ts,ts,offset,channel,station_id\n";
}

void FingerprintLogger::log_fingerprint(fingerprint_t &hash) {
  if (fingerprint_sink_.is_open()) {
    fingerprint_sink_ << hash.hash_1 << "," << hash.hash_2 << "," << hash.org_ts
                      << "," << hash.ts << "," << hash.offset << ","
                      << hash.channel << "," << hash.station_id << std::endl;
    if (fingerprint_sink_.fail()) {
      std::cerr << "Error writing to fingerprint file"
                << std::endl;  // TODO(kkrol): Change to logging lib
    }
  } else {
    std::cerr << "Fingerprint file is not open"
              << std::endl;  // TODO(kkrol): Change to logging lib
  }
}

void FingerprintLogger::log_fingerprints(
    std::vector<fingerprint_t> &fingerprints) {
  for (auto &hash : fingerprints) {
    log_fingerprint(hash);
  }
}

Fingerprinter::Fingerprinter(int sframe_size, int step_size,
                             std::string channel, std::string station_id,
                             int ts)
    : sframe_size_(sframe_size),
      step_size_(step_size),
      station_id_(station_id),
      channel_(channel),
      ts_(ts),
      spectrogram_(sframe_size_ / 2 + 1),
      spectrogram_calculator_(sframe_size_),
      peak_extractor_() {
#ifdef _DEBUG
  logger_.emplace("new_out/fingerprint.csv");
#endif  // _DEBUG
}

void Fingerprinter::peaks_to_fingerprints(
    std::span<peak_t> peaks, std::vector<fingerprint_t> &fingerprints) {
  std::sort(peaks.begin(), peaks.end(), [](const peak_t &a, const peak_t &b) {
    if (a.time == b.time) {
      return a.freq < b.freq;
    }
    return a.time < b.time;
  });
  size_t first_peak = 0;
  if (!initialized_ && !peaks.empty()) {
    last_freq_ = std::max(peaks[0].freq, 0);
    last_t_ = std::max(peaks[0].time, 0);
    first_peak = 1;
    initialized_ = true;
  }
  for (auto &peak : peaks.subspan(first_peak)) {
    if (peak.freq <= 0) continue;
    int t = peak.time - last_t_;
    auto hash_1 = std::to_string(last_freq_) + ":" + std::to_string(peak.freq) +
                  ":" + std::to_string(t);
    auto hash_2 = std::to_string(static_cast<int>(log(last_freq_) * MAGIC_10)) +
                  ":" +
                  std::to_string(static_cast<int>(log(peak.freq) * MAGIC_10)) +
                  ":" + std::to_string(t);
    fingerprints.emplace_back(fingerprint_t{
        hash_1, hash_2, ts_,
        ts_ + static_cast<int>(PROBABLY_SFRAME_TIME_OFFSET * peak.time),
        peak.time, channel_, station_id_});
    last_freq_ = peak.freq;
    last_t_ = peak.time;
  }
}

void Fingerprinter::get_fingerprints(std::span<const sample_t> samples,
                                     std::vector<fingerprint_t> &fingerprints) {
  for (size_t samples_it = 0; samples_it + sframe_size_ <= samples.size();
       samples_it += step_size_) {
    spectrogram_calculator_.get_spectrogram(
        samples.subspan(samples_it, sframe_size_), spectrogram_);
    peak_extractor_.get_peaks(spectrogram_, peaks_ans_);
  }

  peaks_to_fingerprints(
      std::span(peaks_ans_.peaksbuff).first(peaks_ans_.peaks_count),
      fingerprints);
  peaks_ans_.peaks_count = 0;
  // last_t_ -= pframe_to_time(peaks_ans_.pframes_read);
  // peaks_ans_.pframes_read = 0;  // TODO(kkrol): reset so that it can run
  // infinitly or make it LL, now overflow after ~1 year; resetting destroys
  // offset column in fingerprint
  if (logger_) {
    logger_.value().log_fingerprints(fingerprints);
  }
}

std::string dump_fingerprint(const fingerprint_t &hash) {
  nlohmann::json json_hash;
  json_hash["hash_1"] = hash.hash_1;
  json_hash["hash_2"] = hash.hash_2;
  json_hash["org_ts"] = hash.org_ts;
  json_hash["ts"] = hash.ts;
  json_hash["offset"] = hash.offset;
  json_hash["channel"] = hash.channel;
  json_hash["station_id"] = hash.station_id;

  return json_hash.dump();
}
