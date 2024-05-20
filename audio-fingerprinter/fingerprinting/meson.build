# project('fingerprinting', 'cpp', default_options: ['cpp_std=c++23'])
# TODO(kkrol): Make it actual subproject

spdlog = subproject('spdlog')
nlohmann_json = subproject('nlohmann_json')

lib_deps = [
    dependency('fftw3'),
    dependency('fftw3f'),
    nlohmann_json.get_variable('nlohmann_json_dep'),
    spdlog.get_variable('spdlog_dep'),
]

fingerprint_lib = library(
    'libfingerprint',
    sources: [
        'fingerprint.h',
        'fingerprint.cpp',
        'spectrogram.h',
        'spectrogram.cpp',
        'peaks.h',
        'peaks.cpp',
        'common.h',
        'legacy_peaks.h',
    ],
    dependencies: lib_deps,
)

fingerprint_dep = declare_dependency(
    include_directories: include_directories('.'),
    link_with: fingerprint_lib,
)
