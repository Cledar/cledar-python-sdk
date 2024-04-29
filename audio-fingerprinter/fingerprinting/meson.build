# project('libfingerprint', 'cpp', default_options: ['cpp_std=c++23'])

lib_deps = [
    dependency('fftw3'),
    dependency('fftw3f'),
    dependency('nlohmann_json'),
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
