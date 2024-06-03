# project('fingerprinting', 'cpp', default_options: ['cpp_std=c++23'])
# TODO(kkrol): Make it actual subproject

lib_deps = [
    dependency('fftw3'),
    dependency('fftw3f'),
    dependency('nlohmann_json'),
    dependency('spdlog'),
]

fingerprint_lib = library(
    'fingerprint',
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

test_fingerprint = executable(
    'test_fingerprint',
    'tests/test_fingerprint.cpp',
    dependencies: [
        fingerprint_dep,
        dependency('gtest_main'),
        dependency('gmock'),
        lib_deps,
    ],
)

test('test_fingerprint', test_fingerprint)
