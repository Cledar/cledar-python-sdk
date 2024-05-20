# project('libfingerprint', 'cpp', default_options: ['cpp_std=c++23'])

spdlog = subproject('spdlog')

lib_deps = [
    dependency('fftw3'),
    dependency('fftw3f'),
    dependency('nlohmann_json'),
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

gtest_proj = subproject('gtest')
gtest_main_dep = gtest_proj.get_variable('gtest_main_dep')
gmock_dep = gtest_proj.get_variable('gmock_dep')

test_fingerprint = executable(
    'test_fingerprint',
    'tests/test_fingerprint.cpp',
    dependencies: [
        fingerprint_dep,
        gtest_main_dep,
        gmock_dep,
    ],
)

test('test_fingerprint', test_fingerprint)