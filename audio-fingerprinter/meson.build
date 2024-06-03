project(
  'audio_fingerprinter',
  'cpp',
  version: '0.1',
  default_options: ['warning_level=3', 'cpp_std=c++23', 'optimization=3'],
)

if get_option('debug')
  add_project_arguments('-D_DEBUG', language: 'cpp')
endif

subdir('fingerprinting')

af_dep = [
  dependency('boost', modules: ['program_options']),
  dependency('rdkafka++'),
  dependency('nlohmann_json'),
  dependency('spdlog'),
]

executable(
  'audio_fingerprinter',
  ['audio_fingerprinter.cpp'],
  dependencies: [fingerprint_dep, af_dep],
)

executable(
  'ambinet-audio-fingerprint-decoder',
  ['prosumer.cpp'],
  dependencies: [fingerprint_dep, af_dep],
)

test_config = executable(
  'test_config',
  'tests/test_config.cpp',
  dependencies: [
    fingerprint_dep,
    af_dep,
    dependency('gtest_main'),
    dependency('gmock'),
  ],
)

test('test_config', test_config)

run_target(
  'e2e_test',
  command: ['./tests/e2e_test.sh', '@BUILD_ROOT@'],
)