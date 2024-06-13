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
spdlog = subproject('spdlog')
nlohmann_json = subproject('nlohmann_json')
rdkafka = subproject('rdkafka')

af_dep = [
  dependency('boost', modules: ['program_options']),
  rdkafka.get_variable('rdkafka_dep'),
  rdkafka.get_variable('rdkafkapp_dep'),
  nlohmann_json.get_variable('nlohmann_json_dep'),
  spdlog.get_variable('spdlog_dep'),
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

gtest_proj = subproject('gtest')
gtest_main_dep = gtest_proj.get_variable('gtest_main_dep')
gmock_dep = gtest_proj.get_variable('gmock_dep')

test_config = executable(
  'test_config',
  'tests/test_config.cpp',
  dependencies: [
    fingerprint_dep,
    gtest_main_dep,
    gmock_dep,
    af_dep,
    spdlog.get_variable('spdlog_dep'),
  ],
)

test('test_config', test_config)

run_target(
  'e2e_test',
  command: ['./tests/e2e_test.sh', '@BUILD_ROOT@'],
)