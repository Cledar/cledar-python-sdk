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
rdkafka = subproject('rdkafka')

af_dep = [
  dependency('boost', modules: ['program_options']),
  rdkafka.get_variable('rdkafka_dep'),
  rdkafka.get_variable('rdkafkapp_dep'),
  spdlog.get_variable('spdlog_dep'),
  dependency('fftw3'),
  dependency('fftw3f'),
  nlohmann_json.get_variable('nlohmann_json_dep'),
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
