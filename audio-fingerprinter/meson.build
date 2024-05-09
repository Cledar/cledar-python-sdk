project(
  'audio_fingerprinter',
  'cpp',
  version: '0.1',
  default_options: ['warning_level=3', 'cpp_std=c++23', 'optimization=3'],
)

if get_option('debug')
  add_project_arguments('-D_DEBUG', language: 'cpp')
endif
# afdep = [dependency('fftw3'), dependency('fftw3f'), dependency('rdkafka'), dependency('rdkafka++'), dependency('nlohmann_json'), dependency('boost', modules:['program_options'])]

subdir('fingerprinting')
spdlog = subproject('spdlog')

af_dep = [
  dependency('rdkafka'),
  dependency('rdkafka++'),
  dependency('boost', modules: ['program_options']),
  spdlog.get_variable('spdlog_dep'),
]

executable(
  'audio_fingerprinter',
  ['main.cpp'],
  dependencies: [fingerprint_dep, af_dep],
)

executable(
  'ambinet-audio-fingerprint-decoder',
  ['prosumer.cpp'],
  dependencies: [fingerprint_dep, af_dep],
)
