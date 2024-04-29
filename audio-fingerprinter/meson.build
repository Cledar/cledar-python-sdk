project(
  'audio_fingerprinter',
  'cpp',
  version: '0.1',
  default_options: ['warning_level=3', 'cpp_std=c++23'],
)

if get_option('debug')
  add_project_arguments('-D_DEBUG', language: 'cpp')
endif
# afdep = [dependency('fftw3'), dependency('fftw3f'), dependency('rdkafka'), dependency('rdkafka++'), dependency('nlohmann_json'), dependency('boost', modules:['program_options'])]

subdir('fingerprinting')

af_dep = [
  dependency('rdkafka'),
  dependency('rdkafka++'),
  dependency('boost', modules: ['program_options']),
]

executable(
  'audio_fingerprinter',
  ['main.cpp'],
  dependencies: [fingerprint_dep, af_dep],
)
