FROM cimg/python:3.12.3 AS builder

USER root

WORKDIR /app

RUN apt-get update && apt-get install -y -qq libfftw3-dev ffmpeg libboost-all-dev nlohmann-json3-dev libfmt-dev gcovr

RUN pip3 install --break-system-packages meson ninja

FROM builder AS build

COPY . ./audio-fingerprinter/

WORKDIR /app/audio-fingerprinter

RUN meson setup build --buildtype=release
RUN meson compile -C build

FROM build AS e2e_test

CMD ["meson", "compile", "e2e_test", "-C", "build"]

FROM build AS run_local
ENTRYPOINT [ "./build/audio_fingerprinter", "--kafka-address", "redpanda-0:9092", "resources/radio/audio.wav", "--offset", "0", "--channel", "10", "--station-id", "kim", "--start-ts", "0"]

