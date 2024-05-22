FROM cimg/python:3.12.3 AS builder

USER root

WORKDIR /app

RUN apt-get update
RUN apt-get install -y -qq libfftw3-dev 
RUN apt-get install -y -qq ffmpeg 
RUN apt-get install -y -qq libboost-all-dev
RUN apt-get install nlohmann-json3-dev
RUN apt-get install -y -qq libfmt-dev

RUN pip3 install --break-system-packages meson ninja

FROM builder AS build

COPY . .
RUN meson setup build
RUN meson compile -C build
