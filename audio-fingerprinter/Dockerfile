FROM debian:trixie AS builder
RUN apt-get update -y && apt-get install -y \
    libboost-all-dev \
    python3-minimal \
    libfftw3-dev \
    python3-pip \
    pkg-config \
    ffmpeg \
    gcovr
RUN pip3 install --break-system-packages meson ninja

FROM builder AS build
WORKDIR /app/audio-fingerprinter/
COPY . .
RUN meson setup build --buildtype=release
RUN meson compile -C build

FROM debian:trixie-slim
RUN apt-get update -y && apt-get install -y \
    libboost-program-options1.83.0 \
    libfftw3-bin \
    adduser \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*
RUN adduser --disabled-password --gecos "" nonroot
COPY --chown=nonroot:nonroot --from=build /app/audio-fingerprinter/build/fingerprinting/libfingerprint.so /lib/x86_64-linux-gnu/
COPY --chown=nonroot:nonroot --from=build /app/audio-fingerprinter/build/audio_fingerprinter /bin/audio_fingerprinter
COPY --chown=nonroot:nonroot --from=build /app/audio-fingerprinter/build/subprojects/*/*.so* /lib/x86_64-linux-gnu/
USER nonroot
ENTRYPOINT [ "/bin/audio_fingerprinter" ]

