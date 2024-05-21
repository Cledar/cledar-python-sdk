# Audio Fingerprinter

## Installation

1. Install the required libraries:

    - libfftw3-dev
    - ffmpeg
    - libboost-all-dev

    You can manually install these libraries by: 
    ```
    apt-get install libfftw3-dev ffmpeg libboost-all-dev
    ```

2. Setup using `meson setup`, for example:
    ```
    meson setup builddir --buildtype debugoptimized --reconfigure
    ```
3. Go into build directory (`builddir` in the example above) and compile:
    ```
    meson compile
    ```
