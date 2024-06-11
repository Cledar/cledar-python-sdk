#!/bin/bash
cd "$1"
mkdir -p new_out
echo "Running audio_fingerprinter with offset 0 on radio.wav"
./audio_fingerprinter --kafka-address localhost:9092 ../resources/radio/audio.wav --offset 0 --channel 10 --station-id kim --start-ts 10 > /dev/null 2>&1
echo "Comparing fingerprints on radio.wav"
RETURN_VAL=0
cmp -s new_out/fingerprint.csv ../resources/radio/fingerprints1.csv
if [ $? -eq 0 ]; then
    echo -e "[\e[32mOK\e[0m] Fingerprints match."
else
    RETURN_VAL=1
    echo -e "[\e[31mFAIL\e[0m] Fingerprints differ."
fi
echo "Comparing peaks on radio.wav"
cmp -s new_out/peakbytes ../resources/radio/peak1.bytes 
if [ $? -eq 0 ]; then
    echo -e "[\e[32mOK\e[0m] Peaks match."
else
    RETURN_VAL=1
    echo -e "[\e[31mFAIL\e[0m] Generated peaks differ."
fi
echo "Comparing specgram on radio.wav"
cmp -s new_out/specbytes ../resources/radio/spec1.bytes 
if [ $? -eq 0 ]; then
    echo -e "[\e[32mOK\e[0m] Spectrograms match."
else
    RETURN_VAL=1
    echo -e "[\e[31mFAIL\e[0m] Generated spectrograms differ."
fi

echo "Re-running audio_fingerprinter with offset 2048 on radio.wav"
./audio_fingerprinter --kafka-address localhost:9092 ../resources/radio/audio.wav --offset 2048 --channel 10 --station-id kim --start-ts 10 > /dev/null 2>&1
echo "Comparing peaks on radio.wav"
cmp -s new_out/peakbytes ../resources/radio/peak2.bytes
if [ $? -eq 0 ]; then
    echo -e "[\e[32mOK\e[0m] Peaks match."
else
    RETURN_VAL=1
    echo -e "[\e[31mFAIL\e[0m] Generated peaks differ."
fi
echo "Comparing specgram on radio.wav"
cmp -s new_out/specbytes ../resources/radio/spec2.bytes
if [ $? -eq 0 ]; then
    echo -e "[\e[32mOK\e[0m] Spectrograms match."
else
    RETURN_VAL=1
    echo -e "[\e[31mFAIL\e[0m] Generated spectrograms differ."
fi

rm -rf new_out
exit $RETURN_VAL