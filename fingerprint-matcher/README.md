# FINGERPRINT MATCHER

## Installation


0. (optional) setup local database for testing purposes.
0. (optional) regenerate app_data_pb2.py file by running:
    ```
    protoc --python_out=. app_data.proto
    ```
    in the fingerprint-matcher.src directory.
1. Setup .env file with correct values for database connections. You can use the provided .env file as a template.
2. Setup STREAM_CHUNKER_START_TIME, STREAM_CHUNKER_RUN_ID, STREAM_PROTOBUF_START_TIME and STREAM_PROTOBUF_RUN_ID variables in .env file to the desired values. STREAM_CHUNKER_START_TIME is the date in format 2024-04-24T00:00:00 that specifies from which point onward should the processing begin. Same for STREAM_PROTOBUF_START_TIME. The STREAM_CHUNKER_RUN_ID/PROTOBUF is just and id of the current run, it can be anything, it is used by the matcher to be able determine from where to continue after pause/interruption.
3. Run docker compose:
    ```
    docker-compose up
    ```
    to start the fingerprint matcher service.


## Detailed description of miernik utrzymania databases and tables

# 3 databases:

1. On tts01 (192.168.45.199) - fingerprints_parsed, fingerprints_stage, output, reference_peaks
2. On tts03 (192.168.46.197) - fingerprints
. 35.241.191.183 - fingerpints_matched_v4 

# More explanation about STREAM_CHUNKER_START_TIME and STREAM_CHUNKER_RUN_ID.
Let's say we set STREAM_CHUNKER_START_TIME as 2024-04-24T00:00:00. This means we start processing from April 24, 2024, at 00:00. Let's say we set STREAM_CHUNKER_RUN_ID as test_run. If we interrupt the processing after 4 hours and want to restart the process, letting it continue from the point it finished last time it ran, we can simply use docker-compose up without changing STREAM_CHUNKER_START_TIME or STREAM_CHUNKER_RUN_ID. However, if we change STREAM_CHUNKER_RUN_ID while keeping STREAM_CHUNKER_START_TIME the same, the processing will start from the beginning, or more specifically, from the exact time specified in STREAM_CHUNKER_START_TIME.
