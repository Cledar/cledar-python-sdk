import os
from datetime import datetime
from dotenv import load_dotenv

DOTENV_PATH = "/matcher/.env"
load_dotenv(DOTENV_PATH)

SECOND_TO_MS = 1000


# FINGERPRINTS STREAM SETTINGS
stream_fingerprints_start_time = datetime.strptime(
    os.getenv("STREAM_FINGERPRINTS_START_TIME"), "%Y-%m-%dT%H:%M:%S"
)
stream_fingerprints_run_id = os.getenv("STREAM_FINGERPRINTS_RUN_ID")


# application properties
# peaks extractor
peaks_extractor_time_delta_s = int(os.getenv("PEAKS_EXTRACTOR_TIME_DELTA_S"))
peaks_extractor_processing_time_s = int(os.getenv("PEAKS_EXTRACTOR_PROCESSING_TIME_S"))

# fingerprint chunker
fingerprint_chunker_dalay_s = int(os.getenv("FINGERPRINTS_CHUNKER_DALAY_S"))
fingerprint_chunker_time_delta_s = int(os.getenv("FINGERPRINT_CHUNKER_TIME_DELTA_S"))
fingerprint_chunker_processing_time_s = int(
    os.getenv("FINGERPRINT_CHUNKER_PROCESSING_TIME_S")
)

# fingerprint matcher
before_window_surplus = int(os.getenv("BEFORE_WINDOW_SURPLUS"))
after_window_surplus = int(os.getenv("AFTER_WINDOW_SURPLUS"))
matcher_processing_time_s = int(os.getenv("MATCHER_PROCESSING_TIME_S"))
ref_restart_freq = int(os.getenv("REF_RESTART_FREQ"))
reference_peaks_delay_s = int(os.getenv("REFERENCE_PEAKS_DELAY_S"))

# logger
logg_file_path = os.getenv("LOGG_FILE_PATH")


# db properties
db_ref_peaks_properties = {
    "hostname": os.getenv("DB_REF_PEAKS_HOSTNAME"),
    "db": os.getenv("DB_REF_PEAKS_DB"),
    "port": os.getenv("DB_REF_PEAKS_PORT"),
    "user": os.getenv("DB_REF_PEAKS_USER"),
    "password": os.getenv("DB_REF_PEAKS_PASSWORD"),
    "driver": os.getenv("DB_REF_PEAKS_DRIVER"),
    "batchSize": os.getenv("DB_REF_PEAKS_BATCHSIZE"),
}

fingerprints_parsed_table_name = os.getenv("FINGERPRINTS_PARSED_TABLE_NAME")
output_table_name = os.getenv("OUTPUT_TABLE_NAME")
reference_peaks_table_name = os.getenv("REFERENCE_PEAKS_TABLE_NAME")

db_fingerprints_properties = {
    "hostname": os.getenv("DB_FINGERPRINTS_HOSTNAME"),
    "db": os.getenv("DB_FINGERPRINTS_DB"),
    "port": os.getenv("DB_FINGERPRINTS_PORT"),
    "user": os.getenv("DB_FINGERPRINTS_USER"),
    "password": os.getenv("DB_FINGERPRINTS_PASSWORD"),
    "driver": os.getenv("DB_FINGERPRINTS_DRIVER"),
    "batchSize": os.getenv("DB_FINGERPRINTS_BATCHSIZE"),
}

raw_fingerprints_table_name = os.getenv("RAW_FINGERPRINTS_TABLE_NAME")

db_parsed_fingerprints_properties = {
    "hostname": os.getenv("DB_PARSED_FINGERPRINTS_HOSTNAME"),
    "db": os.getenv("DB_PARSED_FINGERPRINTS_DB"),
    "port": os.getenv("DB_PARSED_FINGERPRINTS_PORT"),
    "user": os.getenv("DB_PARSED_FINGERPRINTS_USER"),
    "password": os.getenv("DB_PARSED_FINGERPRINTS_PASSWORD"),
    "driver": os.getenv("DB_PARSED_FINGERPRINTS_DRIVER"),
    "batchSize": os.getenv("DB_PARSED_FINGERPRINTS_BATCHSIZE"),
}

db_fingerprints_matched_properties = {
    "hostname": os.getenv("DB_FINGERPRINTS_MATCHED_HOSTNAME"),
    "db": os.getenv("DB_FINGERPRINTS_MATCHED_DB"),
    "port": os.getenv("DB_FINGERPRINTS_MATCHED_PORT"),
    "user": os.getenv("DB_FINGERPRINTS_MATCHED_USER"),
    "password": os.getenv("DB_FINGERPRINTS_MATCHED_PASSWORD"),
    "driver": os.getenv("DB_FINGERPRINTS_MATCHED_DRIVER"),
    "batchSize": os.getenv("DB_FINGERPRINTS_MATCHED_BATCHSIZE"),
}
fingerprints_matched_table_name = os.getenv("FINGERPRINTS_MATCHED_TABLE_NAME")


# SPARK
n_threads = int(os.getenv("N_THREADS"))
n_shuffle_partitions = int(os.getenv("N_SHUFFLE_PARTITIONS"))
driver_mem = os.getenv("DRIVER_MEM")
executor_mem = os.getenv("EXECUTOR_MEM")
postgres_jar_pckg = os.getenv("POSTGRES_JAR_PCKG")
