# pylint: disable=line-too-long
from datetime import datetime
from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings
from dotenv import load_dotenv

ENV_PATH = "/matcher/.env"


def create_database_settings(alias_prefix: str):
    class DatabaseSettings(BaseSettings):
        hostname: str
        db: str
        port: str
        user: str
        password: str
        driver: str
        batchsize: str

        class Config:
            env_prefix = alias_prefix

    return DatabaseSettings


DBParsedFingerprintsSettings = create_database_settings("DB_PARSED_FINGERPRINTS_")
DBRefPeaksSettings = create_database_settings("DB_REF_PEAKS_")
DBFingerprintsSettings = create_database_settings("DB_FINGERPRINTS_")
DBFingerprintsMatchedSettings = create_database_settings("DB_FINGERPRINTS_MATCHED_")


class ProtobufProcessorSettings(BaseSettings):
    """
    Class for storing settings for the Protobuf processor.
    """

    time_delta_s: int = Field(
        alias="PROTOBUF_PROCESSOR_TIME_DELTA_S",
        description="Time range for loading protobuf in seconds. E.g. if we set this to 300, the loader will load 300 seconds of protobuf to process",
    )
    processing_time_s: int = Field(
        alias="PROTOBUF_PROCESSOR_PROCESSING_TIME_S",
        description="Determines how often the loader stream should be triggered. If we set it too low, we may see spark warnings 'Current batch is falling behind' (we can safely ignore these warnings). If we set it too high, the stream may not be triggered often enough to process the data in real-time.",
    )
    delay_s: int = Field(
        alias="PROTOBUF_PROCESSOR_DELAY_S",
        description="Delay in seconds for protobuf processor to wait before processing protobuf. E.g. if we set this to 21600, the protobuf processor will wait 6 hours before processing so that any data that was buffered by mierniks can have time to be written to the fingerprints database.",
    )


class FingerprintsChunkerSettings(BaseSettings):
    """
    Class for storing settings for the fingerprint chunker.
    """

    delay_s: int = Field(
        alias="FINGERPRINTS_CHUNKER_DELAY_S",
        description="Delay in seconds for chunker to wait before chunking fingerprints. The delay is needed to ensure that the peaks extracted by PeaksExtractor are written to the database before the chunker starts processing them.",
    )
    time_delta_s: int = Field(
        alias="FINGERPRINT_CHUNKER_TIME_DELTA_S",
        description="Time range for chunking fingerprints in seconds. E.g. if we set this to 450, the chunker will chunk 450 seconds of fingerprints.",
    )
    processing_time_s: int = Field(
        alias="FINGERPRINT_CHUNKER_PROCESSING_TIME_S",
        description="Determines how often should the chunker stream be triggered. If we set it too low, we may see spark warnings Current batch is falling behind (we can safely ignore these warnings). If we set it too high, the stream may not be triggered often enough to process the data in real-time.",
    )


class FingerprintsMatcherSettings(BaseSettings):
    """
    Class for storing settings for the fingerprint matcher.
    """

    before_window_surplus: int = Field(alias="BEFORE_WINDOW_SURPLUS")
    after_window_surplus: int = Field(alias="AFTER_WINDOW_SURPLUS")
    processing_time_s: int = Field(alias="MATCHER_PROCESSING_TIME_S")
    ref_restart_freq: int = Field(alias="REF_RESTART_FREQ")
    reference_peaks_delay_s: int = Field(alias="REFERENCE_PEAKS_DELAY_S")


class LoggingSettings(BaseSettings):
    """
    Class for storing settings for logging.
    """

    log_file_path: str = Field(alias="LOG_FILE_PATH")


class StreamingSettings(BaseSettings):
    """
    Class for storing settings for the streaming.
    """

    stream_protobuf_start_time: datetime = Field(
        alias="STREAM_PROTOBUF_START_TIME",
        description="Determines where to start processing protobuf. ",
    )
    stream_protobuf_run_id: str = Field(
        alias="STREAM_PROTOBUF_RUN_ID",
        description="Identifier of the run, it is added to OUTPUT_TABLE_NAME to tell protobuf processor where to continue after restarting with the same run_id. If we change the run_id to the one never used before the matching will start from the day selected in STREAM_PROTOBUF_START_TIME. Otherwise, it will continue from the last time in the OUTPUT_TABLE_NAME.",
    )
    stream_fingerprints_start_time: datetime = Field(
        alias="STREAM_CHUNKER_START_TIME",
        description="Determines where to start processing fingerprints. ",
    )
    stream_fingerprints_run_id: str = Field(
        alias="STREAM_CHUNKER_RUN_ID",
        description="Identifier of the run, it is added to OUTPUT_TABLE_NAME to tell chunker where to continue after restarting with the same run_id. If we change the run_id to the one never used before the matching will start from the day selected in STREAM_CHUNKER_START_TIME. Otherwise, it will continue from the last time in the OUTPUT_TABLE_NAME.",
    )

    @field_validator("stream_fingerprints_start_time")
    @classmethod
    def parse_datetime(cls, value):
        if isinstance(value, str):
            return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S")
        return value

    @model_validator(mode="after")
    @classmethod
    def check_run_ids_different(cls, values):
        if values.stream_protobuf_run_id == values.stream_fingerprints_run_id:
            raise ValueError(
                "stream_protobuf_run_id and stream_fingerprints_run_id must be different"
            )
        return values


class TableNames(BaseSettings):
    """
    Class for storing settings for the table names.
    """

    output_table_name: str = Field(alias="OUTPUT_TABLE_NAME")
    reference_peaks_table_name: str = Field(alias="REFERENCE_PEAKS_TABLE_NAME")
    fingerprints_parsed_table_name: str = Field(alias="FINGERPRINTS_PARSED_TABLE_NAME")
    raw_fingerprints_table_name: str = Field(alias="RAW_FINGERPRINTS_TABLE_NAME")
    fingerprints_matched_table_name: str = Field(
        alias="FINGERPRINTS_MATCHED_TABLE_NAME"
    )


class SparkSettings(BaseSettings):
    """
    Class for storing settings for the spark.
    """

    objc_disable_initialize_fork_safety: str = Field(
        alias="OBJC_DISABLE_INITIALIZE_FORK_SAFETY"
    )
    disable_spring: str = Field(alias="DISABLE_SPRING")
    n_threads: int = Field(alias="N_THREADS")
    n_partitions: int = Field(alias="N_PARTITIONS")
    n_shuffle_partitions: int = Field(alias="N_SHUFFLE_PARTITIONS")
    driver_mem: str = Field(alias="DRIVER_MEM")
    executor_mem: str = Field(alias="EXECUTOR_MEM")
    task_max_failures: int = Field(alias="TASK_MAX_FAILURES")
    ui_show_console_progress: str = Field(alias="UI_SHOW_CONSOLE_PROGRESS")
    postgres_jar_pckg: str = Field(alias="POSTGRES_JAR_PCKG")
    shuffle_tracking_enabled: str = Field(alias="SHUFFLE_TRACKING_ENABLED")
    shuffle_service_enabled: str = Field(alias="SHUFFLE_SERVICE_ENABLED")
    shuffle_service_remove_shuffle: str = Field(alias="SHUFFLE_SERVICE_REMOVE_SHUFFLE")
    cleaner_reference_tracking_cln_ckp: str = Field(
        alias="CLEANER_REFERENCE_TRACKING_CLEAN_CKP"
    )
    spark_worker_cleanup_enabled: str = Field(alias="SPARK_WORKER_CLEANUP_ENABLED")
    worker_cleanup_interval: str = Field(alias="WORKER_CLEANUP_INTERVAL")
    shuffle_file_buffer: str = Field(alias="SHUFFLE_FILE_BUFFER")
    compression_lz4_blocksize: str = Field(alias="COMPRESSION_LZ4_BLOCKSIZE")
    force_delete_temp_ckp_loc: str = Field(alias="FORCE_DELETE_CPT_LOC")
    cleaner_ttl: int = Field(alias="CLEANER_TTL")


class Settings(BaseSettings):
    """
    Class for storing settings for the matcher worker.
    """

    protobuf_processor_settings: ProtobufProcessorSettings = Field(
        default_factory=ProtobufProcessorSettings
    )
    fingerprint_chunker_settings: FingerprintsChunkerSettings = Field(
        default_factory=FingerprintsChunkerSettings
    )
    fingerprints_matcher_settings: FingerprintsMatcherSettings = Field(
        default_factory=FingerprintsMatcherSettings
    )

    logging_settings: LoggingSettings = Field(default_factory=LoggingSettings)

    streaming_settings: StreamingSettings = Field(default_factory=StreamingSettings)

    db_ref_peaks_properties: DBRefPeaksSettings = Field(
        default_factory=DBRefPeaksSettings
    )
    db_fingerprints_properties: DBFingerprintsSettings = Field(
        default_factory=DBFingerprintsSettings
    )
    db_fingerprints_matched_properties: DBFingerprintsMatchedSettings = Field(
        default_factory=DBFingerprintsMatchedSettings
    )
    db_parsed_fingerprints_properties: DBParsedFingerprintsSettings = Field(
        default_factory=DBParsedFingerprintsSettings
    )

    table_names: TableNames = Field(default_factory=TableNames)

    spark_settings: SparkSettings = Field(default_factory=SparkSettings)


def init_settings(env_path):
    load_dotenv(env_path)
    settings_ = Settings()
    settings_.db_ref_peaks_properties = settings_.db_ref_peaks_properties.dict()
    settings_.db_fingerprints_properties = settings_.db_fingerprints_properties.dict()
    settings_.db_fingerprints_matched_properties = (
        settings_.db_fingerprints_matched_properties.dict()
    )
    settings_.db_parsed_fingerprints_properties = (
        settings_.db_parsed_fingerprints_properties.dict()
    )
    return settings_


settings = init_settings(ENV_PATH)
