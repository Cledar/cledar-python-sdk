from datetime import datetime
from pydantic import Field, field_validator
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


class PeaksExtractorSettings(BaseSettings):
    """
    Class for storing settings for the peaks extractor.
    """

    time_delta_s: int = Field(alias="PEAKS_EXTRACTOR_TIME_DELTA_S")
    processing_time_s: int = Field(alias="PEAKS_EXTRACTOR_PROCESSING_TIME_S")


class FingerprintsChunkerSettings(BaseSettings):
    """
    Class for storing settings for the fingerprint chunker.
    """

    delay_s: int = Field(alias="FINGERPRINTS_CHUNKER_DELAY_S")
    time_delta_s: int = Field(alias="FINGERPRINT_CHUNKER_TIME_DELTA_S")
    processing_time_s: int = Field(alias="FINGERPRINT_CHUNKER_PROCESSING_TIME_S")


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

    stream_fingerprints_start_time: datetime = Field(
        alias="STREAM_FINGERPRINTS_START_TIME"
    )
    stream_fingerprints_run_id: str = Field(alias="STREAM_FINGERPRINTS_RUN_ID")

    @field_validator("stream_fingerprints_start_time")
    @classmethod
    def parse_datetime(cls, value):
        if isinstance(value, str):
            return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S")
        return value


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
    n_shuffle_partitions: int = Field(alias="N_SHUFFLE_PARTITIONS")
    driver_mem: str = Field(alias="DRIVER_MEM")
    executor_mem: str = Field(alias="EXECUTOR_MEM")
    postgres_jar_pckg: str = Field(alias="POSTGRES_JAR_PCKG")


class Settings(BaseSettings):
    """
    Class for storing settings for the matcher worker.
    """

    peaks_extractor_settings: PeaksExtractorSettings = Field(
        default_factory=PeaksExtractorSettings
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
