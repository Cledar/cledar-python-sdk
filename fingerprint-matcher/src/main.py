# pylint: skip-file
import os
import glob
import shutil
import time
from src.data_processing import (
    PeaksExtractor,
    FingerprintsMatcher,
    ContinuousBatchProcessor,
)
from src.settings import settings
from src.utils import get_syslog_logger, get_fingerprints_df, get_matcher_stream
import pyspark
from pyspark.sql import SparkSession


def init_spark():
    """
    Initialize the Spark session.
    """
    spark_master = os.environ.get("SPARK_MASTER", "local[*]")
    # Default to local mode if SPARK_MASTER is not set
    sql = (
        SparkSession.builder.master(spark_master)
        .appName("load-app")
        .config("spark.driver.memory", settings.spark_settings.driver_mem)
        .config("spark.executor.cores", settings.spark_settings.n_threads)
        .config("spark.executor.memory", settings.spark_settings.executor_mem)
        .config(
            "spark.ui.showConsoleProgress",
            settings.spark_settings.ui_show_console_progress,
        )
        .config("spark.task.maxFailures", settings.spark_settings.task_max_failures)
        .config("spark.jars.packages", settings.spark_settings.postgres_jar_pckg)
        .config(
            "spark.sql.shuffle.partitions", settings.spark_settings.n_shuffle_partitions
        )
        .config(
            "spark.dynamicAllocation.shuffleTracking.enabled",
            settings.spark_settings.shuffle_tracking_enabled,
        )
        .config(
            "spark.shuffle.service.enabled",
            settings.spark_settings.shuffle_service_enabled,
        )
        .config(
            "spark.shuffle.service.removeShuffle",
            settings.spark_settings.shuffle_service_remove_shuffle,
        )
        .config(
            "spark.cleaner.referenceTracking.cleanCheckpoints",
            settings.spark_settings.cleaner_reference_tracking_cln_ckp,
        )
        .config(
            "spark.worker.cleanup.enabled",
            settings.spark_settings.spark_worker_cleanup_enabled,
        )
        .config(
            "spark.worker.cleanup.interval",
            settings.spark_settings.worker_cleanup_interval,
        )
        .config(
            "spark.shuffle.file.buffer", settings.spark_settings.shuffle_file_buffer
        )
        .config(
            "spark.io.compression.lz4.blockSize",
            settings.spark_settings.compression_lz4_blocksize,
        )
        .config(
            "spark.sql.streaming.forceDeleteTempCheckpointLocation",
            settings.spark_settings.force_delete_temp_ckp_loc,
        )
        .getOrCreate()
    )
    return sql, sql.sparkContext


spark, sc = init_spark()
logger = get_syslog_logger(settings.logging_settings.log_file_path)


class TransformAndMatchPipeline:
    """
    A class for the streaming data processing pipeline.
    """

    def __init__(self):
        """
        Initialize the pipeline components and start the streams.
        """
        peaks_extractor = PeaksExtractor(
            spark,
            logger,
            settings.spark_settings.n_threads,
            settings.db_fingerprints_properties,
            settings.db_parsed_fingerprints_properties,
            settings.table_names.raw_fingerprints_table_name,
            settings.table_names.fingerprints_parsed_table_name,
        )
        extract_function = peaks_extractor.protobuf_read_parse_save
        # FIXME(karolpustelnik)
        self.protobuf_processor = ContinuousBatchProcessor(
            spark,
            logger,
            settings.streaming_settings.stream_fingerprints_start_time,
            settings.streaming_settings.stream_fingerprints_run_id,
            db_parsed_fingerprints_properties=settings.db_parsed_fingerprints_properties,
            output_table_name=settings.table_names.output_table_name,
            batch_processor=extract_function,
            time_delta=settings.peaks_extractor_settings.time_delta_s,
            processing_time=settings.peaks_extractor_settings.processing_time_s,
            throttle_fun=lambda x: x - 10,
        )
        fingerprints_chunker = ContinuousBatchProcessor(
            spark,
            logger,
            settings.streaming_settings.stream_fingerprints_start_time,
            settings.streaming_settings.stream_fingerprints_run_id + "_matcher",
            db_parsed_fingerprints_properties=settings.db_parsed_fingerprints_properties,
            output_table_name=settings.table_names.output_table_name,
            batch_processor=self.chunker_batch_processor,
            time_delta=settings.fingerprint_chunker_settings.time_delta_s,
            processing_time=settings.fingerprint_chunker_settings.processing_time_s,
            throttle_fun=lambda __curr_epoch: peaks_extractor.min_ts
            - settings.fingerprint_chunker_settings.delay_s,
        )
        self.fingerprint_matcher = FingerprintsMatcher(
            spark,
            logger,
            settings.spark_settings.n_threads,
            settings.db_ref_peaks_properties,
            settings.db_fingerprints_matched_properties,
            settings.table_names.reference_peaks_table_name,
            settings.table_names.fingerprints_matched_table_name,
            settings.fingerprints_matcher_settings.reference_peaks_delay_s,
            settings.fingerprints_matcher_settings.ref_restart_freq,
            settings.fingerprints_matcher_settings.before_window_surplus,
            settings.fingerprints_matcher_settings.after_window_surplus,
        )
        self.extractor_monitor = self.protobuf_processor.start()
        self.chunker_monitor = fingerprints_chunker.start()
        self.matcher_monitor = get_matcher_stream(
            spark,
            self.match_batch,
            settings.fingerprints_matcher_settings.processing_time_s,
        ).start()

    def match_batch(self, batchdf, __batchid):
        """
        Process a batch DataFrame of fingerprints for matching.

        Parameters:
        - batchdf (DataFrame): DataFrame containing fingerprint data for a batch.
        - __batchid (str): Identifier for the batch.

        Returns:
        - int: Number of matches found in the batch.
        """
        return self.fingerprint_matcher.match_miernik_ref(batchdf)

    def chunker_batch_processor(self, ts_end, ts_start, _batch_id):
        """
        Process fingerprints data into chunks.

        Parameters:
        - ts_end (datetime): End timestamp for processing.
        - ts_start (datetime): Start timestamp for processing.
        - batch_id (str): Identifier for the batch.

        Returns:
        - Tuple[int, datetime]: Number of processed records and timestamp
        of the processed batch.
        """
        beg_ts = time.time()
        fingerprints_df = get_fingerprints_df(
            spark,
            settings.db_parsed_fingerprints_properties,
            settings.table_names.fingerprints_parsed_table_name,
            ts_end.timestamp(),
            ts_start.timestamp(),
        )
        cnt = fingerprints_df.count()
        logger.debug(
            "Chunker process: Loaded miernik Fingerprints "
            "for %s from %s (epoch-%s) "
            "with count: %s in %s seconds",
            ts_end,
            ts_start,
            int(ts_start.timestamp()),
            cnt,
            time.time() - beg_ts,
        )
        fingerprints_df = fingerprints_df.withColumn(
            "offset", fingerprints_df["offset"].cast("long")
        )
        fingerprints_df.write.mode("append").parquet("./fingerprints-chunked/chunks/")
        parquet_files = glob.glob("./fingerprints-chunked/chunks/" + "*parquet")
        for file in parquet_files:
            shutil.move(file, "./fingerprints-chunked/")
        logger.debug(
            "Chunker process: Chunked miernik fingerprints data in %s seconds",
            time.time() - beg_ts,
        )
        return cnt, ts_end

    def monitor_streaming(self, run_date):
        """
        Monitor the status and progress of the streaming data processing
        pipeline iteration.

        This function continuously monitors the status of each component in the pipeline
        and checks for any errors or termination conditions.
        It logs relevant information and waits for the iteration to complete.

        Parameters:
        - run_date (datetime): The date of the current iteration.

        Returns:
        - str: A message indicating the completion of the iteration.
        """
        logger.info("Next iteration: %s", run_date)
        while True:
            if self.extractor_monitor:
                logger.info("SLQ: %s", self.extractor_monitor.status["message"])
            if self.chunker_monitor:
                logger.info("SCQ: %s", self.chunker_monitor.status["message"])
            if self.matcher_monitor:
                logger.info("SMQ: %s", self.matcher_monitor.status["message"])
            no_fingerprints = False
            if self.extractor_monitor:
                statistics_for_last_three_iters = self.protobuf_processor.daily_stats[
                    -3:
                ]
                logger.info(
                    [
                        [str(j) for j in i]  # formating the list of lists
                        for i in statistics_for_last_three_iters
                    ]
                )
                last_three_matches_zero = all(
                    x[2] == 0 for x in statistics_for_last_three_iters
                )
                minimum_iterations = len(self.protobuf_processor.daily_stats) > 5
                no_fingerprints = last_three_matches_zero and minimum_iterations
            if no_fingerprints:
                logger.info("No progress: no_fingerprints")
                break
            if self.extractor_monitor and self.extractor_monitor.status[
                "message"
            ].startswith("Terminated with exception"):
                logger.error("Error in fingerprints loader (slq stream) ")
                break
            if self.matcher_monitor and self.matcher_monitor.status[
                "message"
            ].startswith("Terminated with exception"):
                logger.error("Error in matcher stream (smq)")
                break
            if self.chunker_monitor and self.chunker_monitor.status[
                "message"
            ].startswith("Terminated with exception"):
                logger.error("Error in chunker stream (scq)")
                break
            time.sleep(60)
        return "Iteration completed."


if __name__ == "__main__":
    # Initialize the pipeline and start the streaming data processing.
    pipeline = TransformAndMatchPipeline()
    try:
        logger.info(
            pipeline.monitor_streaming(
                settings.streaming_settings.stream_fingerprints_start_time
            )
        )
    except Exception as exc:
        logger.error("An error occurred: %s", exc)
        import traceback

        traceback.print_exc()
        raise exc
