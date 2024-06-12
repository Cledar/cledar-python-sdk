# pylint: skip-file
import time
from datetime import datetime, timedelta
import pyspark
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    LongType,
    TimestampType,
)
from pyspark import StorageLevel
import pyspark.sql.functions as F
from src.utils import (
    add_processed_up_to_row,
    get_protobuf,
    get_ref_fingerprints_df,
    spark_write_to_db,
    spark_read_from_db,
    parse_protobuf,
    date_to_ts,
)
from src.settings import SECOND_TO_MS


class PeaksExtractor:
    """
    A class for loading and unpacking peaks from protobuf messages.

    Attributes:
        spark (SparkSession): The Spark session object.
        logger (Logger): The logger object.
        n_threads (int): The number of threads to use for processing.
        db_ref_peaks_properties (dict): Additional properties for the database connection.
        db_fingerprints_properties (dict): Additional properties for the fingerprints database
        connection.
        db_parsed_fingerprints_properties (dict): Additional properties for the reference peaks
        database connection.
        raw_fingerprints_table_name (str): The name of the raw fingerprints table in the
        fingerprints database.
        fingerprints_parsed_table_name (str): The name of the parsed fingerprints table in the
        reference peaks database.
        min_ts (int): The minimum timestamp encountered during processing.
        max_ts (int): The maximum timestamp encountered during processing.
    """

    def __init__(
        self,
        spark,
        logger,
        n_threads,
        db_fingerprints_properties,
        db_parsed_fingerprints_properties,
        raw_fingerprints_table_name,
        fingerprints_parsed_table_name,
    ):
        """
        Initialize the PeaksExtractor object.

        Args:
            spark (SparkSession): The Spark session object.
            logger (Logger): The logger object.
            n_threads (int): The number of threads to use for processing.
            db_fingerprints_properties (dict): Additional properties for the
            fingerprints database connection.
            db_parsed_fingerprints_properties (dict): Additional properties for the
            reference peaks database connection.
            raw_fingerprints_table_name (str): The name of the raw fingerprints table in the
            fingerprints database.
            fingerprints_parsed_table_name (str): The name of the parsed fingerprints table in the
            reference peaks database.
        """
        self.spark = spark
        self.logger = logger
        self.n_threads = n_threads
        self.db_fingerprints_properties = db_fingerprints_properties
        self.db_parsed_fingerprints_properties = db_parsed_fingerprints_properties
        self.raw_fingerprints_table_name = raw_fingerprints_table_name
        self.fingerprints_parsed_table_name = fingerprints_parsed_table_name
        self.min_ts = 0  # TODO(karolpustelnik): change to None

    def extract_from_db(self, ts_end, ts_start, batch_id):
        """
        Reads protobuf messages from db and extracts peaks.

        Args:
            ts_end (datetime.datetime): The end timestamp (created_at) of the time range
            (inclusive).
            ts_start (datetime.datetime): The start timestamp (created_at) of the time range
            (exclusive).
            batch_id (int): The batch ID for the processed data.

        Returns:
            tuple: A tuple containing the count of processed fingerprints and the end timestamp
            of the time range.

        """
        output_schema = StructType(
            [
                StructField("lp", IntegerType(), True),
                StructField("hash_1", StringType(), True),
                StructField("hash_2", StringType(), True),
                StructField("t", IntegerType(), True),
                StructField("f", IntegerType(), True),
                StructField("id", StringType(), True),
                StructField("ts", LongType(), True),
                StructField("tts", TimestampType(), True),
                StructField("offset", LongType(), True),
                StructField("created_at", TimestampType(), True),
                StructField("imei", StringType(), True),
                StructField("batchid", IntegerType(), True),
            ]
        )
        beg_ts = time.time()
        self.logger.debug(
            f"**** FingerprintsLoader: Start reading fingerprints {ts_end} - {ts_start}"
        )
        # Fetch fingerprint data within the specified time range
        protobuf_df = get_protobuf(
            self.spark,
            self.db_fingerprints_properties,
            self.raw_fingerprints_table_name,
            ts_end,
            ts_start,
        ).withColumn(
            "ts", F.to_timestamp(F.from_unixtime(F.col("start_time") / SECOND_TO_MS))
        )
        # Filter out erroneous protobufs from the future
        protobuf_df = protobuf_df.where(
            F.col("start_time") <= ts_end.timestamp() * SECOND_TO_MS
        )
        protobuf_df = protobuf_df.repartition(self.n_threads)
        self.logger.debug(
            f"**** FingerprintsLoader: Finished reading fingerprints {ts_end} - {ts_start}, count: {protobuf_df.count()}"
        )
        # Apply the Pandas UDF to parse the fingerprints and create a new dataframe
        protobuf_df = protobuf_df.groupBy("id").applyInPandas(
            lambda df: parse_protobuf(df, batch_id), schema=output_schema
        )
        # Filter out delayed data
        protobuf_df = protobuf_df.where(F.col("tts") > ts_end - timedelta(days=1))
        protobuf_df = protobuf_df.persist(StorageLevel.MEMORY_ONLY)
        # Count the number of processed fingerprints
        cnt = protobuf_df.count()
        self.logger.debug(
            f"**** FingerprintsLoader: Extracted peaks from fingerprints {ts_end} - {ts_start}, count: {cnt} in {time.time() - beg_ts} seconds"
        )
        # Update min_ts
        self.min_ts = int(ts_start.timestamp())
        # Write the processed data to the database
        protobuf_df = protobuf_df.withColumn("batchid", F.lit(batch_id))
        spark_write_to_db(
            protobuf_df,
            self.db_parsed_fingerprints_properties,
            self.fingerprints_parsed_table_name,
        )

        self.logger.debug(
            f"**** FingerprintsLoader: Fully processed fingerprints in {time.time() - beg_ts} seconds"
        )
        return cnt, ts_end


class ContinuousBatchProcessor:
    """
    A class for processing batches of data in a continuous manner.

    Attributes:
        spark (SparkSession): The Spark session object.
        logger (Logger): The logger object.
        run_date (datetime.date): Start date for the run.
        run_id (str): The identifier for the current run.
        db_ref_peaks_properties (dict): Properties for the database connection.
        output_table_name (str): The name of the output table.
        batch_processor (function): The function used for processing batches.
        time_delta (int): Maximum time in seconds between max_ts and min_ts provided
        to batch_processor.
        throttle_fun (function): A function that provides an upper bound for max_ts.
        monitor_stats (list): A list to store monitoring statistics.
        streaming_query: The Spark streaming query object.
        processing_time (int): The processing time for the streaming query.
    """

    def __init__(
        self,
        spark,
        logger,
        run_date,
        run_id,
        db_ref_peaks_properties,
        output_table_name,
        batch_processor,
        time_delta=300,
        processing_time=30,
        throttle_fun=lambda x: x,
    ):
        """
        Initializes the ContinuousBatchProcessor with required parameters.

        Requires table `output` in the database pointed by parameters with Spark schema
        "ts timestamp, batch_ts timestamp, batchid integer, run_id string".

        Parameters:
            spark (SparkSession): The Spark session object.
            logger (Logger): The logger object.
            run_date (datetime.date): Start date for the run.
            run_id (str): The identifier for the current run, used as a filter in the output table.
            db_ref_peaks_properties (dict): Properties for the database connection.
            output_table_name (str): The name of the output table.
            batch_processor (function): The function used for processing batches with signature
            (max_ts: datetime, min_ts: datetime, batch_id: str) ->
            (rows_returned: int, real_max_ts: datetime).
            time_delta (int): Maximum time in seconds between max_ts and min_ts provided
            to batch_processor.
            processing_time (int): The processing time for the streaming query.
            throttle_fun (function): A function giving an upper bound for max_ts with signature
            (batch_epoch: int).
        """
        self.spark = spark
        self.logger = logger
        self.run_date = run_date
        self.run_id = run_id
        self.daily_stats = []
        self.batch_processor = batch_processor
        self.time_delta = time_delta
        self.processing_time = processing_time
        self.throttle_fun = throttle_fun
        self.db_ref_peaks_properties = db_ref_peaks_properties
        self.output_table_name = output_table_name
        self.streaming_query = None

    def _process_batch(self, batchdf, batchid):
        """
        Internal method to process a batch of data.

        This method processes a batch of data by applying the batch_processor function
        and updating the output table.

        Args:
            batchdf (DataFrame): The DataFrame containing the batch of data to process.
            batchid (str): The identifier for the batch.

        Returns:
            None
        """
        rate_row = batchdf.orderBy("timestamp", ascending=False).first()
        if rate_row is None:
            self.logger.info("&&&& Batch start: Pusty batch " + str(self.run_id))
            # TODO(karolpustelnik): rewrite to: Starting stream with run id: {self.run_id}.
            return
        system_ts = rate_row["timestamp"]
        self.logger.info(f"&&&& Batch start: {self.run_id} - {system_ts}")
        # TODO(karolpustelnik): [{batch_ts}]: Stream with run id: {self.run_id}.
        if len(self.daily_stats) > 0:
            processed_up_to, _, _ = self.daily_stats[-1]
            if (
                processed_up_to is not None
                and processed_up_to.date() > self.run_date.date()
            ):
                self.logger.info(
                    f"&&&& Batch start: {self.run_id} finishing because {self.run_date} has been processed, and a new day started: {processed_up_to}"
                )
                # TODO(karolpustelnik): rewrite these to be more informative
                self.daily_stats.append(
                    (
                        processed_up_to,
                        processed_up_to,
                        0,
                    )
                )
                self.streaming_query.stop()
                return
        if self.run_id is None:
            where_cond = "run_id is null"
        else:
            where_cond = f"run_id='{self.run_id}'"
        curr_row = (
            spark_read_from_db(
                self.spark,
                self.db_ref_peaks_properties,
                sql_pattern=f"(select * from {self.output_table_name} where {where_cond})",
            )
            .orderBy("ts", ascending=False)
            .first()
        )
        if curr_row is None:
            self.logger.info(
                f"&&&& There are no records in table 'output_test' for {self.run_id}. We start from the beginning i.e. {self.run_date}"
            )
            add_processed_up_to_row(
                self.spark,
                self.db_ref_peaks_properties,
                self.output_table_name,
                date_to_ts(self.run_date),
                system_ts,
                batchid,
                self.run_id,
            )
            return
        ts_start = curr_row["ts"]
        upper_bound_ts_end = datetime.fromtimestamp(
            self.throttle_fun(system_ts.timestamp())
        )
        ts_end = ts_start + timedelta(seconds=self.time_delta)
        if ts_end > upper_bound_ts_end:
            self.logger.info(
                f"&&&& There is no progress {self.run_id} expected time: {ts_end} current upper_bound: {upper_bound_ts_end}"
            )
            # TODO(karolpustelnik): rewrite these to be more informative
            return
        self.logger.info(
            f"&&&& Processing {self.run_id} time range: {ts_start} - {ts_end} current upper_bound: {upper_bound_ts_end}"
        )
        # TODO(karolpustelnik): rewrite these to be more informative
        rows_processed, _ = self.batch_processor(ts_end, ts_start, batchid)
        self.logger.info(f"&&&& Processed {self.run_id} - {ts_end}")
        self.daily_stats.append(
            (
                ts_end,
                ts_start,
                rows_processed,
            )
        )
        add_processed_up_to_row(
            self.spark,
            self.db_ref_peaks_properties,
            self.output_table_name,
            ts_end,
            system_ts,
            batchid,
            self.run_id,
        )
        self.logger.info(f"&&&& End of the batch {self.run_id} - {ts_end}")

    def start(self):
        """
        Starts the batch processing.

        This method starts the batch processing by creating a streaming query
        that processes data in batches.

        Returns:
            StreamingQuery: The Spark streaming query object.
        """
        once_per_second = (
            self.spark.readStream.format("rate").option("rowsPerSecond", 1).load()
        )
        # ^^^ this is a way to start triggering _process_batch every self.processing_time seconds.
        self.streaming_query = (
            once_per_second.writeStream.trigger(
                processingTime=f"{self.processing_time} seconds"
            )
            .outputMode("append")
            .foreachBatch(self._process_batch)
            .start()
        )
        return self.streaming_query

    def stop(self):
        """
        Stops the batch processing.

        This method stops the streaming query if it is running.

        Returns:
            None
        """
        if self.streaming_query:
            self.streaming_query.stop()


class FingerprintsMatcher:
    """
    Class for matching fingerprints with reference fingerprints.

    Attributes:
        spark (SparkSession): The Spark session object.
        logger (Logger): The logger object.
        n_threads (int): The number of threads to use for processing.
        db_ref_peaks_conn_url_jdbc (str): JDBC URL for the reference peaks database.
        db_ref_peaks_properties (dict): Properties for the database connection.
        reference_peaks_table_name (str): The name of the reference peaks table.
        match_window_before (int): Time window before matching.
        match_window_after (int): Time window after matching.
        pk_restart_cnt (int): Counter for peak restarts.
        min_ts (int): The minimum timestamp for matching.
        max_ts (int): The maximum timestamp for matching.
        pk_df (DataFrame): DataFrame to cache reference peaks.
    """

    def __init__(
        self,
        spark,
        logger,
        n_threads,
        db_ref_peaks_properties,
        db_fingerprints_matched_properties,
        reference_peaks_table_name,
        fingerprints_matched_table_name,
        reference_peaks_delay_s=0,
        ref_restart_freq=20,
        after_window_surplus=15,
        before_window_surplus=15,
    ):
        """
        Initializes FingerprintsMatcher with default parameters.

        Args:
            spark (SparkSession): The Spark session object.
            logger (Logger): The logger object.
            n_threads (int): The number of threads to use for processing.
            db_ref_peaks_properties (dict): Properties for the database connection.
            db_fingerprints_matched_properties (dict): Properties for the matched fingerprints
            database connection.
            reference_peaks_table_name (str): The name of the reference peaks table.
            fingerprints_matched_table_name (str): The name of the matched fingerprints table.
            reference_peaks_delay_s (int): Delay for reference peaks.
            pk_restart_cnt (int): Counter for peak restarts.
            before_window_surplus (int): Time window before matching.
            after_window_surplus (int): Time window after matching.
        """
        self.logger = logger
        self.spark = spark
        self.n_threads = n_threads
        self.db_ref_peaks_properties = db_ref_peaks_properties
        self.db_fingerprints_matched_properties = db_fingerprints_matched_properties
        self.reference_peaks_table_name = reference_peaks_table_name
        self.fingerprints_matched_table_name = fingerprints_matched_table_name
        self.reference_peaks_delay_s = reference_peaks_delay_s
        self.pk_min_ts = 0
        self.pk_max_ts = 0
        self.ref_window_start_ts = 0
        self.ref_window_end_ts = 0
        self.ref_restart_idx = ref_restart_freq
        self.ref_restart_freq = ref_restart_freq
        self.before_window_surplus = before_window_surplus
        self.after_window_surplus = after_window_surplus
        self.ref_df = None

    def _match_fingerprints(self, miernik_fp_df):
        """
        Matches fingerprints.

        Args:
            fingerprints_df (DataFrame): DataFrame containing fingerprint data.
            ref_df (DataFrame): DataFrame containing reference fingerprints.

        Returns:
            int: The count of matched peaks.

        Example:
            fingerprints_df = spark.read.parquet("fingerprints.parquet")
            ref_df = spark.read.parquet("reference_peaks.parquet")
            FingerprintsMatcher._peak_matching(fingerprints_df, ref_df)
        """
        miernik_fp_df = miernik_fp_df.repartition(self.n_threads, "offset")
        # TODO(karolpustelnik): choose a better column for repartitioning
        self.ref_df = self.ref_df.repartition(self.n_threads, "offset").persist(
            StorageLevel.MEMORY_ONLY
        )
        self.logger.info(
            f"&&&& Peak matching: size of fingerprints df {miernik_fp_df.count()} and ref_df: {self.ref_df.count()}"
        )
        # Create temporary views for fingerprints and reference peaks
        miernik_fp_df.createOrReplaceGlobalTempView("fp")
        self.ref_df.registerTempTable("pk")

        # Check timestamps in fingerprints and peaks
        min_miernik_ts, max_miernik_ts = miernik_fp_df.select(
            F.min(F.col("ts")), F.max(F.col("ts"))
        ).first()
        min_ref_ts, max_ref_ts = self.ref_df.select(
            F.min(F.col("ts")), F.max(F.col("ts"))
        ).first()

        self.logger.info(
            f"&&&& Peak matching: fingerprints timestamp: \
                         min={datetime.utcfromtimestamp(min_miernik_ts)}, \
                         max={datetime.utcfromtimestamp(max_miernik_ts)}, \
                         reference peaks timestamp: min={datetime.utcfromtimestamp(min_ref_ts)}, \
                         max={datetime.utcfromtimestamp(max_ref_ts)}"
        )

        # SQL query for peak matching
        sql_query = """
        with fp_pk as (
        -- liczymy county z polaczen
        select count(1) cnt, int(fp.ts/30)*30 as time30s, fp.ts - pk.ts ts_diff, min(fp.tts) as start_time, imei, station_id
        from global_temp.fp inner join pk 
              on fp.hash_1=pk.hash_1 
              and (pk.ts between fp.ts-15 and fp.ts+15)
        group by int(fp.ts/30), imei, station_id, fp.ts - pk.ts
        ),
        -- sumujemy cnty dla imei, stacji, interwalow i przesuniec
        join_peaks_0 as (
        select sum(cnt) cnt, time30s, ts_diff, min(start_time) start_time, imei, station_id
        from fp_pk 
        group by time30s, imei, station_id, ts_diff
        ),
        -- bierzemy max z przesuniec
        join_peaks as (
        select max(cnt) cnt, time30s, min(start_time) start_time, imei, station_id
        from join_peaks_0
        group by time30s, imei, station_id
        having max(cnt) >= 10
        )
        select id, start_time, cnt, imei, station_id, 0 as channel, rnk
        from (
            select
                 imei || '_' || time30s as id, start_time, cnt, imei, station_id,
                 rank()       over (partition by imei, time30s order by cnt desc) as rnk
              from join_peaks_0
              ) ranked_peaks
            where rnk <= 2 
        """
        # Execute SQL query
        matched_df = self.spark.sql(sql_query).cache()
        # Count the matched peaks
        cnt = matched_df.count()
        spark_write_to_db(
            matched_df,
            self.db_fingerprints_matched_properties,
            self.fingerprints_matched_table_name,
        )
        # Drop temporary views
        self.spark.catalog.dropGlobalTempView("fp")
        self.logger.info(
            f"&&&& Peak matching: matched fingerprints, \
                         timestamp: min={datetime.utcfromtimestamp(min_miernik_ts)}, \
                         max={datetime.utcfromtimestamp(max_miernik_ts)}, \
                         timestamp of reference peaks: min={datetime.utcfromtimestamp(min_ref_ts)},\
                         max={datetime.utcfromtimestamp(max_ref_ts)}"
        )
        return cnt

    def match_miernik_ref(self, miernik_df):
        """
        Matches fingerprints from miernik with reference signal fingerprints.

        This method takes a DataFrame of fingerprints and matches them with
        the reference peaks.
        It updates the reference peaks DataFrame based on the specified time windows
        and performs peak matching.

        Args:
            fingerprints_df (DataFrame): DataFrame of fingerprints to match.

        Returns:
            None
        """
        beg_ts = time.time()
        miernik_df = miernik_df.persist(pyspark.StorageLevel.MEMORY_ONLY)
        miernik_df_count = miernik_df.count()
        self.logger.debug(
            f"++++ FingerprintsMatcher: fingerprints input for match \
                          {miernik_df_count}"
        )
        if miernik_df_count == 0:
            return

        miernik_df.createOrReplaceGlobalTempView("fps")
        overlap_start_ts = self.spark.sql(
            "select min(ts) minval from global_temp.fps"
        ).first()["minval"]
        overlap_end_ts = miernik_df.agg(dict(ts="max")).collect()[0][0]
        if overlap_end_ts is None:
            return
        if (
            self.ref_restart_idx < 1
            or self.ref_df is None
            or self.ref_window_end_ts < overlap_start_ts - self.before_window_surplus
        ):  # TODO(karolpustelnik):why is this necessary?
            self.ref_window_start_ts = overlap_start_ts - self.before_window_surplus
            self.ref_window_end_ts = overlap_end_ts + self.after_window_surplus
            self.ref_df = get_ref_fingerprints_df(
                self.spark,
                self.db_ref_peaks_properties,
                self.reference_peaks_table_name,
                self.ref_window_end_ts,
                self.ref_window_start_ts,
                self.reference_peaks_delay_s,
            )
            self.ref_df.cache()
            self.ref_restart_idx = self.ref_restart_freq
            self.logger.debug(
                f"++++ FingerprintsMatcher: new parralelization fingerprintMatcher: \
                       reference peaks from {datetime.utcfromtimestamp(self.ref_window_start_ts)} \
                       to {datetime.utcfromtimestamp(self.ref_window_end_ts)} - downloaded \
                       {self.ref_df.count()}"
            )
        else:
            old_ref_window_end_ts = self.ref_window_end_ts
            self.ref_window_start_ts = overlap_start_ts - self.before_window_surplus
            self.ref_window_end_ts = overlap_end_ts + self.after_window_surplus
            self.ref_df = self.ref_df.filter(f"ts > {self.ref_window_start_ts}")
            new_ref_fingerprints = get_ref_fingerprints_df(
                self.spark,
                self.db_ref_peaks_properties,
                self.reference_peaks_table_name,
                self.ref_window_end_ts,
                old_ref_window_end_ts,
                self.reference_peaks_delay_s,
            )
            self.ref_df = self.ref_df.union(new_ref_fingerprints)
            self.ref_df.persist(pyspark.StorageLevel.MEMORY_AND_DISK_2)
            self.ref_restart_idx -= 1
            self.logger.debug(
                f"++++ FingerprintsMatcher: new parralelization reference peaks \
                       from {datetime.utcfromtimestamp(self.ref_window_start_ts)} to \
                       {datetime.utcfromtimestamp(self.ref_window_end_ts)} - \
                       refreshed {self.ref_df.count()}"
            )
        self.logger.debug(
            f"++++ FingerprintsMatcher new parralelization: \
                          reference peaks processed in {time.time() - beg_ts} seconds"
        )
        beg_ts = time.time()
        cnt = self._match_fingerprints(miernik_df)
        self.logger.debug(
            f"++++ FingerprintsMatcher new parralelization: \
                          matched ref-finger cnt: {cnt} took {time.time() - beg_ts} seconds"
        )
