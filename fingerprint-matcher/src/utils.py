# pylint: skip-file
from datetime import datetime
from logging.handlers import RotatingFileHandler
import base64
import math
import logging
import pandas as pd
import src.app_data_pb2 as app_data_pb2
from src.settings import SECOND_TO_MS


def add_processed_up_to_row(
    spark,
    db_ref_peaks_properties,
    output_table_name,
    data_ts,
    batch_ts,
    batchid,
    run_id,
):
    """
    Adds a new row to the 'output' table with information about the processed data.

    Parameters:
        data_ts (datetime): Timestamp of the data.
        batch_ts (datetime): Timestamp of the batch.
        batchid (int): Batch ID.
        run_id (str): Identifier for the run.
        url (str): URL of the database.
        properties (dict): Connection properties.

    Returns:
        None
    """

    schema = "ts timestamp, batch_ts timestamp, batchid integer, run_id string"
    df = spark.createDataFrame([(data_ts, batch_ts, batchid, run_id)], schema=schema)
    spark_write_to_db(df, db_ref_peaks_properties, output_table_name)


def date_to_ts(date):
    """
    Converts a datetime object to a timestamp.
    """
    return datetime(date.year, date.month, date.day)


def get_syslog_logger(logg_file_path):
    """
    Get a logger instance for the Spark worker.
    """
    logger = logging.getLogger("spark-worker")
    logger.setLevel(logging.DEBUG)
    if len(logger.handlers) == 0:
        file_handler = RotatingFileHandler(logg_file_path)
        file_handler.setLevel(logging.DEBUG)
        logger.addHandler(file_handler)
    return logger


def get_matcher_stream(spark, batch_process, matcher_processing_time_s):
    """
    Configure streaming DataFrame for fingerprint matching.

    Returns:
    - DataFrameWriter: DataFrameWriter configured for streaming processing.
    """
    out_schema = "lp int, hash_1 string, hash_2 string, t int, f int, id string, ts bigint, tts timestamp, offset bigint, created_at timestamp, imei string, batchid integer"
    # Read streaming data from Parquet files with the defined schema.
    df_stream = (
        spark.readStream.format("parquet")
        .schema(out_schema)
        .option("cleanSource", "DELETE")
        .option("maxFilesPerTrigger", 1)
        .load("./fingerprints-chunked/*parquet")
    )
    return df_stream.writeStream.trigger(
        processingTime=f"{matcher_processing_time_s} seconds"
    ).foreachBatch(batch_process)


def spark_read_from_db(spark, properties, sql_pattern):
    """

    This function connects to a JDBC source and reads data into a Spark DataFrame based
    on the provided SQL query pattern.
    It partitions the data for parallel processing.

    Parameters:
    spark (SparkSession): The Spark session object.
    properties (dict): A dictionary of properties to pass to the JDBC connection.
    sql_pattern (str): The SQL query pattern to execute on the database.
    Returns:
    DataFrame: A Spark DataFrame containing the data read from the JDBC source.

    Example:
    sql_pattern = "SELECT * FROM table WHERE condition"
    properties = {
        "user": "username",
        "password": "password",
        "driver": "org.postgresql.Driver"
    }
    df = spark_jdbc_reader(spark, properties, sql_pattern)
    """
    df = spark.read.jdbc(
        url=f"jdbc:postgresql://{properties['hostname']}:{properties['port']}/{properties['db']}",
        table=f"({sql_pattern}) AS alias",  # Wrapping the query in parentheses and aliasing it
        properties=properties,
        numPartitions=1,
    )

    return df


def spark_write_to_db(df, properties, table_name):
    """

    This function connects to a JDBC source and writes data from a Spark DataFrame
    to the specified table.

    Parameters:
    df (DataFrame): The Spark DataFrame containing the data to write.
    properties (dict): A dictionary of properties to pass to the JDBC
    table_name (str): The name of the table to write the data to.
    """

    df.write.jdbc(
        url=f"jdbc:postgresql://{properties['hostname']}:{properties['port']}/{properties['db']}",
        table=table_name,
        mode="append",
        properties=properties,
    )


def get_protobuf(
    spark, db_fingerprints_properites, raw_fingerprints_table_name, ts_end, ts_start
):
    """
    This function connects to a PostgreSQL database and retrieves packed
    protobuf peaks data
    that were created within the given time range.
    The time range is specified by the timestamps `ts_end` and `ts_start`.

    Args:
        spark (SparkSession): The Spark session object.
        db_fingerprints_properites (dict): A dictionary of properties to pass
        to the JDBC connection.
        ts_end (datetime.datetime): The end timestamp of the time range (inclusive).
        ts_start (datetime.datetime): The start timestamp of the time range (exclusive).

    Returns:
        pyspark.sql.DataFrame: A Spark DataFrame containing the
        fetched protbuf peaks data.

    """
    # Convert datetime objects to string representations
    ts_end_str = ts_end.strftime("%Y-%m-%d %X")
    ts_start_str = ts_start.strftime("%Y-%m-%d %X")
    # Define the SQL query pattern
    sql_pattern = f"""
    select id, created_at, uuid, start_time, duration_ms, fingerprint, imei, device_id,
    ( '{ts_end_str}'::timestamptz ) as ts_end,
    ( '{ts_start_str}'::timestamptz ) as ts_start
    from {raw_fingerprints_table_name}
    where created_at > ( '{ts_start_str}'::timestamptz )
    and created_at <= ( '{ts_end_str}'::timestamptz )
    and length(fingerprint) > 16
    """.format(
        ts_end_strr=ts_end_str, ts_start_str=ts_start_str
    )
    return spark_read_from_db(spark, db_fingerprints_properites, sql_pattern)


def parse_protobuf(df, batch_id):
    """
    Parses fingerprint data from a DataFrame and returns a processed DataFrame
    with extracted information.

    This function takes a DataFrame containing fingerprint data and a batch ID,
    parses the fingerprint data using protobuf, and extracts relevant information
    into a new DataFrame. Each fingerprint is decoded and processed to generate various
    hash values and timestamps.

    Args:
        df (pandas.DataFrame): A DataFrame containing the fingerprint data to be parsed.
        The DataFrame should have columns:
        'fingerprint', 'id', 'start_time', 'ts', 'created_at', and 'imei'.
        batch_id (str): The batch ID to be associated with the parsed data.

    Returns:
        pandas.DataFrame: A DataFrame containing the parsed and processed
        fingerprint data.
        The output DataFrame contains columns:
        'lp', 'hash_1', 'hash_2', 't', 'f', 'id', 'ts', 'tts', 'offset',
        'created_at', 'imei', and 'batchid'.
    """
    results = []
    for row in df.itertuples(index=False):
        fingerprint, row_id, start_time, ts, created_at, imei = (
            row.fingerprint,
            row.id,
            row.start_time,
            row.ts,
            row.created_at,
            row.imei,
        )
        pb = app_data_pb2.PeaksBatch()
        pb.ParseFromString(base64.b64decode(fingerprint))
        element_prev = 1
        lp = 0
        offset = 0
        for _, element in enumerate(pb.channel1):
            t = int(element / 2048)
            f = abs((element % 2048))
            prev = element_prev % 2048
            f = 1 if f == 0 else f
            prev = 1 if prev == 0 else prev
            offset += t
            results.append(
                {
                    "lp": lp,
                    "hash_1": f"{int(prev)}:{f}:{t}",
                    "hash_2": f"{int(math.log(prev) * 10)}:{int(math.log(f) * 10)}:{t}",
                    "t": t,
                    "f": f,
                    "id": str(row_id),  # Ensure id is a string
                    "ts": int((int(start_time) + offset * 92.8) / SECOND_TO_MS),
                    "tts": pd.Timestamp(ts),  # Ensure tts is a Timestamp
                    "offset": int(offset),
                    "created_at": pd.Timestamp(
                        created_at
                    ),  # Ensure created_at is a Timestamp
                    "imei": str(imei),  # Ensure imei is a string
                    "batchid": batch_id,  # Add batchid
                }
            )
            lp += 1
            element_prev = element
    return pd.DataFrame(results)


# for processing parsed fingerprints


def get_fingerprints_df(
    spark, db_ref_peaks_properties, fingerprints_parsed_table_name, ts_end, ts_start
):
    """

    This function constructs and executes a SQL query to fetch fingerprints from the
    `FINGERPRINTS_PARSED_TABLE_NAME` table within the specified timestamp range.

    Args:
        spark (SparkSession): The Spark session object.
        db_ref_peaks_properties (dict): A dictionary of properties
        to pass to the JDBC connection.
        fingerprints_parsed_table_name (str): The name of the table containing
        the parsed fingerprints data.
        ts_end (int): The maximum timestamp value.
        ts_start (int): The minimum timestamp value.

    Returns:
        DataFrame: A DataFrame containing the fetched fingerprints data within
        the specified time range.

    """
    where_condition = f"""tts > to_timestamp({int(ts_start)}-90) and tts < to_timestamp({int(ts_end)}+90)"""
    sql_pattern = f"""
                select * 
                from {fingerprints_parsed_table_name}
                where {where_condition}
           """
    fp_df = spark_read_from_db(spark, db_ref_peaks_properties, sql_pattern)
    return fp_df


# for processing reference peaks


def get_ref_fingerprints_df(
    spark,
    db_ref_peaks_properties,
    reference_peaks_table_name,
    ts_end,
    ts_start,
    reference_peaks_delay_s,
):
    """
    Retrieves reference fingerprints data within a specified timestamp range.

    This function retrieves reference peaks data within the specified timestamp range
    from a database by constructing a SQL query with a WHERE condition based
    on the provided minimum and maximum timestamp values.

    Args:
        spark (SparkSession): The Spark session object.
        db_ref_peaks_properties (dict): A dictionary of properties
        to pass to the JDBC connection.
        reference_peaks_table_name (str): The name of the table containing
        the reference peaks data.
        ts_end (int): The end timestamp of the time range.
        ts_start (int): The start timestamp of the time range.
        reference_peaks_delay_s (int): The delay in seconds for the reference
        peaks data. It is used to
        adjust the timestamp range for diffrent systems (macos, linux, etc.)

    Returns:
        DataFrame: A DataFrame containing the retrieved reference peaks data
        within the specified timestamp range.

    """
    where_condition = f"""
        tts > to_timestamp({ts_start-reference_peaks_delay_s}-100) and tts < to_timestamp({ts_end-reference_peaks_delay_s}+100)
    """
    sql_pattern = f"""
                select * 
                from {reference_peaks_table_name}
                where {where_condition}
           """

    pk_df = spark_read_from_db(spark, db_ref_peaks_properties, sql_pattern)
    return pk_df
