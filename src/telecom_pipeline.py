# ============================================================
# PHASE 2 - SP7
# End-to-End Telecom Spark Pipeline
# ============================================================

import argparse
import logging
import shutil
import sys
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType
)
from pyspark import StorageLevel
from pyspark.sql.functions import broadcast
import os
os.environ["HADOOP_HOME"] = r"D:\capstone1\hadoop"
os.environ["PATH"] = os.environ["PATH"] + r";D:\capstone1\hadoop\bin"

# ============================================================
# LOGGING
# ============================================================

def setup_logging(log_path):

    log_path = Path(log_path)

    log_path.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    logging.basicConfig(
        filename=str(log_path),
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    return logging.getLogger("telecom_pipeline")


# ============================================================
# PIPELINE CLASS
# ============================================================

class TelecomPipeline:

    def __init__(
        self,
        input_path,
        output_path,
        reference_path,
        logger
    ):

        self.input_path = Path(input_path)
        self.output_path = Path(output_path)
        self.reference_path = Path(reference_path)

        self.logger = logger

        self.spark = None

        self.raw_df = None
        self.cleaned_df = None
        self.hourly_grid_summary = None
        self.grid_lookup = None
        self.grid_activity_geo_df = None

        self.input_rows = 0
        self.rejected_rows = 0
        self.activity_nulls_handled = 0
        self.output_rows = 0

        self.start_time = None
        self.end_time = None


    # ========================================================
    # SPARK SESSION
    # ========================================================

    def create_spark(self):

        self.spark = (
            SparkSession.builder
            .appName("TelecomNetworkPipeline")
            .master("local[4]")
            .config("spark.driver.memory", "8g")
            .config("spark.driver.maxResultSize", "2g")
            .config("spark.sql.shuffle.partitions", "32")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.files.maxPartitionBytes", "64m")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .getOrCreate()
            )

        self.spark.sparkContext.setLogLevel("WARN")


        self.logger.info(
            "SparkSession created"
        )


    # ========================================================
    # 1. read_raw()
    # ========================================================

    def read_raw(self):

        self.logger.info(
            "Starting read_raw()"
        )

        # ----------------------------------------------------
        # Validate input directory
        # ----------------------------------------------------

        if not self.input_path.exists():

            raise FileNotFoundError(
                f"Input path does not exist: "
                f"{self.input_path}"
            )


        # ----------------------------------------------------
        # Find only the required daily files.
        #
        # IMPORTANT:
        # The -mi- pattern is deliberate.
        # ----------------------------------------------------

        input_files = sorted(
            self.input_path.glob(
                "sms-call-internet-mi-*.csv"
            )
        )


        if not input_files:

            raise FileNotFoundError(
                "No input files found matching "
                f"'sms-call-internet-mi-*.csv' "
                f"under {self.input_path}"
            )


        self.logger.info(
            "Input files found: %d",
            len(input_files)
        )


        for file_path in input_files:

            self.logger.info(
                "Input file: %s",
                file_path
            )


        # ----------------------------------------------------
        # Read raw files
        #
        # inferSchema=False is intentional here.
        # We preserve raw values before cleaning.
        # ----------------------------------------------------

        self.raw_df = (
            self.spark.read
            .option("header", True)
            .option("inferSchema", False)
            .csv(
                [str(file) for file in input_files]
            )
        )


        # ----------------------------------------------------
        # Add source file for traceability
        # ----------------------------------------------------

        self.raw_df = (
            self.raw_df
            .withColumn(
                "_source_file",
                F.input_file_name()
            )
        )


        self.input_rows = (
            self.raw_df.count()
        )


        self.logger.info(
            "Raw input rows: %d",
            self.input_rows
        )


        self.logger.info(
            "read_raw() completed"
        )


        return self.raw_df


    # ========================================================
    # 2. clean()
    # ========================================================

    def clean(self):

        self.logger.info(
            "Starting clean()"
        )


        df = self.raw_df

      

        if df is None:

            raise RuntimeError(
                "clean() called before read_raw()"
            )


        # ----------------------------------------------------
        # Rename raw columns
        #
        # Adjust this mapping if your actual SP1 raw names
        # differ.
        # ----------------------------------------------------

        rename_mapping = {
            "datetime":"timestamp",
            "CellID": "grid_id",
            "countrycode": "country_code",
            "smsin": "sms_in",
            "smsout": "sms_out",
            "callin": "call_in",
            "callout": "call_out",
            "internet": "internet_activity"
        }


        for old_name, new_name in rename_mapping.items():

            if old_name in df.columns:

                df = df.withColumnRenamed(
                    old_name,
                    new_name
                )

      
        # ----------------------------------------------------
        # Check required columns
        # ----------------------------------------------------

        required_columns = [
            "timestamp",
            "grid_id",
            "sms_in",
            "sms_out",
            "call_in",
            "call_out",
            "internet_activity"
        ]
        

        missing_columns = [
            column
            for column in required_columns
            if column not in df.columns
        ]


        if missing_columns:

            raise ValueError(
                "Missing required columns: "
                f"{missing_columns}"
            )


        # ----------------------------------------------------
        # Cast timestamp
        # ----------------------------------------------------

        df = df.withColumn(
            "timestamp",
            F.to_timestamp(
                F.col("timestamp")
            )
        )


        # ----------------------------------------------------
        # Cast activity measures
        # ----------------------------------------------------

        activity_columns = [
            "sms_in",
            "sms_out",
            "call_in",
            "call_out",
            "internet_activity"
        ]


        for column in activity_columns:

            df = df.withColumn(
                column,
                F.col(column).cast(
                    DoubleType()
                )
            )


        # ----------------------------------------------------
        # Profile nulls BEFORE null-to-zero
        # ----------------------------------------------------

        null_expressions = [
            F.sum(
                F.when(
                    F.col(column).isNull(),
                    1
                ).otherwise(0)
            ).alias(column)
            for column in activity_columns
        ]


        null_profile = (
            df.select(
                *null_expressions
            )
            .collect()[0]
        )


        total_activity_nulls = 0


        for column in activity_columns:

            null_count = null_profile[column]

            if null_count is None:

                null_count = 0

            total_activity_nulls += null_count


            self.logger.info(
                "Activity nulls - %s: %d",
                column,
                null_count
            )


        self.activity_nulls_handled = (
            total_activity_nulls
        )


        self.logger.info(
            "Total activity nulls before "
            "null-to-zero rule: %d",
            self.activity_nulls_handled
        )


        # ----------------------------------------------------
        # Identify invalid rows
        #
        # Missing grid
        # Missing timestamp
        # Negative activity
        # ----------------------------------------------------

        invalid_condition = (
            F.col("grid_id").isNull()
            | F.col("timestamp").isNull()
            | (F.col("sms_in").isNotNull() & (F.col("sms_in") < 0))
            | (F.col("sms_out").isNotNull() & (F.col("sms_out") < 0))
            | (F.col("call_in").isNotNull() & (F.col("call_in") < 0))
            | (F.col("call_out").isNotNull() & (F.col("call_out") < 0))
            | (F.col("internet_activity").isNotNull() & (F.col("internet_activity") < 0))
        )


        rejected_df = (
            df.filter(
                invalid_condition
            )
        )


        self.rejected_rows = (
            rejected_df.count()
        )


        self.logger.info(
            "Rejected rows: %d",
            self.rejected_rows
        )


        # ----------------------------------------------------
        # Keep only valid rows
        # ----------------------------------------------------

        df = (
            df.filter(
                ~invalid_condition
            )
        )


        # ----------------------------------------------------
        # Curated null-to-zero rule
        #
        # IMPORTANT:
        # This happens AFTER raw null profiling.
        # ----------------------------------------------------

        for column in activity_columns:

            df = df.withColumn(
                column,
                F.coalesce(
                    F.col(column),
                    F.lit(0.0)
                )
            )


        # ----------------------------------------------------
        # Create total SMS
        # ----------------------------------------------------

        df = df.withColumn(
            "total_sms",
            F.col("sms_in")
            + F.col("sms_out")
        )


        # ----------------------------------------------------
        # Create total calls
        # ----------------------------------------------------

        df = df.withColumn(
            "total_calls",
            F.col("call_in")
            + F.col("call_out")
        )


        # ----------------------------------------------------
        # Project-defined total activity
        # ----------------------------------------------------

        df = df.withColumn(
            "total_activity",
            F.col("total_sms")
            + F.col("total_calls")
            + F.col("internet_activity")
        )


        # ----------------------------------------------------
        # Date / hour / day_of_week
        # ----------------------------------------------------

        df = (
            df
            .withColumn(
                "date",
                F.to_date(
                    F.col("timestamp")
                )
            )
            .withColumn(
                "hour",
                F.hour(
                    F.col("timestamp")
                )
            )
            .withColumn(
                "day_of_week",
                F.dayofweek(
                    F.col("timestamp")
                )
        )
        )

        self.cleaned_df = df


        cleaned_count = (
            self.cleaned_df.count()
        )

        cleaned_count = self.cleaned_df.count()


        self.logger.info(
            "Cleaned rows: %d",
            cleaned_count
        )


        self.logger.info(
            "Null activity values handled: %d",
            self.activity_nulls_handled
        )


        self.logger.info(
            "clean() completed"
        )




        return self.cleaned_df


    # ========================================================
    # 3. aggregate()
    # ========================================================

    def aggregate(self):

        self.logger.info(
            "Starting aggregate()"
        )


        if self.cleaned_df is None:

            raise RuntimeError(
                "aggregate() called before clean()"
            )


        # ----------------------------------------------------
        # Collapse country-code records
        #
        # One row per timestamp + grid_id
        # ----------------------------------------------------

        hourly = (
            self.cleaned_df
            .groupBy(
                "timestamp",
                "grid_id"
            )
            .agg(

                F.sum(
                    "sms_in"
                ).alias(
                    "sms_in"
                ),

                F.sum(
                    "sms_out"
                ).alias(
                    "sms_out"
                ),

                F.sum(
                    "call_in"
                ).alias(
                    "call_in"
                ),

                F.sum(
                    "call_out"
                ).alias(
                    "call_out"
                ),

                F.sum(
                    "internet_activity"
                ).alias(
                    "internet_activity"
                )
            )
        )


        # ----------------------------------------------------
        # Total SMS
        # ----------------------------------------------------

        hourly = hourly.withColumn(
            "total_sms",
            F.col("sms_in")
            + F.col("sms_out")
        )


        # ----------------------------------------------------
        # Total calls
        # ----------------------------------------------------

        hourly = hourly.withColumn(
            "total_calls",
            F.col("call_in")
            + F.col("call_out")
        )


        # ----------------------------------------------------
        # Total activity
        # ----------------------------------------------------

        hourly = hourly.withColumn(
            "total_activity",
            F.col("total_sms")
            + F.col("total_calls")
            + F.col("internet_activity")
        )


        # ----------------------------------------------------
        # Date
        # ----------------------------------------------------

        hourly = hourly.withColumn(
            "date",
            F.to_date(
                F.col("timestamp")
            )
        )


        # ----------------------------------------------------
        # Daily activity per grid
        # ----------------------------------------------------

        daily = (
            hourly
            .groupBy(
                "date",
                "grid_id"
            )
            .agg(
                F.sum(
                    "total_activity"
                ).alias(
                    "daily_activity"
                ),

                F.sum(
                    "total_sms"
                ).alias(
                    "daily_sms"
                ),

                F.sum(
                    "total_calls"
                ).alias(
                    "daily_calls"
                ),

                F.sum(
                    "internet_activity"
                ).alias(
                    "daily_internet"
                )
            )
        )


        self.daily_traffic_summary = daily


        # ----------------------------------------------------
        # Internet share
        # ----------------------------------------------------

        hourly = hourly.withColumn(
            "internet_share",
            F.when(
                F.col("total_activity") > 0,
                F.col("internet_activity")
                / F.col("total_activity")
            ).otherwise(
                F.lit(0.0)
            )
        )


        # ----------------------------------------------------
        # Peak activity hour
        #
        # Global peak hour across the consolidated data.
        # ----------------------------------------------------

    
        
        peak_row = (
            hourly
            .groupBy("timestamp")
            .agg(
                F.sum(
                    "total_activity"
                ).alias(
                    "total_activity"
                )
            )
            .orderBy(
                F.desc(
                    "total_activity"
                )
            )
            .first()
        )


        if peak_row is not None:

            self.logger.info(
                "Peak activity hour: %s | activity=%s",
                peak_row["timestamp"],
                peak_row["total_activity"]
            )


        # ----------------------------------------------------
        # Top ten high-activity grids
        # ----------------------------------------------------

        top_grids = (
            hourly
            .groupBy(
                "grid_id"
            )
            .agg(
                F.sum(
                    "total_activity"
                ).alias(
                    "total_grid_activity"
                )
            )
            .orderBy(
                F.desc(
                    "total_grid_activity"
                )
            )
            .limit(10)
        )


        self.top_grids = top_grids


        # ----------------------------------------------------
        # Canonical downstream DataFrame
        #
        # Exactly one record per grid + hour.
        # ----------------------------------------------------

        self.hourly_grid_summary = (
            hourly
            .select(
                "timestamp",
                "grid_id",
                "sms_in",
                "sms_out",
                "call_in",
                "call_out",
                "internet_activity",
                "total_sms",
                "total_calls",
                "total_activity",
                "internet_share",
                "date"
            )
           
        )


        self.output_rows = (
            self.hourly_grid_summary.count()
        )


        self.logger.info(
            "hourly_grid_summary rows: %d",
            self.output_rows
        )


        self.logger.info(
            "aggregate() completed"
        )


        return self.hourly_grid_summary


    # ========================================================
    # 4. enrich()
    # ========================================================

    def enrich(self):

        self.logger.info(
            "Starting enrich()"
        )


        if self.hourly_grid_summary is None:

            raise RuntimeError(
                "enrich() called before aggregate()"
            )


        # ----------------------------------------------------
        # Locate GeoJSON
        # ----------------------------------------------------

        geojson_path = (
            self.reference_path
            / "milano-grid.geojson"
        )


        if not geojson_path.exists():

            raise FileNotFoundError(
                f"Reference GeoJSON not found: "
                f"{geojson_path}"
            )


        # ----------------------------------------------------
        # Read GeoJSON
        #
        # Spark's JSON reader reads GeoJSON features into
        # a DataFrame.
        # ----------------------------------------------------

        geojson_df = (
            self.spark.read
            .option("multiline", "true")
            .json(
                str(geojson_path)
            )
        )
# ----------------------------------------------------
        # Flatten properties.cellId -> grid_id
        # ----------------------------------------------------
        

        self.grid_lookup = (
                geojson_df
                .select(F.explode("features").alias("feature"))
                .select(
                    F.col("feature.properties.cellId").cast("string").alias("grid_id"),
                    F.col("feature.geometry").alias("geometry")
                )
                .filter(F.col("grid_id").isNotNull())
            )

        grid_lookup_count = (
            self.grid_lookup.count()
        )


        self.logger.info(
            "Grid lookup rows: %d",
            grid_lookup_count
        )


        # ----------------------------------------------------
        # Broadcast join
        #
        # Grid reference is small compared with activity data.
        # ----------------------------------------------------

        self.grid_activity_geo_df = (
            self.hourly_grid_summary
            .join(
                broadcast(
                    self.grid_lookup
                ),
                on="grid_id",
                how="left"
            )
        )


        # ----------------------------------------------------
        # Validate distinct activity grids
        # ----------------------------------------------------

        activity_grids_before = (
            self.hourly_grid_summary
            .select("grid_id")
            .distinct()
            .count()
        )


        activity_grids_after = (
            self.grid_activity_geo_df
            .select("grid_id")
            .distinct()
            .count()
        )


        missing_geometry_grids = (
            self.grid_activity_geo_df
            .filter(
                F.col("geometry").isNull()
            )
            .select("grid_id")
            .distinct()
            .count()
        )


        if activity_grids_before > 0:

            coverage_percentage = (
                (
                    activity_grids_before
                    - missing_geometry_grids
                )
                / activity_grids_before
            ) * 100

        else:

            coverage_percentage = 0.0


        self.logger.info(
            "Distinct activity grids before enrichment: %d",
            activity_grids_before
        )

        self.logger.info(
            "Distinct activity grids after enrichment: %d",
            activity_grids_after
        )

        self.logger.info(
            "Grids missing geometry: %d",
            missing_geometry_grids
        )

        self.logger.info(
            "Geometry enrichment coverage: %.2f%%",
            coverage_percentage
        )


        # ----------------------------------------------------
        # Unmatched grids
        # ----------------------------------------------------

        unmatched_grid_ids = (
            self.grid_activity_geo_df
            .filter(
                F.col("geometry").isNull()
            )
            .select("grid_id")
            .distinct()
        )


        unmatched_count = (
            unmatched_grid_ids.count()
        )


        self.logger.info(
            "Unmatched grid IDs: %d",
            unmatched_count
        )


        # ----------------------------------------------------
        # Top grids with geometry retained
        # ----------------------------------------------------

        self.top_grids_with_geometry = (
            self.top_grids
            .join(
                broadcast(
                    self.grid_lookup
                ),
                on="grid_id",
                how="left"
            )
        )


        self.logger.info(
            "Top high-activity grids with geometry prepared"
        )


        self.logger.info(
            "enrich() completed"
        )


        return self.grid_activity_geo_df


    # ========================================================
    # 5. write_outputs()
    # ========================================================

    def write_outputs(self):

        self.logger.info(
            "Starting write_outputs()"
        )


        # ----------------------------------------------------
        # Create output directories
        # ----------------------------------------------------

        curated_path = (
            self.output_path
            / "clean_activity_parquet"
        )

       
        hourly_path = (
            self.output_path
            / "hourly_grid_summary_parquet"
        )

        dashboard_path = (
            self.output_path
            / "dashboard_summary"
        )


        self.output_path.mkdir(
            parents=True,
            exist_ok=True
        )


        # ----------------------------------------------------
        # Clean activity Parquet
        # ----------------------------------------------------
        
        self.logger.info(
            "About to write clean activity Parquet: %s",
            curated_path
        )

        self.logger.info(
            "Clean activity columns: %s",
            self.cleaned_df.columns
        )

        self.logger.info(
            "Clean activity schema:"
    )

        
        self.cleaned_df = self.cleaned_df.persist(StorageLevel.DISK_ONLY)
        self.cleaned_df.count()

        self.hourly_grid_summary = self.hourly_grid_summary.persist(
        StorageLevel.DISK_ONLY
    )
        self.hourly_grid_summary.count()



        (
            self.cleaned_df
            .repartition(32, "date")
            .write
            .mode("overwrite")
            .option("compression", "snappy")
            .partitionBy("date")
            .parquet(str(curated_path))
                )
        
        
        self.logger.info(
                    "Clean activity Parquet write: SUCCESS"
            )

        self.logger.info(
            "Clean activity Parquet written: %s",
            curated_path
        )


        # ----------------------------------------------------
        # Hourly grid summary
        #
        # Geometry is intentionally NOT included.
        # ----------------------------------------------------

        hourly_output = (
            self.hourly_grid_summary
            .select(
                "timestamp",
                "grid_id",
                "sms_in",
                "sms_out",
                "call_in",
                "call_out",
                "internet_activity",
                "total_sms",
                "total_calls",
                "total_activity",
                "internet_share",
                "date"
            )
        )


        (
            hourly_output
            .write
            .mode("overwrite")
            .parquet(
                str(hourly_path)
            )
        )


        self.logger.info(
            "hourly_grid_summary Parquet written: %s",
            hourly_path
        )


        # ----------------------------------------------------
        # Dashboard summary
        # ----------------------------------------------------

        dashboard_summary = (
            self.hourly_grid_summary
            .groupBy("timestamp")
            .agg(

                F.sum(
                    "total_activity"
                ).alias(
                    "total_activity"
                ),

                F.sum(
                    "total_sms"
                ).alias(
                    "total_sms"
                ),

                F.sum(
                    "total_calls"
                ).alias(
                    "total_calls"
                ),

                F.sum(
                    "internet_activity"
                ).alias(
                    "internet_activity"
                ),

                F.countDistinct(
                    "grid_id"
                ).alias(
                    "active_grid_count"
                )
            )
            .orderBy(
                "timestamp"
            )
        )


        (
            dashboard_summary
            .coalesce(1)
            .write
            .mode("overwrite")
            .option(
                "header",
                True
            )
            .csv(
                str(dashboard_path)
            )
        )


        self.logger.info(
            "Dashboard CSV written: %s",
            dashboard_path
        )


        # ----------------------------------------------------
        # Read-back validation
        # ----------------------------------------------------

        clean_readback = (
            self.spark.read
            .parquet(
                str(curated_path)
            )
        )


        hourly_readback = (
            self.spark.read
            .parquet(
                str(hourly_path)
            )
        )


        clean_readback_count = (
            clean_readback.count()
        )

        hourly_readback_count = (
            hourly_readback.count()
        )


        expected_clean_count = (
            self.cleaned_df.count()
        )

        expected_hourly_count = (
            self.hourly_grid_summary.count()
        )


        self.logger.info(
            "Clean output rows: %d",
            clean_readback_count
        )

        self.logger.info(
            "Expected clean rows: %d",
            expected_clean_count
        )

        self.logger.info(
            "Hourly output rows: %d",
            hourly_readback_count
        )

        self.logger.info(
            "Expected hourly rows: %d",
            expected_hourly_count
        )


        if (
            clean_readback_count
            != expected_clean_count
        ):

            raise RuntimeError(
                "Clean activity Parquet row-count "
                "validation failed"
            )


        if (
            hourly_readback_count
            != expected_hourly_count
        ):

            raise RuntimeError(
                "Hourly grid Parquet row-count "
                "validation failed"
            )


        self.logger.info(
            "Parquet read-back validation: PASS"
        )


        self.logger.info(
            "write_outputs() completed"
        )


    # ========================================================
    # 6. MAIN PIPELINE
    # ========================================================

    def run(self):

        self.start_time = datetime.now()


        self.logger.info("=" * 60)
        self.logger.info(
            "TELECOM PIPELINE START"
        )
        self.logger.info(
            "Start time: %s",
            self.start_time
        )
        self.logger.info("=" * 60)


        try:

            self.create_spark()

            self.read_raw()

            self.clean()

            self.aggregate()

            self.enrich()

            self.write_outputs()


            self.end_time = datetime.now()


            duration = (
                self.end_time
                - self.start_time
            ).total_seconds()


            self.logger.info(
                "Input rows: %d",
                self.input_rows
            )

            self.logger.info(
                "Rejected rows: %d",
                self.rejected_rows
            )

            self.logger.info(
                "Activity nulls handled: %d",
                self.activity_nulls_handled
            )

            self.logger.info(
                "Output rows: %d",
                self.output_rows
            )

            self.logger.info(
                "End time: %s",
                self.end_time
            )

            self.logger.info(
                "Duration seconds: %.2f",
                duration
            )

            self.logger.info(
                "FINAL STATUS: SUCCESS"
            )

            self.logger.info("=" * 60)


            return True


        except Exception as exc:

            self.end_time = datetime.now()


            self.logger.exception(
                "PIPELINE FAILED: %s",
                exc
            )


            self.logger.info(
                "End time: %s",
                self.end_time
            )

            self.logger.error(
                "FINAL STATUS: FAILED"
            )

            self.logger.info("=" * 60)


            return False


        finally:

            if self.spark is not None:

                self.spark.stop()

                self.logger.info(
                    "SparkSession stopped"
                )


# ============================================================
# COMMAND-LINE ARGUMENTS
# ============================================================

def parse_arguments():

    parser = argparse.ArgumentParser(
        description=(
            "End-to-end telecom network Spark pipeline"
        )
    )


    parser.add_argument(
        "--input",
        required=True,
        help=(
            "Input directory containing "
            "sms-call-internet-mi-*.csv files"
        )
    )


    parser.add_argument(
        "--output",
        required=True,
        help=(
            "Output directory for curated "
            "Parquet and dashboard outputs"
        )
    )


    parser.add_argument(
        "--reference",
        required=True,
        help=(
            "Reference directory containing "
            "milano-grid.geojson"
        )
    )


    parser.add_argument(
        "--log",
        default="data/logs/telecom_pipeline.log",
        help=(
            "Pipeline log file path"
        )
    )


    return parser.parse_args()


# ============================================================
# MAIN
# ============================================================

def main():

    args = parse_arguments()


    logger = setup_logging(
        args.log
    )


    pipeline = TelecomPipeline(
        input_path=args.input,
        output_path=args.output,
        reference_path=args.reference,
        logger=logger
    )


    success = pipeline.run()


    if not success:

        sys.exit(1)


    print(
        "\nTelecom pipeline completed successfully."
    )


    print(
        f"Log: {args.log}"
    )


if __name__ == "__main__":

    main()

