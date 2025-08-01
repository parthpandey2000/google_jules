import argparse
import sys
from etl_pipeline.src.utils import get_spark_session, load_config, notify
from etl_pipeline.src.logger import get_logger
from etl_pipeline.dq_checks.source_dq import run_source_dq
from etl_pipeline.dq_checks.target_dq import run_target_dq
from etl_pipeline.src.etl import run_transformation

# Initialize logger
logger = get_logger(__name__)

def get_config_for_item(item_name: str, configs: list, item_key: str = 'name') -> dict:
    """Finds the configuration for a specific item by its name/key."""
    for config in configs:
        if config.get(item_key) == item_name:
            return config
    return None

def main():
    """
    Main entry point for the ETL pipeline.
    Orchestrates the execution of DQ checks and transformations.

    Usage:
    - Run full pipeline: python -m etl_pipeline.main --run-all
    - Run source DQ for one table: python -m etl_pipeline.main --source-dq --table <source_name>
    - Run transformation for one table: python -m etl_pipeline.main --transform --table <target_name>
    - Run target DQ for one table: python -m etl_pipeline.main --target-dq --table <target_name>
    """
    # --- Argument Parsing ---
    parser = argparse.ArgumentParser(description="PySpark ETL Pipeline Orchestrator")
    parser.add_argument("--run-all", action="store_true", help="Run the entire ETL pipeline.")
    parser.add_argument("--source-dq", action="store_true", help="Run source data quality checks.")
    parser.add_argument("--transform", action="store_true", help="Run a transformation.")
    parser.add_argument("--target-dq", action="store_true", help="Run target data quality checks.")
    parser.add_argument("--table", type=str, help="Specify the name of the source/target table to process.")

    args = parser.parse_args()

    # --- Initial Setup ---
    try:
        spark = get_spark_session("ETLOrchestrator")

        # Load configurations
        # Note: In a real Databricks environment, paths might be different (e.g., /dbfs/...)
        sources_config = load_config("etl_pipeline/configs/sources.yaml")['sources']
        targets_config = load_config("etl_pipeline/configs/targets.yaml")['targets']
        transforms_config = load_config("etl_pipeline/configs/transformations.yaml")['transformations']

    except Exception as e:
        logger.error(f"Failed during initial setup: {e}")
        notify("FAILURE", "Pipeline Setup Failed", str(e))
        sys.exit(1)

    # --- Execution Logic ---
    try:
        if args.run_all:
            logger.info("Starting full pipeline run...")

            # 1. Source DQ
            logger.info("--- Running Source DQ for all sources ---")
            for source in sources_config:
                run_source_dq(spark, source)

            # 2. Transformations (in order)
            # Simple dependency resolution: dims first, then facts.
            # A more robust solution would use a graph library like networkx.
            logger.info("--- Running All Transformations ---")
            run_order = sorted(transforms_config, key=lambda t: t['target_table'].startswith('fact'))

            for transform in run_order:
                run_transformation(spark, transform, sources_config, targets_config)

                # 3. Target DQ (run immediately after each transformation)
                logger.info("--- Running Target DQ ---")
                target_name = transform['target_table']
                target_conf = get_config_for_item(target_name, targets_config)
                if target_conf:
                    run_target_dq(spark, target_conf)
                else:
                    logger.warning(f"No target configuration found for {target_name}, skipping target DQ.")

            notify("SUCCESS", "Full Pipeline Run", "The entire ETL pipeline completed successfully.")

        elif args.source_dq:
            if not args.table:
                raise ValueError("--table argument is required for --source-dq.")
            conf = get_config_for_item(args.table, sources_config)
            if not conf:
                raise ValueError(f"No source configuration found for table: {args.table}")
            run_source_dq(spark, conf)

        elif args.transform:
            if not args.table:
                raise ValueError("--table argument is required for --transform.")
            conf = get_config_for_item(args.table, transforms_config, 'target_table')
            if not conf:
                raise ValueError(f"No transformation configuration found for table: {args.table}")
            run_transformation(spark, conf, sources_config, targets_config)

        elif args.target_dq:
            if not args.table:
                raise ValueError("--table argument is required for --target-dq.")
            conf = get_config_for_item(args.table, targets_config)
            if not conf:
                raise ValueError(f"No target configuration found for table: {args.table}")
            run_target_dq(spark, conf)

        else:
            logger.info("No action specified. Use --help for options.")

    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}", exc_info=True)
        notify("FAILURE", "Pipeline Execution Failed", str(e))
        sys.exit(1)
    finally:
        spark.stop()
        logger.info("Spark session stopped.")

if __name__ == "__main__":
    main()
