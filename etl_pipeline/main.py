import argparse
import sys
from collections import deque
from etl_pipeline.src.utils import get_spark_session, load_config, notify
from etl_pipeline.src.logger import get_logger
from etl_pipeline.dq_checks.dq_checker import run_dq_pipeline
from etl_pipeline.src.etl import run_transformation

logger = get_logger(__name__)

def get_config_for_item(item_name: str, configs: list, item_key: str = 'name') -> dict:
    for config in configs:
        if config.get(item_key) == item_name:
            return config
    return None

def get_run_order(transforms_config: list) -> list:
    logger.info("Determining transformation execution order using topological sort.")
    graph = {t['target_table']: [] for t in transforms_config}
    in_degree = {t['target_table']: 0 for t in transforms_config}
    transform_map = {t['target_table']: t for t in transforms_config}
    all_targets = set(transform_map.keys())

    for transform in transforms_config:
        target = transform['target_table']
        for source in transform['source_tables']:
            if source in all_targets:
                graph[source].append(target)
                in_degree[target] += 1

    queue = deque([t for t, deg in in_degree.items() if deg == 0])
    run_order = []
    while queue:
        node = queue.popleft()
        run_order.append(transform_map[node])
        for neighbor in graph[node]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)

    if len(run_order) != len(transforms_config):
        raise Exception("Circular dependency detected in transformations.")

    logger.info(f"Execution order: {[t['target_table'] for t in run_order]}")
    return run_order

def main():
    parser = argparse.ArgumentParser(description="PySpark ETL Pipeline Orchestrator", formatter_class=argparse.RawTextHelpFormatter)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--run-all", action="store_true", help="Run the entire ETL pipeline based on dependency order.")
    group.add_argument("--source-dq", action="store_true", help="Run source data quality checks for a specific table.")
    group.add_argument("--transform", action="store_true", help="Run a single transformation for a specific target table.")
    group.add_argument("--target-dq", action="store_true", help="Run target data quality checks for a specific table.")
    group.add_argument("--queue", type=str, help="Run a specific sequence of transformations.\nProvide a comma-separated list of target table names.\nExample: --queue dim_year,dim_disease,fact_comorbidities")
    parser.add_argument("--table", type=str, help="Specify the source/target table name for single-task modes.")
    args = parser.parse_args()

    try:
        spark = get_spark_session("ETLOrchestrator")
        sources_config = load_config("etl_pipeline/configs/sources.yaml")['sources']
        targets_config = load_config("etl_pipeline/configs/targets.yaml")['targets']
        transforms_config = load_config("etl_pipeline/configs/transformations.yaml")['transformations']
    except Exception as e:
        logger.error(f"Failed during initial setup: {e}")
        notify("FAILURE", "Pipeline Setup Failed", str(e))
        sys.exit(1)

    try:
        if args.run_all:
            logger.info("--- Starting Full Pipeline Run ---")
            for source in sources_config:
                run_dq_pipeline(spark, source, 'source')
            run_order = get_run_order(transforms_config)
            for transform in run_order:
                run_transformation(spark, transform, sources_config, targets_config)
                target_name = transform['target_table']
                target_conf = get_config_for_item(target_name, targets_config)
                if target_conf:
                    run_dq_pipeline(spark, target_conf, 'target')
            notify("SUCCESS", "Full Pipeline Run", "The entire ETL pipeline completed successfully.")

        elif args.queue:
            logger.info(f"--- Starting Queue-Based Transformation Run ---")
            queue_list = [table.strip() for table in args.queue.split(',') if table.strip()]
            if not queue_list:
                logger.warning("Queue is empty. No transformations will be run.")
            else:
                logger.info(f"Execution queue: {queue_list}")
                for table_name in queue_list:
                    transform_conf = get_config_for_item(table_name, transforms_config, 'target_table')
                    if not transform_conf:
                        raise ValueError(f"No transformation configuration found for table: {table_name} in the queue.")
                    run_transformation(spark, transform_conf, sources_config, targets_config)
                    target_conf = get_config_for_item(table_name, targets_config)
                    if target_conf:
                        logger.info(f"--- Running Target DQ for {table_name} ---")
                        run_dq_pipeline(spark, target_conf, 'target')
                notify("SUCCESS", "Queue-Based Run", f"Successfully executed transformations for: {args.queue}")

        elif args.source_dq:
            if not args.table: raise ValueError("--table is required for --source-dq.")
            conf = get_config_for_item(args.table, sources_config)
            if not conf: raise ValueError(f"No source configuration found for table: {args.table}")
            run_dq_pipeline(spark, conf, 'source')

        elif args.transform:
            if not args.table: raise ValueError("--table is required for --transform.")
            conf = get_config_for_item(args.table, transforms_config, 'target_table')
            if not conf: raise ValueError(f"No transformation configuration found for table: {args.table}")
            run_transformation(spark, conf, sources_config, targets_config)

        elif args.target_dq:
            if not args.table: raise ValueError("--table is required for --target-dq.")
            conf = get_config_for_item(args.table, targets_config)
            if not conf: raise ValueError(f"No target configuration found for table: {args.table}")
            run_dq_pipeline(spark, conf, 'target')

    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}", exc_info=True)
        notify("FAILURE", "Pipeline Execution Failed", str(e))
        sys.exit(1)
    finally:
        spark.stop()
        logger.info("Spark session stopped.")

if __name__ == "__main__":
    main()
