"""
Configuration utilities for loading YAML files.
"""
import yaml
import os
import logging

logger = logging.getLogger(__name__)

CONFIG_BASE_PATH = "config" # Relative to project root

def _load_yaml(file_path: str) -> dict:
    """
    Loads a YAML file.

    Args:
        file_path (str): The full path to the YAML file.

    Returns:
        dict: The content of the YAML file as a dictionary.

    Raises:
        FileNotFoundError: If the YAML file does not exist.
        yaml.YAMLError: If there is an error parsing the YAML file.
    """
    if not os.path.exists(file_path):
        logger.error(f"Configuration file not found: {file_path}")
        raise FileNotFoundError(f"Configuration file not found: {file_path}")

    try:
        with open(file_path, 'r') as f:
            config = yaml.safe_load(f)
        logger.info(f"Successfully loaded configuration from {file_path}")
        return config if config else {}
    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML file {file_path}: {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred while loading {file_path}: {e}", exc_info=True)
        raise

def load_pipeline_config(pipeline_name: str) -> dict:
    """
    Loads a specific pipeline's configuration file.
    Example: pipeline_name='sample_pipeline' -> loads 'config/pipeline/sample_pipeline.yaml'
    """
    file_path = os.path.join(CONFIG_BASE_PATH, "pipeline", f"{pipeline_name}.yaml")
    return _load_yaml(file_path)

def load_source_config(source_name: str) -> dict:
    """
    Loads a specific data source's configuration file.
    Example: source_name='my_source' -> loads 'config/sources/my_source.yaml'
    """
    file_path = os.path.join(CONFIG_BASE_PATH, "sources", f"{source_name}.yaml")
    return _load_yaml(file_path)

def load_target_config(target_name: str) -> dict:
    """
    Loads a specific data target's configuration file.
    Example: target_name='my_target' -> loads 'config/targets/my_target.yaml'
    """
    file_path = os.path.join(CONFIG_BASE_PATH, "targets", f"{target_name}.yaml")
    return _load_yaml(file_path)

def load_transformation_config(transformation_name: str) -> dict:
    """
    Loads a specific transformation's configuration file (if it's YAML-based).
    Example: transformation_name='my_transformation' -> loads 'config/transformations/my_transformation.yaml'
    """
    file_path = os.path.join(CONFIG_BASE_PATH, "transformations", f"{transformation_name}.yaml")
    return _load_yaml(file_path)

def load_dq_rule_config(rule_name: str) -> dict:
    """
    Loads a specific Data Quality (DQ) rule's configuration file.
    Example: rule_name='my_dq_rule' -> loads 'config/dq_rules/my_dq_rule.yaml'
    """
    file_path = os.path.join(CONFIG_BASE_PATH, "dq_rules", f"{rule_name}.yaml")
    return _load_yaml(file_path)

if __name__ == '__main__':
    # Create dummy config files for testing
    # Ensure the config directory structure exists
    os.makedirs(os.path.join(CONFIG_BASE_PATH, "pipeline"), exist_ok=True)
    os.makedirs(os.path.join(CONFIG_BASE_PATH, "sources"), exist_ok=True)

    sample_pipeline_path = os.path.join(CONFIG_BASE_PATH, "pipeline", "test_pipeline.yaml")
    sample_source_path = os.path.join(CONFIG_BASE_PATH, "sources", "test_source.yaml")

    with open(sample_pipeline_path, 'w') as f:
        yaml.dump({"name": "Test Pipeline", "version": "1.0"}, f)

    with open(sample_source_path, 'w') as f:
        yaml.dump({"type": "csv", "path": "/data/input"}, f)

    logging.basicConfig(level=logging.INFO)

    try:
        pipeline_conf = load_pipeline_config("test_pipeline")
        logger.info(f"Loaded test_pipeline config: {pipeline_conf}")

        source_conf = load_source_config("test_source")
        logger.info(f"Loaded test_source config: {source_conf}")

        # Test non-existent file
        try:
            load_pipeline_config("non_existent_pipeline")
        except FileNotFoundError as e:
            logger.info(f"Correctly caught FileNotFoundError: {e}")

    finally:
        # Clean up dummy files
        if os.path.exists(sample_pipeline_path):
            os.remove(sample_pipeline_path)
        if os.path.exists(sample_source_path):
            os.remove(sample_source_path)
        logger.info("Cleaned up test config files.")
