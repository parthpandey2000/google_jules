import logging
import logging.config
import yaml
import os

DEFAULT_LOG_LEVEL = "INFO"
DEFAULT_LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

def setup_logging(log_level: str = None, config_path: str = None, default_format: str = None):
    """
    Setup logging configuration.

    Args:
        log_level (str): Minimum log level (e.g., 'INFO', 'DEBUG'). Overrides config file if set.
        config_path (str): Path to a logging configuration file (YAML).
        default_format (str): The default logging format if no config file is provided.
    """
    level = log_level.upper() if log_level else DEFAULT_LOG_LEVEL
    log_format = default_format if default_format else DEFAULT_LOG_FORMAT

    configured_by_file = False

    if config_path and os.path.exists(config_path):
        try:
            with open(config_path, 'rt') as f:
                logging_config = yaml.safe_load(f.read())
            logging.config.dictConfig(logging_config)
            configured_by_file = True
            # If log_level is provided, it overrides the root logger's level from the file
            if log_level:
                logging.getLogger().setLevel(level) # Set root logger level
            logger = logging.getLogger(__name__)
            logger.info(f"Logging configured from file: {config_path}")
            if log_level:
                 logger.info(f"Log level overridden by command line/direct call to: {level}")
        except Exception as e:
            logging.basicConfig(level=level, format=log_format)
            logging.warning(f"Failed to load logging config from {config_path}. Using basicConfig. Error: {e}")
    else:
        logging.basicConfig(level=level, format=log_format)
        logger = logging.getLogger(__name__)
        logger.info("Logging configured with basicConfig.")
        if config_path:
            logger.warning(f"Logging configuration file not found: {config_path}. Using basicConfig.")

    # Set default levels for noisy libraries if not configured by file
    # (If configured by file, the file is expected to handle these)
    default_libraries = {
        "pyspark": "WARNING",
        "py4j": "WARNING",
        # Add other libraries and their desired default levels here
        # "another_library": "ERROR",
    }

    if not configured_by_file:
        for lib_name, lib_level_str in default_libraries.items():
            lib_logger = logging.getLogger(lib_name)
            # Check if the logger's level is already set (e.g. by basicConfig or other means)
            # Only set if current level is NOTSET or higher than desired default
            current_level_val = lib_logger.getEffectiveLevel() # Gets level from parents if not set directly
            desired_level_val = logging.getLevelName(lib_level_str)
            if current_level_val > desired_level_val or current_level_val == logging.NOTSET : # NOTSET is 0
                 lib_logger.setLevel(desired_level_val)

    # Add a flag to root handlers to indicate they were set by this setup function
    # This helps in the __main__ test block to selectively remove handlers
    # if not configured_by_file:
    #      for handler in logging.getLogger().handlers:
    #          handler._set_by_setup_logging = True


if __name__ == '__main__':
    # Create a dummy logging_config.yaml for testing
    dummy_config_content = {
        'version': 1,
        'formatters': {
            'detailed': {
                'format': '%(asctime)s - %(name)s - %(levelname)s - %(module)s - %(lineno)d - %(message)s'
            }
        },
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'formatter': 'detailed',
                'level': 'DEBUG' # Handler level
            }
        },
        'root': { # Root logger configuration
            'handlers': ['console'],
            'level': 'DEBUG' # Root logger level
        },
        'disable_existing_loggers': False
    }
    dummy_config_path = 'logging_config_test.yaml'
    with open(dummy_config_path, 'w') as f:
        yaml.dump(dummy_config_content, f)

    print("--- Test 1: Basic config with default level (INFO) ---")
    # Reset logging state
    for handler in logging.root.handlers[:]: logging.root.removeHandler(handler)
    logging.root.setLevel(logging.NOTSET) # Reset root logger level
    setup_logging() # Default level is INFO
    logging.info("Info message (Test 1)")
    logging.debug("Debug message (Test 1) - should not be visible")
    logging.getLogger("pyspark").info("PySpark info (Test 1) - should not be visible") # pyspark level set to WARNING by default
    logging.getLogger("pyspark").warning("PySpark warning (Test 1) - should be visible")

    print("\n--- Test 2: Basic config with DEBUG level specified ---")
    for handler in logging.root.handlers[:]: logging.root.removeHandler(handler)
    logging.root.setLevel(logging.NOTSET)
    setup_logging(log_level="DEBUG")
    logging.info("Info message (Test 2)")
    logging.debug("Debug message (Test 2) - should be visible")
    logging.getLogger("pyspark").debug("PySpark debug (Test 2) - should not be visible") # pyspark still WARNING
    logging.getLogger("pyspark").warning("PySpark warning (Test 2) - should be visible")


    print("\n--- Test 3: File-based config (reads DEBUG from file) ---")
    for handler in logging.root.handlers[:]: logging.root.removeHandler(handler)
    logging.root.setLevel(logging.NOTSET)
    setup_logging(config_path=dummy_config_path) # File sets DEBUG
    logging.info("Info message (Test 3 file config)")
    logging.debug("Debug message (Test 3 file config) - should be visible")
    logging.getLogger("pyspark").info("PySpark info (Test 3 file config) - should be visible as file doesn't specify pyspark level")
    logging.getLogger("pyspark").warning("PySpark warning (Test 3 file config) - should be visible")


    print("\n--- Test 4: File-based config with log_level override (INFO) ---")
    for handler in logging.root.handlers[:]: logging.root.removeHandler(handler)
    logging.root.setLevel(logging.NOTSET)
    # File config sets DEBUG, but log_level="INFO" should override root logger
    setup_logging(config_path=dummy_config_path, log_level="INFO")
    logging.info("Info message (Test 4 file config + override)")
    logging.debug("Debug message (Test 4 file config + override) - should NOT be visible") # Root is INFO
    # The pyspark logger's level is not explicitly set in the file, so it inherits from root (INFO)
    logging.getLogger("pyspark").debug("PySpark debug (Test 4 file config + override) - should NOT be visible")
    logging.getLogger("pyspark").info("PySpark info (Test 4 file config + override) - should be visible")


    print("\n--- Test 5: Non-existent config file ---")
    for handler in logging.root.handlers[:]: logging.root.removeHandler(handler)
    logging.root.setLevel(logging.NOTSET)
    setup_logging(config_path='non_existent_config.yaml', log_level="DEBUG")
    logging.info("Info message (Test 5 non-existent file)")
    logging.debug("Debug message (Test 5 non-existent file) - should be visible")


    # Clean up dummy config file
    if os.path.exists(dummy_config_path):
        os.remove(dummy_config_path)
