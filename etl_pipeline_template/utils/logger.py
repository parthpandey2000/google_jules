import logging
import sys

def get_logger(name: str, level: str = 'INFO', log_format: str = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'):
    """
    Initializes and returns a logger instance.

    Args:
        name (str): The name of the logger (typically __name__ of the calling module).
        level (str): Logging level (e.g., 'INFO', 'DEBUG').
        log_format (str): The format string for log messages.

    Returns:
        logging.Logger: Configured logger instance.
    """
    # Get the numeric value of the log level
    numeric_level = getattr(logging, level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {level}")

    # Create a logger
    logger = logging.getLogger(name)
    logger.setLevel(numeric_level)

    # Create a handler (console handler by default)
    # In Databricks, print statements and logging output to stdout/stderr are captured.
    # So, a basic stream handler is usually sufficient.
    handler = logging.StreamHandler(sys.stdout) # Use sys.stdout for Databricks compatibility
    handler.setLevel(numeric_level)

    # Create a formatter and set it for the handler
    formatter = logging.Formatter(log_format)
    handler.setFormatter(formatter)

    # Add the handler to the logger
    # Check if handlers are already added to avoid duplicate logs if function is called multiple times
    if not logger.handlers:
        logger.addHandler(handler)

    return logger

if __name__ == '__main__':
    # Example usage:
    # This part is for testing the logger independently.
    # In the main application, you'd typically get the config from the YAML.

    # Test with default settings
    logger_default = get_logger(__name__)
    logger_default.info("This is an INFO message from default logger.")
    logger_default.debug("This is a DEBUG message from default logger (should not appear with INFO level).")

    # Test with DEBUG level
    logger_debug = get_logger("my_debug_logger", level="DEBUG")
    logger_debug.debug("This is a DEBUG message from debug_logger.")
    logger_debug.info("This is an INFO message from debug_logger.")
    logger_debug.warning("This is a WARNING message from debug_logger.")
    logger_debug.error("This is an ERROR message from debug_logger.")
    logger_debug.critical("This is a CRITICAL message from debug_logger.")

    # Test with a different format
    custom_format = "%(levelname)s: %(name)s: %(message)s"
    logger_custom_format = get_logger("custom_format_logger", level="INFO", log_format=custom_format)
    logger_custom_format.info("Info message with custom format.")

    # Test getting the same logger again (should not add handler again)
    logger_default_again = get_logger(__name__)
    logger_default_again.info("Another INFO message from default logger (no duplicate handlers).")

    print("Logger tests complete. Check console output.")
