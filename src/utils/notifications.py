from .logger import get_logger

logger = get_logger(__name__)

def send_success_notification(pipeline_name: str, message: str, details: dict = None):
    """
    Sends a success notification.
    Currently, it logs the message. Can be extended to send emails, Slack messages, etc.

    Args:
        pipeline_name (str): Name of the pipeline that succeeded.
        message (str): A summary success message.
        details (dict, optional): Additional details about the success. Defaults to None.
    """
    log_message = f"SUCCESS: Pipeline '{pipeline_name}' - {message}"
    if details:
        log_message += f" | Details: {details}"
    logger.info(log_message)
    # Placeholder for actual notification logic (e.g., email, Slack)
    # print(f"NOTIFICATION (Success): {log_message}") # For simple stdout visibility

def send_failure_notification(pipeline_name: str, message: str, error_details: str = None, details: dict = None):
    """
    Sends a failure notification.
    Currently, it logs the message. Can be extended to send emails, Slack messages, etc.

    Args:
        pipeline_name (str): Name of the pipeline that failed.
        message (str): A summary failure message.
        error_details (str, optional): Specific error information (e.g., exception traceback).
        details (dict, optional): Additional details about the failure. Defaults to None.
    """
    log_message = f"FAILURE: Pipeline '{pipeline_name}' - {message}"
    if error_details:
        # Log the full error details with traceback via logger.error
        logger.error(f"FAILURE: Pipeline '{pipeline_name}' - {message}. Error: {error_details}", exc_info=False) # Set exc_info based on whether error_details contains it
    else:
        logger.error(log_message)

    if details:
        logger.error(f"Additional Failure Details: {details}")

    # Placeholder for actual notification logic (e.g., email, Slack)
    # print(f"NOTIFICATION (Failure): {log_message}") # For simple stdout visibility

if __name__ == "__main__":
    # Example Usage
    logger.info("Testing notification system...")

    send_success_notification(
        pipeline_name="Test Data Pipeline",
        message="Data ingestion completed successfully.",
        details={"source_table": "raw_sales", "records_processed": 10000}
    )

    send_failure_notification(
        pipeline_name="Test Data Pipeline",
        message="Transformation step failed.",
        error_details="ValueError: Invalid data type found in column 'amount'",
        details={"transformation_name": "normalize_sales_data", "input_rows": 500}
    )

    send_failure_notification(
        pipeline_name="Another Pipeline",
        message="Failed to connect to source database."
    )
    logger.info("Notification system test complete.")
