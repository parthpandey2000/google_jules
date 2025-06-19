import logging

logger = logging.getLogger(__name__)

def send_notification(subject: str, message: str, success: bool = True, config: dict = None):
    """
    Sends a notification. Currently logs the notification.
    Can be extended to use other methods like email, Slack, etc.

    Args:
        subject (str): The subject of the notification.
        message (str): The main content of the notification.
        success (bool, optional): True if the notification indicates success, False for failure.
                                  Defaults to True. This affects log level.
        config (dict, optional): Configuration for notification channels (e.g., email server,
                                 API keys). Not used in the basic logging implementation.
                                 Example structure for future:
                                 {
                                     "method": "email", # or "slack", "webhook"
                                     "recipients": ["admin@example.com"],
                                     "smtp_server": "smtp.example.com",
                                     # ... other channel-specific configs
                                 }
    """
    log_level = logging.INFO if success else logging.ERROR
    status_prefix = "SUCCESS" if success else "FAILURE"

    full_subject = f"Notification [{status_prefix}]: {subject}"

    logger.log(log_level, f"{full_subject} - Message: {message}")

    # Placeholder for future extensions
    if config:
        method = config.get("method")
        if method == "email":
            logger.info(f"Email notification requested (not implemented): Subject: {subject}, To: {config.get('recipients')}")
            # Example: _send_email(subject, message, config)
        elif method == "slack":
            logger.info(f"Slack notification requested (not implemented): Subject: {subject}, Channel: {config.get('channel')}")
            # Example: _send_to_slack(subject, message, config)
        # Add other methods as needed
    else:
        logger.debug("No extended notification config provided; using logger only.")

if __name__ == '__main__':
    # Configure basic logging for testing this module
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    logger.info("--- Testing Notifications ---")

    send_notification(
        subject="Test Pipeline Success",
        message="The test pipeline completed all stages successfully.",
        success=True
    )

    send_notification(
        subject="Test Pipeline Failure",
        message="The test pipeline failed at the transformation stage. See logs for details.",
        success=False
    )

    send_notification(
        subject="User Signup Alert",
        message="A new VIP user 'test_user' has signed up.",
        success=True,
        config={"method": "email", "recipients": ["test@example.com"]} # Example config
    )

    send_notification(
        subject="Critical System Error",
        message="The main data processing job encountered a critical error.",
        success=False,
        config={"method": "slack", "channel": "#alerts"} # Example config
    )
    logger.info("--- Notification Tests Complete ---")
