import logging
import functools
import asyncio
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime
import os
from os.path import abspath, dirname
import inspect
import platform

# from opentelemetry import trace
# from opentelemetry._logs import set_logger_provider
# from opentelemetry.sdk._logs import (
#     LoggerProvider,
#     LoggingHandler
# )
# from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
# from opentelemetry.sdk.trace import TracerProvider
# from azure.monitor.opentelemetry.exporter import AzureMonitorLogExporter
import threading
import json
from os.path import dirname, abspath
import sys

# PARENT_DIR_PATH = dirname(dirname(dirname(abspath("__file__"))))
# #logger.info(f"PARENT_DIR_PATH: {PARENT_DIR_PATH}")
# sys.path.append(PARENT_DIR_PATH)
# PARENT_DIR_PATH = dirname(dirname(dirname(abspath("__file__"))))
CURRENT_DIR = dirname(abspath(__file__))
PARENT_DIR = dirname(CURRENT_DIR)
#logger.info(f"PARENT_DIR_PATH: {PARENT_DIR_PATH}")
sys.path.append(PARENT_DIR)

# import platform

# if platform.system() == "Windows":
#     from key_valuts_service import KeyVaultService
# else:
#     from key_valuts_service import KeyVaultService

# Keyvault_service = KeyVaultService()



# APP_INSIGHTS_CONNECTION_STRING = Keyvault_service.get_keyvault_secret_by_name('APP-INSIGHTS-CONNECTION-STRING')

# Thread-local storage so item_id flows through all files automatically ──
_log_context = threading.local()
 
def set_log_context(item_id: str):
    """Call once in on_message after item_id is parsed. Works across all files automatically."""
    _log_context.item_id = item_id
 
def clear_log_context():
    """Call at the end of on_message to reset for the next message."""
    _log_context.item_id = ""

### Define a custom JSON log formatter
class JsonFormatter(logging.Formatter):
    def format(self, record):
        def safe(val):
            try:
                json.dumps(val)
                return val
            except Exception:
                return str(val)

        log_entry = {
            "timestamp": self.formatTime(record, "%Y-%m-%dT%H:%M:%SZ"),
            "level": record.levelname,
            "item_id": getattr(record, "item_id", None) or getattr(_log_context, "item_id", ""),
            "message": record.getMessage(),
            "user_id": getattr(record, "user_id", ""),
            "conversation_session_id": getattr(record, "conversation_session_id", ""),
            "platform": getattr(record, "platform", ""),
            "language": getattr(record, "language", ""),
            "request_payload": safe(getattr(record, "request_payload", {})),
            "response_payload": safe(getattr(record, "response_payload", {})),
            "function_name": getattr(record, "function_name", ""),
            "input_data": safe(getattr(record, "input_data", "")),
            "error_details": safe(getattr(record, "error_details", "")),
            "status": getattr(record, "status", "Success"),
            "latency": getattr(record, "latency", ""),
            "environment": getattr(record, "environment", os.environ.get("ENV_NAME")),
            "logger_name": f"[Streaming Pipeline]"
        }

        return json.dumps(log_entry, ensure_ascii=False)

def setup_local_logger():
     ### When running from app.py
    FOLDER_PATH = dirname(os.path.abspath('__file__'))
    LOGS_FOLDER_PATH = os.path.join(FOLDER_PATH, "logs_data")

    """ commented code in """
    ### Get the current date and time for the log filename
    log_filename = datetime.now().strftime("mcit-logs-%Y_%m_%d %H_%M_%S.log")
    ### Create a TimedRotatingFileHandler to rotate the log file daily
    handler = TimedRotatingFileHandler(
        os.path.join(LOGS_FOLDER_PATH, log_filename),
        when="midnight",
        interval=1,
        backupCount=7,  # Keep the last 7 days of logs, you can adjust this value
        encoding='utf-8',
    )
    """ commented code out """
    
    ### Define the log formatting
    formatter = JsonFormatter()##logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    """ commented code in """
    handler.setFormatter(formatter)
    """ commented code out """

    # Create a logger object and attach the handler to it
    logger = logging.getLogger("mcit_logger")
    logger.setLevel(logging.INFO)

    """ commented code in """
    logger.addHandler(handler)
    """ commented code out """

    return logger

# def setup_app_insights_logger():
#     try:
#         ### Retrieve instrumentation key
#         connection_string = f"{APP_INSIGHTS_CONNECTION_STRING}"

#         # Create logger
#         logger_provider = LoggerProvider() ### logging.getLogger(__name__)
#         set_logger_provider(logger_provider)

#         exporter = AzureMonitorLogExporter(connection_string=connection_string)
#         logger_provider.add_log_record_processor(BatchLogRecordProcessor(exporter))

#         handler = LoggingHandler()
#         logger = logging.getLogger(__name__)

#         formatter = JsonFormatter()  # Use your custom formatter directly
#         # handler.setFormatter(logging.Formatter('[AI Chatbot] %(asctime)s - %(name)s - %(levelname)s - %(message)s'))
#         handler.setFormatter(formatter)
        
#         logger.addHandler(handler)
#         logger.setLevel(logging.DEBUG)
#         return logger

#     except Exception as e:
#         return None

# Initialize the logger

if platform.system() == "Windows":
    logger = setup_local_logger()
else:
    # logger = setup_app_insights_logger()
    pass
    
if logger is None:
    exit(1)

def log_function(func):
    @functools.wraps(func)
    async def async_wrapper(*args, **kwargs):
        logger.debug(f"Called async function: {func.__name__} with args: {args} and kwargs: {kwargs}")
        start_time = datetime.now()
        result = await func(*args, **kwargs)
        end_time = datetime.now()
        logger.debug(f"Function {func.__name__} returned {result}")
        time_taken_seconds = (end_time - start_time).total_seconds()
        logger.debug(f"Time taken to execute async function '{func.__name__}': {time_taken_seconds} seconds")
        logger.info(
            f"Latency: {func.__name__} completed in {time_taken_seconds:.3f}s",
            extra={
                "function_name": func.__name__,
                "latency_seconds": round(time_taken_seconds, 3),
                "latency": f"{time_taken_seconds:.3f}s", 
                "metric_type": "latency"
            }
        )
        return result

    @functools.wraps(func)
    def sync_wrapper(*args, **kwargs):
        logger.debug(f"Called function: {func.__name__} with args: {args} and kwargs: {kwargs}")
        start_time = datetime.now()
        result = func(*args, **kwargs)
        end_time = datetime.now()
        logger.debug(f"Function {func.__name__} returned {result}")
        time_taken_seconds = (end_time - start_time).total_seconds()
        logger.debug(f"Time taken to execute function '{func.__name__}': {time_taken_seconds} seconds")
        logger.info(
            f"Latency: {func.__name__} completed in {time_taken_seconds:.3f}s",
            extra={
                "function_name": func.__name__,
                "latency_seconds": round(time_taken_seconds, 3),
                "latency": f"{time_taken_seconds:.3f}s", 
                "metric_type": "latency"
            }
        )
        return result

    # Decide which wrapper to return based on whether the original function is async
    if inspect.iscoroutinefunction(func):
        return async_wrapper
    else:
        return sync_wrapper

# Example usage of the decorator with an async function
@log_function
async def example_function(a, b):
    await asyncio.sleep(1)  # Simulate some async operation
    return a + b

# Example of calling the async function
async def main():
    result = await example_function(3, 4)

# Run the main event loop
if __name__ == "__main__":
    # asyncio.run(main())
    pass
