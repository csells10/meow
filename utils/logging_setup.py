# logging.py
import logging
import json
import sys
import os
from logging.handlers import RotatingFileHandler
class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            'level': record.levelname,
            'message': record.getMessage(),
            'timestamp': self.formatTime(record),
            'module': record.module,
            'line': record.lineno,
            'service': os.getenv('K_SERVICE', 'local'),
            'revision': os.getenv('K_REVISION', 'local')
        }
        if record.exc_info:
            log_record['exception'] = self.formatException(record.exc_info)
        return json.dumps(log_record)

def setup_logging():
    if logging.getLogger().hasHandlers():
        return  # Prevent double logging when re-imported

    formatter = JsonFormatter()
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)

    handlers = [stream_handler]

    if os.getenv('LOG_TO_FILE', 'false').lower() == 'true':
        log_file = os.getenv('LOG_FILE_PATH', 'app.log')
        file_handler = RotatingFileHandler(log_file, maxBytes=5 * 1024 * 1024, backupCount=5)
        file_handler.setFormatter(formatter)
        handlers.append(file_handler)

    log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
    logging.basicConfig(level=getattr(logging, log_level), handlers=handlers)

def log_event(level, event, **kwargs):
    """
    Structured log helper.
    :param level: logging level as a string (e.g., 'info', 'error')
    :param event: event name (string)
    :param kwargs: any additional structured fields
    """
    logger = logging.getLogger(__name__)
    log_func = getattr(logger, level, logger.info)
    log_func({"event": event, **kwargs})

