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
            'service': os.getenv('K_SERVICE', 'local'),  # Cloud Run service name
            'revision': os.getenv('K_REVISION', 'local') # Cloud Run revision name
        }
        if record.exc_info:
            log_record['exception'] = self.formatException(record.exc_info)
        return json.dumps(log_record)

def setup_logging():
    formatter = JsonFormatter()

    # StreamHandler for stdout
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)

    handlers = [stream_handler]

    # Conditionally add FileHandler for local development
    if os.getenv('LOG_TO_FILE', 'false') == 'true':
        log_file = os.getenv('LOG_FILE_PATH', 'app.log')
        file_handler = RotatingFileHandler(log_file, maxBytes=1024*1024*5, backupCount=5)
        file_handler.setFormatter(formatter)
        handlers.append(file_handler)

    log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
    logging.basicConfig(level=getattr(logging, log_level), handlers=handlers)

