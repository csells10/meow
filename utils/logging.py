import logging
import json
import sys

class JsonFormatter(logging.Formatter):
    def format(self, record):
        """
        Format log messages in JSON format for structured logging.
        """
        log_record = {
            'level': record.levelname,
            'message': record.getMessage(),
            'timestamp': self.formatTime(record),
            'module': record.module,
            'line': record.lineno,
        }
        if record.exc_info:
            log_record['exception'] = self.formatException(record.exc_info)
        return json.dumps(log_record)

def setup_logging():
    """
    Sets up logging with a JSON formatter for structured logs.
    """
    formatter = JsonFormatter()
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    logging.basicConfig(level=logging.INFO, handlers=[handler])
