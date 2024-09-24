import logging
import json
import sys

class JsonFormatter(logging.Formatter):
    def format(self, record):
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
    formatter = JsonFormatter()

    # StreamHandler for stdout
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)

    # FileHandler for log file
    file_handler = logging.FileHandler('app.log')
    file_handler.setFormatter(formatter)

    logging.basicConfig(level=logging.INFO, handlers=[stream_handler, file_handler])
