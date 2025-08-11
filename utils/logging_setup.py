# utils/logging_setup.py
import logging, json, sys, os
from logging.handlers import RotatingFileHandler

# Keys the logging system uses internally — we won't duplicate these
_RESERVED = {
    'name','msg','args','levelname','levelno','pathname','filename','module',
    'exc_info','exc_text','stack_info','lineno','funcName','created','msecs',
    'relativeCreated','thread','threadName','processName','process'
}

class JsonFormatter(logging.Formatter):
    def format(self, record):
        # Base payload (will be overwritten by caller info after stacklevel is applied)
        log_record = {
            "level": record.levelname,
            "timestamp": self.formatTime(record),
            "module": record.module,
            "line": record.lineno,
            "service": os.getenv("K_SERVICE", "local"),
            "revision": os.getenv("K_REVISION", "local"),
        }

        # Always include event and message if present
        if hasattr(record, "event"):
            log_record["event"] = record.event

        # Prefer explicit 'message' attr if set by log_event, else the normal message
        log_record["message"] = getattr(record, "message", record.getMessage())

        # ✅ Add ALL extra fields passed via log_event(...) that aren't reserved
        for k, v in record.__dict__.items():
            if k not in log_record and k not in _RESERVED:
                log_record[k] = v

        if record.exc_info:
            log_record["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_record)

def setup_logging():
    root = logging.getLogger()
    if root.hasHandlers():
        return

    formatter = JsonFormatter()
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)

    handlers = [stream_handler]

    if os.getenv('LOG_TO_FILE', 'false').lower() == 'true':
        file_handler = RotatingFileHandler(os.getenv('LOG_FILE_PATH', 'app.log'),
                                           maxBytes=5*1024*1024, backupCount=5)
        file_handler.setFormatter(formatter)
        handlers.append(file_handler)

    level = os.getenv('LOG_LEVEL', 'INFO').upper()
    logging.basicConfig(level=getattr(logging, level, logging.INFO), handlers=handlers)

def log_event(level: str, event: str, **kwargs):
    """
    Structured log helper. Example:
      log_event("info", "yesterday_games_inserted", game_date="20250810", inserted=12)
    """
    logger = logging.getLogger("nfl-app")  # stable logger name
    log_func = getattr(logger, level.lower(), logger.info)

    # Put a human string into 'message' if you want something readable in log explorers
    msg = kwargs.pop("message", "")

    # ✅ stacklevel=2 makes the logger report the *caller* (module + line), not this helper
    log_func(msg, extra={"event": event, **kwargs}, stacklevel=2)
