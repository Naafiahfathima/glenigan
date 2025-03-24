import logging.config
import os
from datetime import datetime

# Create logs directory if it doesn't exist
log_dir = 'document_download/online_doc_download_logs'
os.makedirs(log_dir, exist_ok=True)

# Use a unique log filename with a timestamp
log_filename = os.path.join(
    log_dir, datetime.now().strftime('%Y-%m-%d_%H-%M-%S') + '.log'
)


def get_logging_config():
    return {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'detailed': {
                'format': (
                    '%(asctime)s - %(levelname)s - %(name)s - %(filename)s - '
                    '%(funcName)s - Line %(lineno)d - %(message)s'
                ),
            },
        },
        'handlers': {
            'file': {
                'level': 'DEBUG',  # Capture all logs to file
                'class': 'logging.FileHandler',
                'filename': log_filename,
                'formatter': 'detailed',
            },
            'console': {
                'level': 'INFO',  # Show INFO and above on console
                'class': 'logging.StreamHandler',
                'formatter': 'detailed',
            },
        },
        'loggers': {
            '': {  # Root logger captures all messages
                'handlers': ['file', 'console'],
                'level': 'DEBUG',
                'propagate': True,
            },
            'logger': {  # Custom logger if needed
                'handlers': ['file', 'console'],
                'level': 'DEBUG',
                'propagate': False,
            },
        },
    }


# Apply the logging configuration
logging.config.dictConfig(get_logging_config())

# Create a logger instance for use in your application
logger = logging.getLogger('logger')
