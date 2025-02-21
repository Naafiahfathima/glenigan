import logging.config
import os
from datetime import datetime

# Create logs directory if it doesn't exist
log_dir = 'logs'
os.makedirs(log_dir, exist_ok=True)

# Generate log file name based on current date
log_filename = os.path.join(log_dir, datetime.now().strftime('%Y-%m-%d_%H-%M-%S') + '.log')

# Define logging configuration
def get_logging_config():
    return {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'standard': {
                'format': '%(asctime)s - %(levelname)s - %(message)s',
            },
        },
        'handlers': {
            'file': {
                'level': 'INFO',
                'class': 'logging.FileHandler',
                'filename': log_filename,
                'formatter': 'standard',
            },
            'console': {
                'level': 'INFO',
                'class': 'logging.StreamHandler',
                'formatter': 'standard',
            },
        },
        'loggers': {
            'logger': {
                'handlers': ['file', 'console'],
                'level': 'INFO',
                'propagate': False,
            },
        },
    }

# Apply logging configuration
logging.config.dictConfig(get_logging_config())

# Create logger instance
logger = logging.getLogger('logger')
