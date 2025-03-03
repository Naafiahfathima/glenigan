import logging.config
import os
from datetime import datetime
import scrapy.utils.log  # <-- Important to override Scrapy's logging behavior

# Create logs directory if it doesn't exist
log_dir = 'logs'
os.makedirs(log_dir, exist_ok=True)

# Use a fixed log filename to capture everything
log_filename = os.path.join(log_dir, datetime.now().strftime('%Y-%m-%d_%H-%M-%S') + '.log')

# Define logging configuration
def get_logging_config():
    return {
        'version': 1,
        'disable_existing_loggers': False,  # Ensure Scrapy logs are not blocked
        'formatters': {
            'detailed': {
                'format': '%(asctime)s - %(levelname)s - %(name)s - %(filename)s - %(funcName)s - Line %(lineno)d - %(message)s',
            },
            'scrapy': {
                'format': '%(asctime)s - %(levelname)s - %(name)s - %(message)s',
            },
        },
        'handlers': {
            'file': {
                'level': 'DEBUG',  # Capture everything
                'class': 'logging.FileHandler',
                'filename': log_filename,
                'formatter': 'detailed',
            },
            'console': {
                'level': 'INFO',
                'class': 'logging.StreamHandler',
                'formatter': 'detailed',
            },
            'scrapy_log': {
                'level': 'DEBUG',
                'class': 'logging.FileHandler',
                'filename': log_filename,
                'formatter': 'scrapy',
            },
        },
        'loggers': {
            '': {  # Root logger (captures everything)
                'handlers': ['file', 'console'],
                'level': 'DEBUG',
                'propagate': True,
            },
            'logger': {  # Custom logger
                'handlers': ['file', 'console'],
                'level': 'DEBUG',
                'propagate': True,
            },
            # Capture all Scrapy logs
            'scrapy': {
                'handlers': ['scrapy_log', 'file', 'console'],
                'level': 'DEBUG',
                'propagate': True,
            },
            # Capture Scrapy retry and HTTP errors
            'scrapy.downloadermiddlewares.retry': {
                'handlers': ['scrapy_log', 'file'],
                'level': 'WARNING',
                'propagate': True,
            },
            'scrapy.spidermiddlewares.httperror': {
                'handlers': ['scrapy_log', 'file'],
                'level': 'INFO',
                'propagate': True,
            },
            'scrapy.extensions.logstats': {
                'handlers': ['scrapy_log', 'file'],
                'level': 'INFO',
                'propagate': True,
            },
            'scrapy.core.engine': {
                'handlers': ['scrapy_log', 'file'],
                'level': 'DEBUG',
                'propagate': True,
            },
        },
    }

# Apply logging configuration
logging.config.dictConfig(get_logging_config())

# Create logger instance
logger = logging.getLogger('logger')

# Override Scrapy’s logging to ensure it uses our settings
scrapy.utils.log.configure_logging(get_logging_config())

