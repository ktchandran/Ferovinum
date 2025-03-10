import logging
from datetime import datetime
import os


cdr = os.getcwd()

# Generate a timestamp for the log file name
timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
log_filename = os.path.join(cdr, f'Ferovinum_{timestamp}.log')

# Create and configure logger
logging.basicConfig(filename=log_filename,
                    format='%(asctime)s %(message)s',
                    filemode='w')

logger = logging.getLogger()

logger.setLevel(logging.INFO)
