"""Entry point wrapper for imagetrove uploader"""


# Importing imgtr module from imgtr package
from imgtr import imgtr
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger(__name__)

if __name__ == '__main__':
    imgtr.main(None)
