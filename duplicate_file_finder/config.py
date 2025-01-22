import json
import logging

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DEFAULT_CONFIG = {
    "default_db": "file_list.db",
    "hash_algorithm": "md5",
    "ignore_list": [],
    "block_size": 65536,
    "batch_size": 1000
}

def load_config(config_file='config.json'):
    """Load configuration from file or return defaults."""
    try:
        with open(config_file, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return DEFAULT_CONFIG.copy()

config = load_config()
