import hashlib
from concurrent.futures import ThreadPoolExecutor
from .config import config, logger

def get_file_hash(file_path, algorithm=config['hash_algorithm'], block_size=config['block_size']):
    """Calculate hash of a file."""
    hasher = hashlib.new(algorithm)
    try:
        with open(file_path, 'rb') as file:
            for block in iter(lambda: file.read(block_size), b''):
                hasher.update(block)
        return hasher.hexdigest()
    except IOError:
        logger.warning(f"Unable to read file {file_path}")
        return None

def calculate_hashes(file_paths):
    """Calculate hashes for multiple files in parallel."""
    with ThreadPoolExecutor() as executor:
        return list(executor.map(get_file_hash, file_paths))

def process_files_in_batches(files, batch_size=config['batch_size']):
    """Process files in batches."""
    for i in range(0, len(files), batch_size):
        yield files[i:i + batch_size]
