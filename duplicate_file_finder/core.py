import os
from collections import defaultdict
from .utils import calculate_hashes, process_files_in_batches, get_file_hash
from .config import logger

def find_duplicates(conn):
    """Find duplicate files in the database."""
    c = conn.cursor()
    c.execute('''SELECT file_key, GROUP_CONCAT(full_path, '; ') as paths,
                 GROUP_CONCAT(file_size, '; ') as sizes, COUNT(DISTINCT mount_point) as mount_count
                 FROM files
                 GROUP BY file_key
                 HAVING mount_count > 1''')
    return c.fetchall()

def analyze_duplicates(duplicates):
    """Analyze duplicates to find exact and path-based matches."""
    exact_duplicates = []
    path_duplicates = []

    for duplicate in duplicates:
        file_key, paths_str, sizes_str, _ = duplicate
        paths = paths_str.split('; ')
        sizes = [int(size) for size in sizes_str.split('; ')]

        # Calculate hashes for each file
        hashes = defaultdict(list)
        for path, size in zip(paths, sizes):
            file_hash = get_file_hash(path)
            if file_hash is not None:
                hashes[file_hash].append((path, size))

        # If all files have the same hash, they're exact duplicates
        if len(hashes) == 1:
            exact_duplicates.append((file_key, list(hashes.values())[0]))
        # If files have different hashes, they're path duplicates
        elif len(hashes) > 1:
            path_duplicates.append((file_key, dict(hashes)))

    return exact_duplicates, path_duplicates

def generate_delete_commands(exact_duplicates):
    """Generate shell commands to delete duplicate files."""
    commands = []
    for file_key, paths_and_sizes in exact_duplicates:
        keep_path, keep_size = max(paths_and_sizes, key=lambda x: x[1])
        for path, size in paths_and_sizes:
            if path != keep_path:
                commands.append(f"rm '{path}'")
        commands.append(f"# Kept: '{keep_path}' (Size: {keep_size} bytes)")
        commands.append("")
    return commands
