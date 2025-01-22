import os
import json
from tqdm import tqdm
from .config import logger
from .utils import process_files_in_batches

def save_checkpoint(mount_point, last_processed_file):
    """Save scanning checkpoint."""
    with open('checkpoint.json', 'w') as f:
        json.dump({'mount_point': mount_point, 'last_file': last_processed_file}, f)

def load_checkpoint():
    """Load scanning checkpoint."""
    try:
        with open('checkpoint.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return None

def scan_mount_point(mount_point):
    """Scan a mount point for files."""
    checkpoint = load_checkpoint()
    files_info = []
    resume_from = checkpoint['last_file'] if checkpoint and checkpoint['mount_point'] == mount_point else None
    resumed = False

    total_files = sum(len(files) for _, _, files in os.walk(mount_point))
    with tqdm(total=total_files, desc=f"Scanning {mount_point}", unit="file") as pbar:
        for root, _, files in os.walk(mount_point):
            for filename in files:
                full_path = os.path.join(root, filename)
                if resume_from and not resumed:
                    if full_path == resume_from:
                        resumed = True
                    pbar.update(1)
                    continue

                relative_path = os.path.relpath(root, mount_point)
                file_key = os.path.join(relative_path, filename)
                try:
                    file_size = os.path.getsize(full_path)
                    last_modified = os.path.getmtime(full_path)
                    files_info.append((file_key, full_path, mount_point, file_size, last_modified))
                    save_checkpoint(mount_point, full_path)
                except OSError:
                    logger.warning(f"Unable to get info for file {full_path}")
                pbar.update(1)

    return files_info

def add_mount_points(conn, mount_points):
    """Add mount points to the database."""
    c = conn.cursor()

    for mount_point in mount_points:
        if not os.path.exists(mount_point):
            logger.warning(f"Mount point {mount_point} does not exist. Skipping.")
            continue

        files_info = scan_mount_point(mount_point)
        for batch in process_files_in_batches(files_info):
            c.executemany("INSERT OR REPLACE INTO files (file_key, full_path, mount_point, file_size, last_modified) VALUES (?, ?, ?, ?, ?)", batch)
            conn.commit()

def remove_mount_point(conn, mount_point):
    """Remove a mount point from the database."""
    c = conn.cursor()
    mount_point = os.path.abspath(mount_point)
    c.execute("DELETE FROM files WHERE mount_point = ?", (mount_point,))
    conn.commit()

def update_mount_point(conn, mount_point):
    """Update a mount point in the database."""
    c = conn.cursor()
    c.execute("SELECT file_key, full_path, file_size, last_modified FROM files WHERE mount_point = ?", (mount_point,))
    existing_files = {row[1]: row for row in c.fetchall()}

    updated_files = []
    new_files = []

    total_files = sum(len(files) for _, _, files in os.walk(mount_point))
    with tqdm(total=total_files, desc=f"Updating {mount_point}", unit="file") as pbar:
        for root, _, files in os.walk(mount_point):
            for filename in files:
                full_path = os.path.join(root, filename)
                relative_path = os.path.relpath(root, mount_point)
                file_key = os.path.join(relative_path, filename)
                try:
                    file_size = os.path.getsize(full_path)
                    last_modified = os.path.getmtime(full_path)

                    if full_path in existing_files:
                        old_info = existing_files[full_path]
                        if last_modified != old_info[3] or file_size != old_info[2]:
                            updated_files.append((file_key, full_path, mount_point, file_size, last_modified))
                        del existing_files[full_path]
                    else:
                        new_files.append((file_key, full_path, mount_point, file_size, last_modified))
                except OSError:
                    logger.warning(f"Unable to get info for file {full_path}")
                pbar.update(1)

    # Delete files that no longer exist
    c.executemany("DELETE FROM files WHERE full_path = ?", ((path,) for path in existing_files.keys()))

    # Update modified files
    for batch in process_files_in_batches(updated_files):
        c.executemany("UPDATE files SET file_size = ?, last_modified = ? WHERE full_path = ?",
                     ((size, mtime, path) for _, path, _, size, mtime in batch))

    # Insert new files
    for batch in process_files_in_batches(new_files):
        c.executemany("INSERT INTO files (file_key, full_path, mount_point, file_size, last_modified) VALUES (?, ?, ?, ?, ?)", batch)

    conn.commit()
    logger.info(f"Updated {len(updated_files)} files, added {len(new_files)} new files, and removed {len(existing_files)} files for {mount_point}")
