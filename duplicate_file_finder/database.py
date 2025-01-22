import os
import shutil
import sqlite3
from .config import logger

CURRENT_DB_VERSION = 1

def get_db_version(conn):
    c = conn.cursor()
    c.execute("PRAGMA user_version")
    return c.fetchone()[0]

def set_db_version(conn, version):
    c = conn.cursor()
    c.execute(f"PRAGMA user_version = {version}")

def create_database(db_name):
    """Create and initialize the database."""
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS files
                 (file_key TEXT, full_path TEXT, mount_point TEXT, file_size INTEGER,
                 last_modified REAL, UNIQUE(file_key, full_path))''')
    c.execute("CREATE INDEX IF NOT EXISTS idx_file_key ON files (file_key)")

    db_version = get_db_version(conn)
    if db_version < CURRENT_DB_VERSION:
        set_db_version(conn, CURRENT_DB_VERSION)

    conn.commit()
    return conn

def backup_database(db_path):
    """Create a backup of the database."""
    if os.path.exists(db_path):
        backup_path = f"{db_path}.backup"
        shutil.copy2(db_path, backup_path)
        logger.info(f"Database backed up to {backup_path}")
    else:
        logger.info(f"No existing database found at {db_path}. Skipping backup.")
