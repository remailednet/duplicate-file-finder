import os
import time
import click
from .config import config, logger
from .database import create_database, backup_database
from .scanner import scan_mount_point, add_mount_points, remove_mount_point, update_mount_point
from .core import find_duplicates, analyze_duplicates, generate_delete_commands

@click.group()
def cli():
    """Duplicate File Finder - Find and manage duplicate files across mount points."""
    pass

@cli.command()
@click.option('--db', default=config['default_db'], help='Database file')
@click.argument('mount_points', nargs=-1, required=True)
def add(db, mount_points):
    """Add mount points to the database."""
    conn = create_database(db)
    backup_database(db)
    add_mount_points(conn, mount_points)
    conn.close()
    logger.info(f"Mount points added to {db}")

@cli.command()
@click.option('--db', default=config['default_db'], help='Database file')
def check(db):
    """Check for duplicates in the database."""
    conn = create_database(db)
    backup_database(db)
    duplicates = find_duplicates(conn)
    conn.close()

    if duplicates:
        exact_duplicates, path_duplicates = analyze_duplicates(duplicates)

        if exact_duplicates:
            logger.info("Exact duplicates found (same path and content). Generated delete commands:")
            commands = generate_delete_commands(exact_duplicates)
            for cmd in commands:
                print(cmd)

            print("\nTo execute these commands, save them to a file and run:")
            print("bash delete_commands.sh")
        else:
            logger.info("No exact duplicates found.")

        if path_duplicates:
            logger.info("Path duplicates found (same path, different content):")
            for file_key, hashes in path_duplicates:
                print(f"\nDuplicate path: {file_key}")
                for file_hash, paths_and_sizes in hashes.items():
                    print(f"  Hash: {file_hash}")
                    for path, size in paths_and_sizes:
                        print(f"    {path} (Size: {size} bytes)")
        else:
            logger.info("No path duplicates found.")
    else:
        logger.info("No duplicate file paths found.")

@cli.command()
@click.option('--db', default=config['default_db'], help='Database file')
@click.argument('mount_point')
def remove(db, mount_point):
    """Remove a mount point from the database."""
    conn = create_database(db)
    backup_database(db)
    remove_mount_point(conn, mount_point)
    conn.close()
    logger.info(f"Mount point {mount_point} removed from {db}")

@cli.command()
@click.option('--db', default=config['default_db'], help='Database file')
@click.argument('mount_point')
def update(db, mount_point):
    """Update a mount point in the database."""
    conn = create_database(db)
    backup_database(db)
    update_mount_point(conn, mount_point)
    conn.close()
    logger.info(f"Mount point {mount_point} updated in {db}")

@cli.command()
@click.option('--db', default=config['default_db'], help='Database file')
@click.option('--mount-point', help='Filter by mount point')
def list(db, mount_point):
    """List all files in the database."""
    conn = create_database(db)

    # Get all files
    c = conn.cursor()
    if mount_point:
        c.execute('''SELECT mount_point, file_key, file_size, last_modified
                     FROM files WHERE mount_point = ?
                     ORDER BY mount_point, file_key''', (mount_point,))
    else:
        c.execute('''SELECT mount_point, file_key, file_size, last_modified
                     FROM files ORDER BY mount_point, file_key''')
    files = c.fetchall()
    conn.close()

    if not files:
        logger.info("No files found in the database.")
        return

    # Group files by mount point for display
    current_mount = None
    for mount, file_key, size, modified in files:
        if current_mount != mount:
            current_mount = mount
            print(f"\nMount point: {mount}")
        print(f"  {file_key} (Size: {size} bytes, Modified: {time.ctime(modified)})")

    total_files = len(files)
    total_size = sum(size for _, _, size, _ in files)
    print(f"\nTotal: {total_files} files, {total_size} bytes")
