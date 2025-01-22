import unittest
import os
import tempfile
import time
from duplicate_file_finder.database import create_database
from duplicate_file_finder.scanner import (
    add_mount_points,
    update_mount_point
)
from duplicate_file_finder.cli import list

def list_files(conn, mount_point=None):
    c = conn.cursor()
    if mount_point:
        c.execute('''SELECT mount_point, file_key, file_size, last_modified
                     FROM files WHERE mount_point = ?
                     ORDER BY mount_point, file_key''', (mount_point,))
    else:
        c.execute('''SELECT mount_point, file_key, file_size, last_modified
                     FROM files ORDER BY mount_point, file_key''')
    return c.fetchall()

class TestUpdate(unittest.TestCase):
    def setUp(self):
        # Create temporary test directory and database
        self.test_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.test_dir, 'test.db')

        # Create test mount points
        self.mount_points = [
            os.path.join(self.test_dir, 'mount1'),
            os.path.join(self.test_dir, 'mount2')
        ]
        for mount_point in self.mount_points:
            os.makedirs(mount_point)

        # Create initial test files
        self.create_initial_files()

        # Setup database with initial data
        self.conn = create_database(self.db_path)
        add_mount_points(self.conn, self.mount_points)

    def tearDown(self):
        self.conn.close()
        # Clean up test files and directories
        for mount_point in self.mount_points:
            for root, _, files in os.walk(mount_point, topdown=False):
                for name in files:
                    os.remove(os.path.join(root, name))
                os.rmdir(root)
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        os.rmdir(self.test_dir)

    def create_initial_files(self):
        """Create initial set of test files"""
        content1 = b"Initial content 1"
        content2 = b"Initial content 2"

        # Create files in mount1
        with open(os.path.join(self.mount_points[0], 'existing1.txt'), 'wb') as f:
            f.write(content1)
        with open(os.path.join(self.mount_points[0], 'to_be_modified.txt'), 'wb') as f:
            f.write(content2)
        with open(os.path.join(self.mount_points[0], 'to_be_deleted.txt'), 'wb') as f:
            f.write(content2)

    def test_update_modified_file(self):
        """Test updating a modified file"""
        modified_file = os.path.join(self.mount_points[0], 'to_be_modified.txt')

        # Modify the file
        time.sleep(0.1)  # Ensure modification time is different
        with open(modified_file, 'wb') as f:
            f.write(b"Modified content")

        # Update mount point
        update_mount_point(self.conn, self.mount_points[0])

        # Verify file was updated
        c = self.conn.cursor()
        c.execute('SELECT last_modified FROM files WHERE full_path = ?', (modified_file,))
        db_mtime = c.fetchone()[0]
        actual_mtime = os.path.getmtime(modified_file)
        self.assertEqual(db_mtime, actual_mtime)

    def test_update_new_file(self):
        """Test adding a new file during update"""
        # Add new file
        new_file = os.path.join(self.mount_points[0], 'new_file.txt')
        with open(new_file, 'wb') as f:
            f.write(b"New file content")

        # Update mount point
        update_mount_point(self.conn, self.mount_points[0])

        # Verify new file was added
        c = self.conn.cursor()
        c.execute('SELECT COUNT(*) FROM files WHERE full_path = ?', (new_file,))
        count = c.fetchone()[0]
        self.assertEqual(count, 1)

    def test_update_deleted_file(self):
        """Test handling deleted files during update"""
        # Delete a file
        to_delete = os.path.join(self.mount_points[0], 'to_be_deleted.txt')
        os.remove(to_delete)

        # Update mount point
        update_mount_point(self.conn, self.mount_points[0])

        # Verify file was removed from database
        c = self.conn.cursor()
        c.execute('SELECT COUNT(*) FROM files WHERE full_path = ?', (to_delete,))
        count = c.fetchone()[0]
        self.assertEqual(count, 0)

    def test_update_nonexistent_mount_point(self):
        """Test updating a non-existent mount point"""
        nonexistent_path = os.path.join(self.test_dir, 'nonexistent')

        # Get initial file count
        c = self.conn.cursor()
        c.execute('SELECT COUNT(*) FROM files')
        initial_count = c.fetchone()[0]

        # Attempt to update non-existent mount point
        update_mount_point(self.conn, nonexistent_path)

        # Verify no changes were made
        c.execute('SELECT COUNT(*) FROM files')
        final_count = c.fetchone()[0]
        self.assertEqual(final_count, initial_count)

    def test_update_multiple_changes(self):
        """Test multiple simultaneous changes"""
        # Modify existing file
        modified_file = os.path.join(self.mount_points[0], 'to_be_modified.txt')
        with open(modified_file, 'wb') as f:
            f.write(b"Modified content")

        # Add new file
        new_file = os.path.join(self.mount_points[0], 'new_file.txt')
        with open(new_file, 'wb') as f:
            f.write(b"New file content")

        # Delete existing file
        to_delete = os.path.join(self.mount_points[0], 'to_be_deleted.txt')
        os.remove(to_delete)

        # Update mount point
        update_mount_point(self.conn, self.mount_points[0])

        # Verify all changes
        files = list_files(self.conn, self.mount_points[0])

        # Use basename for comparison since we only care about the filenames
        file_names = [os.path.basename(f[1]) for f in files]

        # Debug output
        print("\nFound files:", file_names)

        self.assertIn('new_file.txt', file_names)
        self.assertIn('to_be_modified.txt', file_names)
        self.assertNotIn('to_be_deleted.txt', file_names)
        self.assertEqual(len(files), 3)  # existing1, modified, and new file

if __name__ == '__main__':
    unittest.main()
