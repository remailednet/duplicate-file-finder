import unittest
import os
import tempfile
import sqlite3
from duplicate_file_finder.dff import (
    create_database,
    add_mount_points,
    remove_mount_point,
    list_files
)

class TestRemove(unittest.TestCase):
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

        # Create test files
        self.create_test_files()

        # Setup database with test data
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

    def create_test_files(self):
        # Create test files
        content1 = b"Test content 1"
        content2 = b"Test content 2"

        # Create files in mount1
        with open(os.path.join(self.mount_points[0], 'file1.txt'), 'wb') as f:
            f.write(content1)
        with open(os.path.join(self.mount_points[0], 'file2.txt'), 'wb') as f:
            f.write(content2)

        # Create files in mount2
        with open(os.path.join(self.mount_points[1], 'file3.txt'), 'wb') as f:
            f.write(content1)
        with open(os.path.join(self.mount_points[1], 'file4.txt'), 'wb') as f:
            f.write(content2)

    def test_remove_mount_point(self):
        """Test removing a single mount point"""
        # Verify initial state
        initial_files = list_files(self.conn)
        self.assertEqual(len(initial_files), 4, "Should have four files initially")

        # Remove mount1
        remove_mount_point(self.conn, self.mount_points[0])

        # Verify files from mount1 are removed
        remaining_files = list_files(self.conn)
        self.assertEqual(len(remaining_files), 2, "Should have two files remaining")

        # Verify remaining files are from mount2
        for mount_point, _, _, _ in remaining_files:
            self.assertEqual(mount_point, self.mount_points[1])

    def test_remove_nonexistent_mount_point(self):
        """Test removing a non-existent mount point"""
        # Get initial file count
        initial_files = list_files(self.conn)
        initial_count = len(initial_files)

        # Attempt to remove non-existent mount point
        nonexistent_path = os.path.join(self.test_dir, 'nonexistent')
        remove_mount_point(self.conn, nonexistent_path)

        # Verify no files were removed
        remaining_files = list_files(self.conn)
        self.assertEqual(len(remaining_files), initial_count,
                        "File count should remain unchanged")

    def test_remove_all_mount_points(self):
        """Test removing all mount points"""
        # Remove all mount points
        for mount_point in self.mount_points:
            remove_mount_point(self.conn, mount_point)

        # Verify database is empty
        remaining_files = list_files(self.conn)
        self.assertEqual(len(remaining_files), 0,
                        "Database should be empty after removing all mount points")

    def test_remove_mount_point_idempotent(self):
        """Test removing the same mount point multiple times"""
        # Remove mount1 twice
        remove_mount_point(self.conn, self.mount_points[0])
        remove_mount_point(self.conn, self.mount_points[0])

        # Verify only mount1 files were removed
        remaining_files = list_files(self.conn)
        self.assertEqual(len(remaining_files), 2,
                        "Should have two files remaining from mount2")
        for mount_point, _, _, _ in remaining_files:
            self.assertEqual(mount_point, self.mount_points[1])

if __name__ == '__main__':
    unittest.main()
