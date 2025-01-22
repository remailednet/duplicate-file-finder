import unittest
import os
import tempfile
import sqlite3
from duplicate_file_finder.database import create_database
from duplicate_file_finder.scanner import add_mount_points
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

class TestList(unittest.TestCase):
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
        # Create files with known content and structure
        content1 = b"Test content 1"
        content2 = b"Test content 2"

        # Create directory structure
        os.makedirs(os.path.join(self.mount_points[0], 'subdir'))
        os.makedirs(os.path.join(self.mount_points[1], 'subdir'))

        # Create files in mount1
        with open(os.path.join(self.mount_points[0], 'file1.txt'), 'wb') as f:
            f.write(content1)
        with open(os.path.join(self.mount_points[0], 'subdir', 'file2.txt'), 'wb') as f:
            f.write(content2)

        # Create files in mount2
        with open(os.path.join(self.mount_points[1], 'file3.txt'), 'wb') as f:
            f.write(content1)

    def test_list_all_files(self):
        """Test listing all files in the database"""
        files = list_files(self.conn)
        self.assertEqual(len(files), 3, "Should list all three files")

        # Verify file paths are present - use normalized paths
        file_keys = set(os.path.normpath(file[1]) for file in files)  # file_key is second element
        expected_keys = {
            'file1.txt',
            os.path.join('subdir', 'file2.txt'),
            'file3.txt'
        }
        self.assertEqual(file_keys, expected_keys, "File keys should match expected paths")

    def test_list_mount_point_filter(self):
        """Test listing files filtered by mount point"""
        files = list_files(self.conn, self.mount_points[0])
        self.assertEqual(len(files), 2, "Should list two files from mount1")

        # All files should be from mount1
        for mount_point, _, _, _ in files:
            self.assertEqual(mount_point, self.mount_points[0])

    def test_list_empty_database(self):
        """Test listing files with empty database"""
        # Create new empty database
        empty_db_path = os.path.join(self.test_dir, 'empty.db')
        empty_conn = create_database(empty_db_path)

        files = list_files(empty_conn)
        self.assertEqual(len(files), 0, "Should return empty list for empty database")

        empty_conn.close()
        os.remove(empty_db_path)

    def test_list_metadata(self):
        """Test that listed files include correct metadata"""
        files = list_files(self.conn)
        for mount_point, file_key, size, modified in files:
            full_path = os.path.join(mount_point, file_key)
            self.assertTrue(os.path.exists(full_path), f"File should exist: {full_path}")
            self.assertEqual(size, os.path.getsize(full_path))
            self.assertEqual(modified, os.path.getmtime(full_path))

    def test_list_nonexistent_mount_point(self):
        """Test listing files with non-existent mount point"""
        files = list_files(self.conn, '/nonexistent/path')
        self.assertEqual(len(files), 0, "Should return empty list for non-existent mount point")

if __name__ == '__main__':
    unittest.main()
