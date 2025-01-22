import unittest
import os
import tempfile
import sqlite3
from duplicate_file_finder.database import create_database
from duplicate_file_finder.scanner import scan_mount_point, add_mount_points
from duplicate_file_finder.utils import get_file_hash

class TestAdd(unittest.TestCase):
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

    def tearDown(self):
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
        # Create test files with different content but same relative paths
        content1 = b"Content for first file1.txt"
        content2 = b"Different content for second file1.txt"
        content3 = b"Content for unique file"

        # Create same-named files in different mount points
        os.makedirs(os.path.join(self.mount_points[0], 'subdir'))
        os.makedirs(os.path.join(self.mount_points[1], 'subdir'))

        # file1.txt in both mount points (same relative path, different content)
        with open(os.path.join(self.mount_points[0], 'subdir', 'file1.txt'), 'wb') as f:
            f.write(content1)
        with open(os.path.join(self.mount_points[1], 'subdir', 'file1.txt'), 'wb') as f:
            f.write(content2)

        # Unique files in each mount point
        with open(os.path.join(self.mount_points[0], 'unique1.txt'), 'wb') as f:
            f.write(content3)
        with open(os.path.join(self.mount_points[1], 'unique2.txt'), 'wb') as f:
            f.write(content3)

    def test_add_single_mount_point(self):
        """Test adding a single mount point"""
        conn = create_database(self.db_path)
        add_mount_points(conn, [self.mount_points[0]])

        # Verify files were added
        c = conn.cursor()
        c.execute('SELECT COUNT(*) FROM files WHERE mount_point = ?', (self.mount_points[0],))
        count = c.fetchone()[0]
        self.assertEqual(count, 2)  # Should have two files from mount1
        conn.close()

    def test_add_multiple_mount_points(self):
        """Test adding multiple mount points"""
        conn = create_database(self.db_path)
        add_mount_points(conn, self.mount_points)

        # Verify files were added from both mount points
        c = conn.cursor()
        c.execute('SELECT COUNT(*) FROM files')
        total_count = c.fetchone()[0]
        self.assertEqual(total_count, 4)  # Should have all four test files
        conn.close()

    def test_add_nonexistent_mount_point(self):
        """Test adding a non-existent mount point"""
        conn = create_database(self.db_path)
        nonexistent_path = os.path.join(self.test_dir, 'nonexistent')
        add_mount_points(conn, [nonexistent_path])

        # Verify no files were added
        c = conn.cursor()
        c.execute('SELECT COUNT(*) FROM files')
        count = c.fetchone()[0]
        self.assertEqual(count, 0)  # Should have no files
        conn.close()

    def test_add_duplicate_files(self):
        """Test adding mount points with duplicate files"""
        conn = create_database(self.db_path)
        add_mount_points(conn, self.mount_points)

        # Verify duplicate files were properly handled
        c = conn.cursor()
        c.execute('''SELECT COUNT(*) FROM
                     (SELECT file_key, COUNT(*) as count
                      FROM files
                      GROUP BY file_key
                      HAVING count > 1)''')
        duplicate_count = c.fetchone()[0]
        self.assertEqual(duplicate_count, 1)  # Should have one duplicate file
        conn.close()

    def test_file_metadata(self):
        """Test that file metadata is correctly stored"""
        conn = create_database(self.db_path)
        add_mount_points(conn, [self.mount_points[0]])

        # Verify file metadata - use the correct path (file is in subdir)
        file_path = os.path.join(self.mount_points[0], 'subdir', 'file1.txt')
        c = conn.cursor()
        c.execute('SELECT file_size, last_modified FROM files WHERE full_path = ?', (file_path,))
        result = c.fetchone()
        self.assertIsNotNone(result, "File should exist in database")
        size, modified = result

        # Compare with actual file metadata
        self.assertEqual(size, os.path.getsize(file_path))
        self.assertEqual(modified, os.path.getmtime(file_path))
        conn.close()

    def test_path_based_duplicates(self):
        """Test that files with same relative paths are identified as duplicates"""
        conn = create_database(self.db_path)
        add_mount_points(conn, self.mount_points)

        # Verify path-based duplicates
        c = conn.cursor()

        # Debug: Print all files in database
        c.execute('SELECT file_key, full_path, mount_point FROM files')
        all_files = c.fetchall()
        print("\nAll files in database:")
        for file_key, full_path, mount_point in all_files:
            print(f"Key: {file_key}, Path: {full_path}, Mount: {mount_point}")

        # First verify total number of files
        c.execute('SELECT COUNT(*) FROM files')
        total_count = c.fetchone()[0]
        self.assertEqual(total_count, 4, "Should have four files total")

        # Then verify duplicates
        c.execute('''SELECT file_key, COUNT(*) as count
                     FROM files
                     GROUP BY file_key
                     HAVING count > 1''')
        duplicates = c.fetchall()

        # Should find one duplicate (subdir/file1.txt)
        self.assertEqual(len(duplicates), 1, "Should find exactly one duplicate file path")
        self.assertEqual(duplicates[0][0], os.path.join('subdir', 'file1.txt'))
        self.assertEqual(duplicates[0][1], 2, "Duplicate should appear exactly twice")

        # Verify unique files - update query to check actual file paths
        c.execute('''
            SELECT COUNT(*)
            FROM files
            WHERE file_key NOT IN (
                SELECT file_key
                FROM files
                GROUP BY file_key
                HAVING COUNT(*) > 1
            )
        ''')
        unique_count = c.fetchone()[0]
        self.assertEqual(unique_count, 2, "Should have two unique files")
        conn.close()

    def test_relative_paths(self):
        """Test that relative paths are properly stored"""
        conn = create_database(self.db_path)
        add_mount_points(conn, [self.mount_points[0]])

        c = conn.cursor()
        c.execute('SELECT file_key FROM files WHERE file_key LIKE ?', ('subdir/%',))
        files = c.fetchall()

        # Verify the relative path is correctly stored
        self.assertEqual(len(files), 1, "Should find one file in subdir")
        self.assertEqual(files[0][0], os.path.join('subdir', 'file1.txt'))
        conn.close()

if __name__ == '__main__':
    unittest.main()
