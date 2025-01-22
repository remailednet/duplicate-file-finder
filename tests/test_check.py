import unittest
import os
import tempfile
import sqlite3
from duplicate_file_finder.database import create_database
from duplicate_file_finder.scanner import add_mount_points
from duplicate_file_finder.core import (
    find_duplicates,
    analyze_duplicates,
    generate_delete_commands,
    get_file_hash
)

class TestCheck(unittest.TestCase):
    def setUp(self):
        # Create temporary test directory and database
        self.test_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.test_dir, 'test.db')

        # Create test mount points
        self.mount_points = [
            os.path.join(self.test_dir, 'mount1'),
            os.path.join(self.test_dir, 'mount2'),
            os.path.join(self.test_dir, 'mount3')
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
        # Create test scenarios:
        # 1. Same path, different content
        # 2. Different path, same content
        # 3. Same path, same content
        # 4. Unique files

        same_content = b"Same content in different files"
        different_content = b"Different content for same path"
        unique_content = b"Unique file content"

        # Scenario 1: Same path, different content
        os.makedirs(os.path.join(self.mount_points[0], 'dir1'))
        os.makedirs(os.path.join(self.mount_points[1], 'dir1'))
        with open(os.path.join(self.mount_points[0], 'dir1', 'file1.txt'), 'wb') as f:
            f.write(different_content)
        with open(os.path.join(self.mount_points[1], 'dir1', 'file1.txt'), 'wb') as f:
            f.write(b"Another different content")

        # Scenario 2: Different path, same content
        with open(os.path.join(self.mount_points[0], 'same_content1.txt'), 'wb') as f:
            f.write(same_content)
        with open(os.path.join(self.mount_points[1], 'same_content2.txt'), 'wb') as f:
            f.write(same_content)

        # Scenario 3: Same path, same content
        os.makedirs(os.path.join(self.mount_points[1], 'dir2'))
        os.makedirs(os.path.join(self.mount_points[2], 'dir2'))
        with open(os.path.join(self.mount_points[1], 'dir2', 'exact_dup.txt'), 'wb') as f:
            f.write(same_content)
        with open(os.path.join(self.mount_points[2], 'dir2', 'exact_dup.txt'), 'wb') as f:
            f.write(same_content)

        # Scenario 4: Unique files
        with open(os.path.join(self.mount_points[0], 'unique1.txt'), 'wb') as f:
            f.write(unique_content)
        with open(os.path.join(self.mount_points[1], 'unique2.txt'), 'wb') as f:
            f.write(b"Another unique content")

    def test_find_duplicates(self):
        """Test finding duplicate files"""
        duplicates = find_duplicates(self.conn)
        self.assertGreater(len(duplicates), 0, "Should find some duplicates")

        # Test duplicates format
        for file_key, paths, sizes, mount_count in duplicates:
            self.assertIsInstance(file_key, str)
            self.assertIsInstance(paths, str)
            self.assertIsInstance(sizes, str)
            self.assertGreater(mount_count, 1)

    def test_analyze_duplicates(self):
        """Test analyzing duplicates for exact and path-based matches"""
        duplicates = find_duplicates(self.conn)

        # Debug: Print duplicates found
        print("\nDuplicates found:")
        for dup in duplicates:
            print(f"Duplicate: {dup}")

        # Check if we have duplicates before analyzing
        self.assertGreater(len(duplicates), 0, "Should find duplicates to analyze")

        # Analyze duplicates
        exact_duplicates, path_duplicates = analyze_duplicates(duplicates)

        # Debug: Print analyzed results
        print("\nExact duplicates:")
        for dup in exact_duplicates:
            print(f"Key: {dup[0]}, Paths: {dup[1]}")
        print("\nPath duplicates:")
        for dup in path_duplicates:
            print(f"Key: {dup[0]}, Hashes: {dup[1]}")

        # Verify exact duplicates (same path and content)
        self.assertTrue(len(exact_duplicates) > 0, "Should find exact duplicates")
        for dup_key, dup_paths in exact_duplicates:
            if os.path.basename(dup_key) == 'exact_dup.txt':
                self.assertEqual(len(dup_paths), 2, "Should have two copies of exact duplicate")
                return
        self.fail("Did not find expected exact duplicate 'exact_dup.txt'")

    def test_empty_database(self):
        """Test checking duplicates in empty database"""
        empty_db_path = os.path.join(self.test_dir, 'empty.db')
        empty_conn = create_database(empty_db_path)

        duplicates = find_duplicates(empty_conn)
        self.assertEqual(len(duplicates), 0, "Should find no duplicates in empty database")

        empty_conn.close()
        os.remove(empty_db_path)

    def test_duplicate_content_different_paths(self):
        """Test finding files with same content but different paths"""
        duplicates = find_duplicates(self.conn)
        exact_duplicates, _ = analyze_duplicates(duplicates)

        # Debug: Print all duplicates
        print("\nChecking for content duplicates with different paths:")
        for dup_key, dup_paths in exact_duplicates:
            print(f"Checking {dup_key}:")
            for path, size in dup_paths:
                print(f"  Path: {path}, Size: {size}")

        # Find duplicates with same content but different paths
        same_content_dups = []
        for dup_key, dup_paths in exact_duplicates:
            paths = [path for path, _ in dup_paths]
            if any('same_content' in path for path in paths):
                same_content_dups.append((dup_key, dup_paths))

        self.assertEqual(len(same_content_dups), 0,
                        "Should not identify files with different paths as duplicates")

if __name__ == '__main__':
    unittest.main()
