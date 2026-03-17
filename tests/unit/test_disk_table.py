"""
Unit tests for disk-based table storage.
"""

import os
import struct
import pytest
from core.TableDisque import TableDisque
from core.FullScanTableDisque import FullScanTableDisque
from core.Tuple import Tuple


class TestDiskTable:
    """Test disk-based table functionality"""
    
    def test_create_and_read(self, tmp_path):
        """Test creating and reading a disk table"""
        table_file = tmp_path / "test_table.dat"
        
        # Create table
        table = TableDisque(str(table_file))
        table.create(tuple_size=2, table_size=10, randomize=False)
        
        # Verify file exists
        assert table_file.exists()
        
        # Open and read
        table.open()
        
        # Check header was read correctly
        assert table.table_size == 10
        assert table.tuple_size == 2
        
        # Read first tuple
        first_tuple = table.get_tuple(0)
        assert first_tuple is not None
        assert len(first_tuple.val) == 2
        assert first_tuple.val[0] == 0  # Should be zero (not randomized)
        assert first_tuple.val[1] == 0
        
        table.close()
    
    def test_full_scan(self, tmp_path):
        """Test full scan of disk table"""
        table_file = tmp_path / "scan_table.dat"
        
        # Create and populate
        table = TableDisque(str(table_file))
        table.create(tuple_size=3, table_size=5, randomize=True)
        
        # Scan
        scan = FullScanTableDisque(table)
        scan.open()
        
        count = 0
        while scan.next() is not None:
            count += 1
        
        scan.close()
        
        assert count == 5
        assert scan.reads > 0  # Should have read some blocks
    
    def test_lru_cache(self, tmp_path):
        """Test LRU cache behavior"""
        table_file = tmp_path / "cache_table.dat"
        
        # Create table with small cache
        table = TableDisque(str(table_file), block_size=2, memory_blocks=2)
        table.create(tuple_size=2, table_size=6, randomize=False)
        table.open()
        
        # Access tuples in pattern that tests LRU
        # Block 0: tuples 0, 1
        # Block 1: tuples 2, 3
        # Block 2: tuples 4, 5
        
        # Access block 0
        t0 = table.get_tuple(0)
        assert t0 is not None
        
        # Access block 1
        t2 = table.get_tuple(2)
        assert t2 is not None
        
        # Access block 2 (should evict block 0 from cache)
        t4 = table.get_tuple(4)
        assert t4 is not None
        
        # Access block 0 again (should be read from disk, not cache)
        t0_again = table.get_tuple(0)
        assert t0_again is not None
        
        table.close()
    
    def test_file_format(self, tmp_path):
        """Test file format and header"""
        table_file = tmp_path / "format_table.dat"
        
        # Create table
        table = TableDisque(str(table_file))
        table.create(tuple_size=3, table_size=7, randomize=False)
        
        # Read file directly to verify format
        with open(table_file, 'rb') as f:
            # Read header (2 ints = 8 bytes)
            header = f.read(8)
            table_size, tuple_size = struct.unpack('II', header)
            
            assert table_size == 7
            assert tuple_size == 3
            
            # Read first tuple (3 ints = 12 bytes)
            tuple_data = f.read(12)
            vals = struct.unpack('iii', tuple_data)
            assert vals == (0, 0, 0)  # Zero-filled
    
    def test_error_handling(self, tmp_path):
        """Test error handling"""
        non_existent = tmp_path / "missing.dat"
        
        table = TableDisque(str(non_existent))
        
        with pytest.raises(FileNotFoundError):
            table.open()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])