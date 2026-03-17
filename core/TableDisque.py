"""
Disk-based table storage for handling large datasets.
Inspired by the Java version but implemented in Python.
"""

import os
import struct
from typing import List, Optional
from core.Tuple import Tuple


class TableDisque:
    """
    Disk-based table storage with block I/O and caching.
    
    Stores data in binary format on disk instead of memory.
    Uses block-based I/O for efficient access to large datasets.
    """
    
    def __init__(self, file_path: str = "table.dat", block_size: int = 4, memory_blocks: int = 3):
        """
        Initialize disk-based table.
        
        Args:
            file_path: Path to storage file
            block_size: Number of tuples per block
            memory_blocks: Number of blocks to cache in memory
        """
        self.file_path = file_path
        self.block_size = block_size
        self.memory_blocks = memory_blocks
        self.tuple_size = 0
        self.table_size = 0
        self.file = None
        
        # LRU cache: block_id -> [tuples]
        self.cache = {}
        self.cache_order = []  # LRU order tracking
        
    def create(self, tuple_size: int, table_size: int, randomize: bool = True):
        """
        Create a new disk table with random data.
        
        Args:
            tuple_size: Number of attributes per tuple
            table_size: Number of tuples in table
            randomize: Whether to fill with random data
        """
        self.tuple_size = tuple_size
        self.table_size = table_size
        
        # Create file and write header
        with open(self.file_path, 'wb') as f:
            # Header: table_size (4 bytes), tuple_size (4 bytes)
            f.write(struct.pack('II', table_size, tuple_size))
            
            # Write tuples
            if randomize:
                import random
                for _ in range(table_size):
                    tuple_data = [random.randint(0, 100) for _ in range(tuple_size)]
                    for val in tuple_data:
                        f.write(struct.pack('i', val))  # 4 bytes per int
            else:
                # Write empty tuples (zeros)
                for _ in range(table_size):
                    for _ in range(tuple_size):
                        f.write(struct.pack('i', 0))
        
        print(f"✅ Disk table created: {table_size} tuples × {tuple_size} attributes")
    
    def open(self):
        """Open the table file for reading."""
        if not os.path.exists(self.file_path):
            raise FileNotFoundError(f"Table file not found: {self.file_path}")
        
        self.file = open(self.file_path, 'rb')
        
        # Read header
        header = self.file.read(8)  # 2 × 4 bytes
        if len(header) != 8:
            raise ValueError("Invalid table file format")
        
        self.table_size, self.tuple_size = struct.unpack('II', header)
        print(f"📖 Opened disk table: {self.table_size} tuples × {self.tuple_size} attributes")
    
    def close(self):
        """Close the table file."""
        if self.file:
            self.file.close()
            self.file = None
        self.cache = {}
        self.cache_order = []
    
    def _read_block(self, block_id: int) -> List[Optional[Tuple]]:
        """
        Read a block from disk into cache.
        
        Args:
            block_id: Block index to read
        
        Returns:
            List of tuples in the block (or None for end marker)
        """
        # Calculate file position (skip header + blocks before this one)
        data_start = 8  # header size
        tuple_bytes = self.tuple_size * 4  # 4 bytes per int
        block_bytes = self.block_size * tuple_bytes
        
        position = data_start + block_id * block_bytes
        
        # Check if block is already in cache
        if block_id in self.cache:
            # Move to end of LRU order (most recently used)
            self.cache_order.remove(block_id)
            self.cache_order.append(block_id)
            return self.cache[block_id]
        
        # Read block from disk
        self.file.seek(position)
        block_data = []
        
        for i in range(self.block_size):
            tuple_data = []
            for j in range(self.tuple_size):
                try:
                    val_bytes = self.file.read(4)
                    if len(val_bytes) == 0:
                        # End of file
                        block_data.append(None)
                        continue
                    val = struct.unpack('i', val_bytes)[0]
                    tuple_data.append(val)
                except EOFError:
                    block_data.append(None)
                    continue
            
            if tuple_data:
                tuple_obj = Tuple(len(tuple_data))
                tuple_obj.val = tuple_data
                block_data.append(tuple_obj)
            else:
                block_data.append(None)
        
        # Add to cache (with LRU management)
        if len(self.cache) >= self.memory_blocks:
            # Remove least recently used block
            lru_block = self.cache_order.pop(0)
            del self.cache[lru_block]
        
        self.cache[block_id] = block_data
        self.cache_order.append(block_id)
        
        return block_data
    
    def get_tuple(self, tuple_index: int) -> Optional[Tuple]:
        """
        Get a specific tuple by index.
        
        Args:
            tuple_index: Index of tuple to retrieve
        
        Returns:
            Tuple object or None if not found
        """
        block_id = tuple_index // self.block_size
        block_offset = tuple_index % self.block_size
        
        block = self._read_block(block_id)
        
        if block_offset < len(block) and block[block_offset] is not None:
            return block[block_offset]
        else:
            return None
    
    def __del__(self):
        """Destructor to ensure file is closed."""
        self.close()


# Example usage
if __name__ == "__main__":
    # Create a disk table
    table = TableDisque("example_table.dat")
    table.create(tuple_size=3, table_size=100)
    
    # Scan the table
    from core.FullScanTableDisque import FullScanTableDisque
    scan = FullScanTableDisque(table)
    scan.open()
    
    count = 0
    while True:
        tuple_obj = scan.next()
        if tuple_obj is None:
            break
        print(f"Tuple {count}: {tuple_obj}")
        count += 1
        if count >= 5:  # Just show first 5
            print("... (truncated)")
            break
    
    scan.close()
    print(f"Total tuples: {scan.table.table_size}")
    print(f"Block reads: {scan.reads}")