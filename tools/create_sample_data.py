#!/usr/bin/env python3
"""
Create sample data files for the SQL interpreter demo.

Creates:
- employees.dat: Employee data
- departments.dat: Department data
"""

import sys
import os

# Add the project root directory to Python path so we can import core module
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from core.TableDisque import TableDisque

# Get the tools directory path to ensure files are created there
tools_dir = os.path.dirname(os.path.abspath(__file__))

def create_employees():
    """Create employees table."""
    employees_path = os.path.join(tools_dir, "employees.dat")
    table = TableDisque(employees_path)
    table.create(tuple_size=4, table_size=0, randomize=False)
    
    # Manually add some employee data: id, name, salary, dept_id
    # We'll use the binary format directly
    import struct
    
    employees = [
        (1, 101, 50000, 10),   # John, Engineering
        (2, 102, 60000, 10),   # Jane, Engineering
        (3, 103, 55000, 20),   # Bob, Marketing
        (4, 104, 45000, 20),   # Alice, Marketing
        (5, 105, 70000, 30),   # Charlie, Sales
    ]
    
    with open(employees_path, "wb") as f:
        # Write header: table_size (4 bytes), tuple_size (4 bytes)
        f.write(struct.pack('II', len(employees), 4))
        
        # Write employee data
        for emp in employees:
            for val in emp:
                f.write(struct.pack('i', val))
    
    print("✅ Created employees.dat")
    return table

def create_departments():
    """Create departments table."""
    departments_path = os.path.join(tools_dir, "departments.dat")
    table = TableDisque(departments_path)
    table.create(tuple_size=2, table_size=0, randomize=False)
    
    # Manually add department data: dept_id, name
    import struct
    
    departments = [
        (10, 1001),   # Engineering
        (20, 1002),   # Marketing
        (30, 1003),   # Sales
    ]
    
    with open(departments_path, "wb") as f:
        # Write header
        f.write(struct.pack('II', len(departments), 2))
        
        # Write department data
        for dept in departments:
            for val in dept:
                f.write(struct.pack('i', val))
    
    print("✅ Created departments.dat")
    return table

if __name__ == "__main__":
    print("📁 Creating sample data files...")
    create_employees()
    create_departments()
    print("\n🎉 Sample data created successfully!")
    print("\nYou can now use the SQL interpreter:")
    print("  uv run python sql_interpreter.py")
    print("  LOAD employees employees.dat")
    print("  LOAD departments departments.dat")
    print("  SELECT * FROM employees WHERE salary > 50000")