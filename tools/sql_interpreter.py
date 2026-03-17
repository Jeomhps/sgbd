#!/usr/bin/env python3
"""
Interactive SQL Interpreter for MiniSGBD

Features:
- Load tables from disk
- Interactive REPL for SQL queries
- Execute queries against disk-based tables
- Pretty-print results

Usage:
    python sql_interpreter.py
    or
    uv run python sql_interpreter.py
"""

import os
import sys
from typing import Dict, List, Optional

# Add project root to Python path to ensure imports work
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from core.TableDisque import TableDisque
from core.Tuple import Tuple
from sql.Catalog import Catalog
from sql.Parser import SQLParser
from sql.Planner import QueryPlanner
from sql.Executor import Executor


class SQLInterpreter:
    """Interactive SQL interpreter for MiniSGBD."""
    
    def __init__(self):
        self.catalog = Catalog()
        self.tables: Dict[str, TableDisque] = {}
    
    def load_table(self, table_name: str, file_path: str) -> bool:
        """Load a table from disk into the catalog."""
        try:
            if table_name in self.tables:
                print(f"⚠️  Table '{table_name}' already loaded")
                return False
            
            table = TableDisque(file_path)
            table.open()
            
            self.tables[table_name] = table
            self.catalog.register(table_name, table)
            
            print(f"✅ Loaded table '{table_name}' from {file_path}")
            print(f"   Schema: {table.tuple_size} columns, {table.table_size} rows")
            return True
        except FileNotFoundError:
            print(f"❌ File not found: {file_path}")
            return False
        except Exception as e:
            print(f"❌ Error loading table: {e}")
            return False
    
    def execute_query(self, sql: str) -> Optional[List[List]]:
        """Execute a SQL query and return results."""
        try:
            # Parse SQL
            query = SQLParser(sql).parse()
            
            # Plan query
            planner = QueryPlanner(self.catalog)
            op, plan = planner.plan(query)
            
            # Execute query
            results = Executor.execute(op)
            
            print(f"\n📊 Execution Plan:")
            print(plan)
            print()
            
            return results
        except Exception as e:
            print(f"❌ Query error: {e}")
            return None
    
    def print_results(self, results: List[List], headers: Optional[List[str]] = None):
        """Pretty-print query results."""
        if not results:
            print("(No results)")
            return
        
        if headers:
            # Print headers
            print("\t".join(str(h) for h in headers))
            print("-" * (sum(len(str(h)) for h in headers) + len(headers) * 4))
        
        # Print rows
        for row in results:
            print("\t".join(str(val) for val in row))
        
        print(f"\n({len(results)} row{'s' if len(results) != 1 else ''})")
    
    def repl(self):
        """Start interactive REPL."""
        print("🚀 MiniSGBD SQL Interpreter")
        print("Type 'help' for commands, 'exit' to quit\n")
        
        while True:
            try:
                # Get user input
                user_input = input("miniSGBD> ").strip()
                
                if not user_input:
                    continue
                
                # Handle commands
                if user_input.lower() in ['exit', 'quit', 'q']:
                    print("👋 Goodbye!")
                    break
                
                elif user_input.lower() in ['help', '?']:
                    self.print_help()
                    continue
                
                elif user_input.lower().startswith('load '):
                    parts = user_input.split()
                    if len(parts) == 3:
                        table_name, file_path = parts[1], parts[2]
                        self.load_table(table_name, file_path)
                    else:
                        print("❌ Usage: load <table_name> <file_path>")
                    continue
                
                elif user_input.lower().startswith('tables'):
                    self.list_tables()
                    continue
                
                # Execute SQL query
                results = self.execute_query(user_input)
                
                if results is not None:
                    self.print_results(results)
                
            except KeyboardInterrupt:
                print("\n👋 Goodbye!")
                break
            except EOFError:
                print("\n👋 Goodbye!")
                break
            except Exception as e:
                print(f"❌ Error: {e}")
    
    def print_help(self):
        """Print help information."""
        print("""
📖 MiniSGBD SQL Interpreter Help

Commands:
  LOAD <name> <path>   - Load table from disk file
  TABLES              - List loaded tables
  EXIT, QUIT, Q       - Exit interpreter
  HELP, ?             - Show this help

SQL Support:
  SELECT col1, col2, ... FROM table [WHERE conditions]
  SELECT * FROM table [WHERE conditions]
  SELECT AGG(col) FROM table [GROUP BY col] [WHERE conditions]
  JOIN queries: SELECT ... FROM table1, table2 WHERE table1.col = table2.col

Examples:
  LOAD employees data/employees.dat
  SELECT * FROM employees WHERE salary > 50000
  SELECT AVG(salary) FROM employees GROUP BY department
  SELECT e.name, d.name FROM employees e, departments d WHERE e.dept_id = d.id
""")
    
    def list_tables(self):
        """List loaded tables."""
        if not self.tables:
            print("(No tables loaded)")
            return
        
        print("\n📋 Loaded Tables:")
        for name, table in self.tables.items():
            print(f"  • {name}: {table.tuple_size} columns, {table.table_size} rows")
        print()


def main():
    """Main entry point."""
    interpreter = SQLInterpreter()
    
    # Check if there are any .dat files in the current directory
    dat_files = [f for f in os.listdir('.') if f.endswith('.dat')]
    
    if dat_files:
        print("🔍 Found .dat files in current directory:")
        for f in dat_files:
            print(f"  • {f}")
        print("Use 'LOAD <name> <file>' to load them\n")
    
    # Start REPL
    interpreter.repl()
    
    # Cleanup
    for table in interpreter.tables.values():
        table.close()


if __name__ == "__main__":
    main()
