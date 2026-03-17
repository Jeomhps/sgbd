#!/usr/bin/env python3
"""
Simple test runner for MiniSGBD project
Usage: python3 run.py [test_name]

Available tests:
  - all (default): Run all tests
  - project: Run Project tests
  - restrict: Run Restrict tests
  - chained: Run Chained restrict tests
  - fixed: Run Fixed data tests
  - join: Run Join tests
  - aggregate: Run Aggregate tests
  - clean: Clean cache files
"""

import sys
import subprocess
import os

def run_command(cmd):
    """Run a command and return success status"""
    try:
        result = subprocess.run(cmd, shell=True, check=True)
        return True
    except subprocess.CalledProcessError:
        return False

def main():
    if len(sys.argv) > 1:
        test_name = sys.argv[1].lower()
    else:
        test_name = "all"
    
    # Set up environment
    os.environ['PYTHONPATH'] = '.'
    
    if test_name in ['all', 'test']:
        print("🧪 Running all tests...")
        tests = [
            'tests/manual/TestProject.py',
            'tests/manual/TestRestrict.py',
            'tests/manual/TestRestrictChained.py', 
            'tests/manual/TestRestrictChainedFixed.py',
            'tests/manual/TestJoin.py',
            'tests/manual/TestAggregate.py'
        ]
        for test in tests:
            print(f"\n📋 {test}:")
            run_command(f"python3 {test}")
        print("\n🎉 All tests completed!")
        
    elif test_name == 'project':
        print("🧪 Running Project tests...")
        run_command("python3 tests/manual/TestProject.py")
        
    elif test_name == 'restrict':
        print("🧪 Running Restrict tests...")
        run_command("python3 tests/manual/TestRestrict.py")
        
    elif test_name == 'chained':
        print("🧪 Running Chained restrict tests...")
        run_command("python3 tests/manual/TestRestrictChained.py")
        
    elif test_name == 'fixed':
        print("🧪 Running Fixed data tests...")
        run_command("python3 tests/manual/TestRestrictChainedFixed.py")
    
    elif test_name == 'join':
        print("🧪 Running Join tests...")
        run_command("python3 tests/manual/TestJoin.py")
    
    elif test_name == 'aggregate':
        print("🧪 Running Aggregate tests...")
        run_command("python3 tests/manual/TestAggregate.py")
        
    elif test_name == 'clean':
        print("🧹 Cleaning cache files...")
        run_command("rm -rf __pycache__ tests/__pycache__ operators/__pycache__ core/__pycache__")
        print("✨ Cleaned!")
        
    else:
        print(f"❌ Unknown test: {test_name}")
        print("Available options: all, project, restrict, chained, fixed, join, aggregate, clean")

if __name__ == '__main__':
    main()