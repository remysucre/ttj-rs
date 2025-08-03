#!/usr/bin/env python3
"""
Script to automate the process of running a query test.

Usage:
    python run_query.py 1a
    python run_query.py 20b
"""

import argparse
import subprocess
import sys
import os
from pathlib import Path


def run_command(cmd, description, cwd=None):
    """Run a command and handle errors."""
    print(f"\n{'='*60}")
    print(f"Running: {description}")
    print(f"Command: {' '.join(cmd)}")
    print(f"{'='*60}")
    
    try:
        result = subprocess.run(
            cmd, 
            check=True, 
            capture_output=True, 
            text=True,
            cwd=cwd
        )
        print(result.stdout)
        if result.stderr:
            print("STDERR:", result.stderr)
        return True
    except subprocess.CalledProcessError as e:
        print(f"ERROR: Command failed with exit code {e.returncode}")
        print(f"STDOUT: {e.stdout}")
        print(f"STDERR: {e.stderr}")
        return False
    except FileNotFoundError:
        print(f"ERROR: Command not found: {cmd[0]}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Automate query testing workflow",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python run_query.py 1a
    python run_query.py 20b
    python run_query.py 1a 2b 3c  # Multiple queries
        """
    )
    parser.add_argument(
        "query_names", 
        nargs="+",
        help="Query name(s) to test (e.g., 1a, 20b)"
    )
    parser.add_argument(
        "--skip-replace", 
        action="store_true",
        help="Skip the replace.sh step (useful if files are already in junk/)"
    )
    parser.add_argument(
        "--skip-codegen", 
        action="store_true",
        help="Skip the code generation step"
    )
    parser.add_argument(
        "--skip-test", 
        action="store_true",
        help="Skip the cargo test step"
    )
    
    args = parser.parse_args()
    
    # Get script directory and project root
    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    
    # Change to codegen directory for most operations
    os.chdir(script_dir)
    
    success_count = 0
    total_count = len(args.query_names)
    
    for query_name in args.query_names:
        print(f"\n{'#'*80}")
        print(f"# Processing Query: {query_name}")
        print(f"{'#'*80}")
        
        query_success = True
        
        # Step 1: Run replace.sh
        if not args.skip_replace:
            if not run_command(
                ["./replace.sh", query_name],
                f"Setting up files for query {query_name}",
                cwd=script_dir
            ):
                print(f"❌ Failed to set up files for query {query_name}")
                query_success = False
                continue
        else:
            print("⏭️  Skipping replace.sh step")
        
        # Step 2: Run rust.py code generation
        if not args.skip_codegen:
            if not run_command(
                ["uv", "run", "rust.py", "--sql_dir=junk", "--stats_dir=junk", "--output_dir=junk"],
                f"Generating Rust code for query {query_name}",
                cwd=script_dir
            ):
                print(f"❌ Failed to generate code for query {query_name}")
                query_success = False
                continue
        else:
            print("⏭️  Skipping code generation step")
        
        # Step 3: Run cargo test
        if not args.skip_test:
            test_name = f"test_q{query_name}"
            if not run_command(
                ["cargo", "test", test_name],
                f"Running test for query {query_name}",
                cwd=project_root
            ):
                print(f"❌ Test failed for query {query_name}")
                query_success = False
                continue
        else:
            print("⏭️  Skipping cargo test step")
        
        if query_success:
            print(f"✅ Successfully completed all steps for query {query_name}")
            success_count += 1
        else:
            print(f"❌ Failed processing query {query_name}")
    
    # Final summary
    print(f"\n{'='*80}")
    print(f"FINAL SUMMARY")
    print(f"{'='*80}")
    print(f"Successfully processed: {success_count}/{total_count} queries")
    
    if success_count == total_count:
        print("🎉 All queries processed successfully!")
        sys.exit(0)
    else:
        failed_count = total_count - success_count
        print(f"❌ {failed_count} queries failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
