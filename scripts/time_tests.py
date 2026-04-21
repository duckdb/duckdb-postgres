#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import subprocess
import time
import glob

def run_and_time_unit_tests():
    """
    Discovers and runs unit tests, records their elapsed time, and returns
    a list of test results sorted by elapsed time in descending order.
    """
    # Assuming the script is run from the project root (e.g., duckdb-postgres/)
    project_root = os.getcwd()

    # Define paths to the unittest executable and extension repository
    # These paths are relative to the project_root
    unittest_executable_path = os.path.join(project_root, 'build', 'release', 'test', 'unittest')
    extension_repo_path = os.path.join(project_root, 'build', 'release', 'repository')
    test_directory = os.path.join(project_root, 'test')

    # --- Pre-flight checks ---
    if not os.path.exists(unittest_executable_path):
        print(f"Error: Unittest executable not found at '{unittest_executable_path}'. "
              "Please ensure the project is built and the path is correct.")
        return []

    if not os.path.exists(extension_repo_path):
        print(f"Warning: Extension repository not found at '{extension_repo_path}'. "
              "Tests requiring extensions might fail. Please ensure it exists if needed.")

    if not os.path.isdir(test_directory):
        print(f"Error: Test directory not found at '{test_directory}'. "
              "Please ensure the 'test/' directory exists in the project root.")
        return []

    # Discover all .test files recursively within the 'test/' directory
    test_files = glob.glob(os.path.join(test_directory, '**', '*.test'), recursive=True)

    if not test_files:
        print(f"No '.test' files found in '{test_directory}'.")
        return []

    print(f"Found {len(test_files)} test files. Running them one by one...\n")

    results = []
    # Prepare the environment variables for the subprocess
    # Copy current environment and add/override LOCAL_EXTENSION_REPO
    env_vars = os.environ.copy()
    env_vars['LOCAL_EXTENSION_REPO'] = extension_repo_path

    test_files = sorted(test_files)
    for test_file_full_path in test_files:
        print(test_file_full_path)

    sys.exit(1)

    for test_file_full_path in test_files:
        # Get the path relative to the project root, as required by the unittest executable
        test_name = os.path.relpath(test_file_full_path, project_root)
        command = [unittest_executable_path, test_name]

        print(f"--- Running test: {test_name} ---")
        start_time = time.monotonic()
        process_returncode = -1 # Default to error state

        try:
            # Execute the command. capture_output=True captures stdout/stderr.
            # text=True decodes stdout/stderr as text.
            # check=False prevents an exception for non-zero exit codes, allowing us to record time for failed tests.
            process = subprocess.run(
                command,
                env=env_vars,
                capture_output=True,
                text=True,
                check=False
            )
            end_time = time.monotonic()
            elapsed_time = end_time - start_time
            process_returncode = process.returncode

            results.append({
                'test_name': test_name,
                'elapsed_time': elapsed_time,
                'returncode': process_returncode
            })

            if process_returncode != 0:
                print(f"  Test FAILED (Return Code: {process_returncode}) - Elapsed: {elapsed_time:.4f} seconds")
                # Uncomment the following lines to see stdout/stderr for failed tests
                # print("  --- STDOUT ---")
                # print(process.stdout.strip())
                # print("  --- STDERR ---")
                # print(process.stderr.strip())
            else:
                print(f"  Test PASSED - Elapsed: {elapsed_time:.4f} seconds")

        except FileNotFoundError:
            print(f"  Error: Unittest executable not found or command failed to start for '{test_name}'.")
            results.append({'test_name': test_name, 'elapsed_time': -1.0, 'returncode': -1})
        except Exception as e:
            print(f"  An unexpected error occurred while running '{test_name}': {e}")
            results.append({'test_name': test_name, 'elapsed_time': -1.0, 'returncode': -1})
        print("-" * (len(test_name) + 18)) # Separator for clarity

    # Sort results by elapsed time in descending order
    # Tests that errored out (elapsed_time = -1.0) will appear at the end due to sorting behavior
    sorted_results = sorted(results, key=lambda x: x['elapsed_time'], reverse=True)

    print("\n\n--- Test Summary (Sorted by Elapsed Time, Descending) ---")
    if not sorted_results:
        print("No test results to display.")
    else:
        for result in sorted_results:
            status = "PASSED" if result['returncode'] == 0 else "FAILED" if result['returncode'] != -1 else "ERROR"
            if result['elapsed_time'] >= 0:
                print(f"{result['test_name']}: {result['elapsed_time']:.4f} seconds ({status})")
            else:
                print(f"{result['test_name']}: Error during execution ({status})")

    return sorted_results

if __name__ == "__main__":
    run_and_time_unit_tests()
