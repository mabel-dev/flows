"""
Python Sandbox Runner

This module provides a sandboxed execution environment for user-provided Python scripts.
It loads a script specified via command line, executes it, and manages communication
via stdin/stdout streams using JSON-formatted data.

The runner expects user scripts to define a `run(data, context)` function that processes
input data and returns modified data and context.

Usage:
    python python.py <script_path>

Where:
    <script_path> is the path to the user Python script to execute

Input Format:
    Each line from stdin should be a JSON object with:
    {
        "data": <any JSON data>,
        "context": <execution context object>
    }

Output Format:
    For each input, outputs a JSON object to stdout with:
    {
        "data": <processed data>,
        "context": <updated context>
    }

Error Handling:
    - Script-level errors terminate the process with exit code 1
    - Individual execution errors are caught and return an error context
"""

import json
import sys
import traceback


def main(script_path: str):
    """
    Main runner function that loads and executes user code with data from stdin.

    Args:
        script_path: Path to the Python script file to execute

    Process:
        1. Loads the user script
        2. Verifies it contains an 'execute' function
        3. Processes JSON input lines from stdin
        4. Returns JSON output to stdout
    """
    try:
        with open(script_path, "r") as f:
            user_code = f.read()

        user_globals = {}
        exec(user_code, user_globals)  # nosec

        if "execute" not in user_globals:
            raise ValueError("Script must define a `execute(data, context)` function")

        execute = user_globals["execute"]

        for line in sys.stdin:
            try:
                payload = json.loads(line)
                data = payload["data"]
                context = payload["context"]

                # Optionally pass flow_config here
                out_data, out_context = execute(data, context)

                print(json.dumps({"data": out_data, "context": out_context}), flush=True)

            except Exception:
                traceback.print_exc(file=sys.stderr)
                sys.stderr.flush()
                print(
                    json.dumps({"data": None, "context": {"error": "exception in user code"}}),
                    flush=True,
                )

    except Exception:
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.stderr.write("Usage: runner.py <script_path>\n")
        sys.exit(1)
    main(sys.argv[1])
