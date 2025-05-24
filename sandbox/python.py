# sandbox/runner.py

import json
import sys
import traceback


def main(script_path: str):
    try:
        with open(script_path, "r") as f:
            user_code = f.read()

        user_globals = {}
        exec(user_code, user_globals)

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
