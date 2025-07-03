import zmq
import time
import subprocess
import os

# --- Write the user function to a file ---
with open("user_func.py", "w") as f:
    f.write("""
def user_function(x):
    return x * 2
""")

# --- Start the shim process ---
shim_proc = subprocess.Popen(["python", "flows/sandbox/shim.py"])

# --- Setup ZMQ ---
ctx = zmq.Context()
socket = ctx.socket(zmq.REQ)
socket.connect("ipc:///tmp/shim.sock")

# --- Wait briefly to let shim bind socket ---
time.sleep(0.5)

# --- Send four messages ---
for i in range(4):
    socket.send_json({"data": i})
    reply = socket.recv_json()
    print(f"Response {i}: {reply}")

# --- Shutdown shim ---
socket.send_json({"cmd": "shutdown"})
print("Shutdown response:", socket.recv_json())

# --- Cleanup ---
shim_proc.wait()
os.remove("user_func.py")
os.remove("/tmp/shim.sock")