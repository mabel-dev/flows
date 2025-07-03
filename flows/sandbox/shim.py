# shim.py
import zmq
import importlib.util

def load_function(path):
    spec = importlib.util.spec_from_file_location("user_func", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.user_function

def main():
    socket_path = "ipc:///tmp/shim.sock"
    user_func = load_function("user_func.py")

    ctx = zmq.Context()
    socket = ctx.socket(zmq.REP)
    socket.bind(socket_path)

    while True:
        msg = socket.recv_json()
        if msg.get("cmd") == "shutdown":
            socket.send_json({"status": "ok"})
            break
        try:
            result = user_func(msg["data"])
            socket.send_json({"status": "ok", "result": result})
        except Exception as e:
            socket.send_json({"status": "error", "error": str(e)})

if __name__ == "__main__":
    main()