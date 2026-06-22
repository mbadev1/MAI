"""
MAI Dashboard Backend
Run with: pip install fastapi uvicorn paramiko websockets
Then: python dashboard_server.py
Open: http://localhost:8765
"""

import asyncio
import json
import os
import subprocess
import sys
import threading
from pathlib import Path
from typing import Dict, List, Optional

import paramiko
import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

app = FastAPI()

# ─── CONFIG ───────────────────────────────────────────────────────────────────
GROUPS = [
    {
        "name": "Group 1",
        "id": "group1",
        "server_ip": "86.50.170.225",
        "ssh_user": "ubuntu",
        "ssh_key": str(Path.home() / ".ssh" / "id_rsa"),
    },
    {
        "name": "Group 2",
        "id": "group2",
        "server_ip": "128.214.254.204",
        "ssh_user": "ubuntu",
        "ssh_key": str(Path.home() / ".ssh" / "id_rsa"),
    },
    {
        "name": "Group 3",
        "id": "group3",
        "server_ip": "128.214.255.139",
        "ssh_user": "ubuntu",
        "ssh_key": str(Path.home() / ".ssh" / "id_rsa"),
    },
    {
        "name": "Group 4",
        "id": "group4",
        "server_ip": "86.50.168.55",
        "ssh_user": "ubuntu",
        "ssh_key": str(Path.home() / ".ssh" / "id_rsa"),
    },
]

# ─── COMMANDS ─────────────────────────────────────────────────────────────────
MAI_AGENT_CMD = (
    "cd ~/dev/bazaar/1312MAIAgent/runtime && "
    "nohup java -cp "
    '"../bin:../../BasilicaCore/bin:../../BaseAgent/bin:'
    "../../LightSideMessageAnnotator/bin:"
    "../../SocialAgent/build/classes:"
    "../../SocketIOClient/bin:../../SocketIOClient/lib/socket.io-client-2.1.0.jar:"
    "../../SocketIOClient/lib/engine.io-client-2.1.0.jar:../../SocketIOClient/lib/okhttp-4.0.1.jar:"
    "../../SocketIOClient/lib/okio-2.3.0.jar:../../SocketIOClient/lib/json-20090211.jar:"
    "../../SocketIOClient/lib/json-org.jar:../../SocketIOClient/lib/annotations-13.0.jar:"
    "../../SocketIOClient/lib/kotlin-stdlib-1.3.41.jar:../../SocketIOClient/lib/kotlin-stdlib-common-1.3.41.jar:"
    "../../BaseAgent/lib/*:../../BasilicaCore/lib/*:../../BasilicaCore/lib/OtherLibraries/Utilities.jar:"
    "../../BasilicaCore/lib/OtherLibraries/Xerces/xercesImpl.jar:"
    "../../BasilicaCore/lib/OtherLibraries/Xerces/xml-apis.jar:"
    "../../BaseAgent/lib/commons-lang3-3.2.1.jar:"
    '../../BaseAgent/lib/Environments/ConcertChat/Libraries/*" '
    "basilica2.mai.operation.MAIAgentOperation > ~/MAI2.0/logs/mai_agent.log 2>&1 &"
)

SERVER_PY_CMD = (
    "cd ~/MAI2.0 && source .venv/bin/activate && "
    "nohup python server.py > logs/server.log 2>&1 &"
)

DOCKER_CMD = (
    "cd ~/dev/bazaar/bazaar_server/bazaar_server_lobby && "
    "sudo docker compose up -d && sleep 5 && "
    "sudo docker exec bazaar_lobby sed -i "
    "'1602a\\        socket.on(\\x27sendtrigger\\x27, async (room, command) => {\\n"
    "                io.sockets.in(room).emit(\\x27sendtrigger\\x27, room, command);\\n"
    "        });' /usr/bazaar/lobby/server_bazaar_local.js 2>/dev/null || true"
)

MANUAL_TRIGGER_CMD = """python3 -c "
import socketio, time
sio = socketio.Client()
sio.connect('http://localhost:8000', socketio_path='/bazsocket/socket.io', transports=['websocket'])
time.sleep(1)
sio.emit('adduser', ('testroom', 'testuser', False))
time.sleep(1)
sio.emit('sendtrigger', ('testroom', '{trigger_type}'))
time.sleep(3)
sio.disconnect()
" """

# ─── LOCAL CLIENT CONFIG ──────────────────────────────────────────────────────
LOCAL_CLIENT_PATH = str(Path(__file__).parent / "unified_asio_client.py")
LOCAL_GROUPS_CONFIG = str(Path(__file__).parent / "unified_groups.json")
local_client_process = None

# ─── STATE ────────────────────────────────────────────────────────────────────
connected_clients: List[WebSocket] = []
group_status: Dict[str, dict] = {
    g["id"]: {
        "server_py": "unknown",
        "mai_agent": "unknown",
        "docker": "unknown",
        "last_log": "",
    }
    for g in GROUPS
}

# Store the main event loop here so threads can use it
_main_loop = None


# ─── SSH HELPERS ──────────────────────────────────────────────────────────────
def ssh_run(group: dict, command: str, timeout: int = 30) -> tuple:
    """Run a command on a remote server via SSH."""
    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        # Try with key file first, fall back to agent
        connect_kwargs = {
            "hostname": group["server_ip"],
            "username": group["ssh_user"],
            "timeout": timeout,
            "banner_timeout": 30,
            "auth_timeout": 30,
            "look_for_keys": True,
            "allow_agent": True,
            "passphrase": "LET2023",
        }
        
        # Add key file if it exists
        key_path = group.get("ssh_key", "")
        if key_path and Path(key_path).exists():
            connect_kwargs["key_filename"] = key_path
        
        client.connect(**connect_kwargs)
        stdin, stdout, stderr = client.exec_command(command, timeout=timeout)
        out = stdout.read().decode().strip()
        err = stderr.read().decode().strip()
        client.close()
        return out, err, True
    except Exception as e:
        return "", str(e), False


def check_process(group: dict, process_name: str) -> str:
    out, err, ok = ssh_run(group, f"pgrep -f '{process_name}' && echo running || echo stopped")
    if not ok:
        return "error"
    return "running" if "running" in out else "stopped"


def check_docker(group: dict) -> str:
    out, err, ok = ssh_run(group, "sudo docker ps | grep bazaar_lobby && echo running || echo stopped")
    if not ok:
        return "error"
    return "running" if "running" in out else "stopped"


def get_logs(group: dict, lines: int = 30) -> str:
    out, _, ok = ssh_run(group, f"tail -n {lines} ~/MAI2.0/logs/server.log 2>/dev/null || echo 'No logs yet'")
    return out if ok else "Could not fetch logs"


def get_mai_logs(group: dict, lines: int = 30) -> str:
    out, _, ok = ssh_run(group, f"tail -n {lines} ~/MAI2.0/logs/mai_agent.log 2>/dev/null || echo 'No MAI logs yet'")
    return out if ok else "Could not fetch MAI logs"


# ─── LOCAL CLIENT HELPERS ─────────────────────────────────────────────────────
def read_groups_config() -> dict:
    try:
        with open(LOCAL_GROUPS_CONFIG, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return {"error": f"File not found: {LOCAL_GROUPS_CONFIG}", "groups": []}
    except Exception as e:
        return {"error": str(e), "groups": []}


def write_groups_config(config: dict) -> tuple:
    try:
        with open(LOCAL_GROUPS_CONFIG, 'w') as f:
            json.dump(config, f, indent=4)
        return True, "Config saved successfully"
    except Exception as e:
        return False, str(e)


def start_local_client() -> tuple:
    global local_client_process
    try:
        if local_client_process and local_client_process.poll() is None:
            return False, "Client is already running"
        if not Path(LOCAL_CLIENT_PATH).exists():
            return False, f"Client file not found: {LOCAL_CLIENT_PATH}"
        local_client_process = subprocess.Popen(
            [sys.executable, LOCAL_CLIENT_PATH],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        return True, f"Client started (PID: {local_client_process.pid})"
    except Exception as e:
        return False, str(e)


def stop_local_client() -> tuple:
    global local_client_process
    try:
        if local_client_process and local_client_process.poll() is None:
            local_client_process.terminate()
            local_client_process.wait(timeout=5)
            local_client_process = None
            return True, "Client stopped"
        return False, "Client is not running"
    except Exception as e:
        return False, str(e)


def get_local_client_status() -> str:
    global local_client_process
    if local_client_process and local_client_process.poll() is None:
        return "running"
    return "stopped"


# ─── SAFE SEND — works from any thread ───────────────────────────────────────
def safe_send(ws: WebSocket, message: dict):
    """Send a WebSocket message from a background thread safely."""
    global _main_loop
    if _main_loop is not None:
        asyncio.run_coroutine_threadsafe(ws.send_json(message), _main_loop)


# ─── BROADCAST ────────────────────────────────────────────────────────────────
async def broadcast(message: dict):
    dead = []
    for ws in connected_clients:
        try:
            await ws.send_json(message)
        except Exception:
            dead.append(ws)
    for ws in dead:
        connected_clients.remove(ws)


# ─── BACKGROUND STATUS POLLING ────────────────────────────────────────────────
async def poll_status():
    while True:
        for group in GROUPS:
            gid = group["id"]
            status = {
                "server_py": check_process(group, "server.py"),
                "mai_agent": check_process(group, "MAIAgentOperation"),
                "docker": check_docker(group),
                "last_log": get_logs(group, 5),
            }
            group_status[gid] = status
            await broadcast({"type": "status", "group_id": gid, "status": status})

        await broadcast({
            "type": "local_status",
            "status": get_local_client_status()
        })

        await asyncio.sleep(10)


# ─── ROUTES ───────────────────────────────────────────────────────────────────
@app.on_event("startup")
async def startup():
    global _main_loop
    _main_loop = asyncio.get_running_loop()
    asyncio.create_task(poll_status())


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connected_clients.append(websocket)
    for group in GROUPS:
        gid = group["id"]
        await websocket.send_json({"type": "status", "group_id": gid, "status": group_status[gid]})
    await websocket.send_json({
        "type": "local_status",
        "status": get_local_client_status()
    })
    try:
        while True:
            data = await websocket.receive_json()
            await handle_command(data, websocket)
    except WebSocketDisconnect:
        connected_clients.remove(websocket)


async def handle_command(data: dict, ws: WebSocket):
    action = data.get("action")
    group_id = data.get("group_id")
    group = next((g for g in GROUPS if g["id"] == group_id), None)

    # ── Local actions — no server needed ─────────────────────────────────────
    if action == "start_local_client":
        ok, msg = start_local_client()
        await ws.send_json({"type": "log", "group_id": group_id or "local",
                            "message": f"✅ {msg}" if ok else f"❌ {msg}"})
        await ws.send_json({"type": "local_status", "status": get_local_client_status()})
        return

    elif action == "stop_local_client":
        ok, msg = stop_local_client()
        await ws.send_json({"type": "log", "group_id": group_id or "local",
                            "message": f"✅ {msg}" if ok else f"❌ {msg}"})
        await ws.send_json({"type": "local_status", "status": get_local_client_status()})
        return

    elif action == "get_mic_config":
        config = read_groups_config()
        await ws.send_json({"type": "mic_config", "group_id": group_id or "local", "config": config})
        return

    elif action == "save_mic_config":
        config = data.get("config", {})
        ok, msg = write_groups_config(config)
        await ws.send_json({"type": "log", "group_id": group_id or "local",
                            "message": f"✅ {msg}" if ok else f"❌ {msg}"})
        return

    # ── Server actions — need a valid group ──────────────────────────────────
    if not group:
        await ws.send_json({"type": "error", "message": f"Unknown group: {group_id}"})
        return

    await ws.send_json({"type": "log", "group_id": group_id, "message": f"⚡ Running: {action}..."})

    # FIX: define run_and_respond using safe_send so it works from any thread
    def run_and_respond(cmd, success_msg, fail_msg):
        out, err, ok = ssh_run(group, cmd, timeout=60)
        msg = success_msg if ok else f"{fail_msg}: {err}"
        safe_send(ws, {"type": "log", "group_id": group_id, "message": msg})

    if action == "start_docker":
        threading.Thread(target=run_and_respond,
                         args=(DOCKER_CMD, "✅ Docker started", "❌ Docker failed"),
                         daemon=True).start()

    elif action == "stop_docker":
        threading.Thread(target=run_and_respond,
                         args=("cd ~/dev/bazaar/bazaar_server/bazaar_server_lobby && sudo docker compose down",
                               "✅ Docker stopped", "❌ Stop failed"),
                         daemon=True).start()

    elif action == "start_server":
        threading.Thread(target=run_and_respond,
                         args=(SERVER_PY_CMD, "✅ server.py started", "❌ server.py failed"),
                         daemon=True).start()

    elif action == "stop_server":
        threading.Thread(target=run_and_respond,
                         args=("pkill -f server.py || true", "✅ server.py stopped", "❌ Stop failed"),
                         daemon=True).start()

    elif action == "start_agent":
        threading.Thread(target=run_and_respond,
                         args=(MAI_AGENT_CMD, "✅ MAI agent started", "❌ Agent failed"),
                         daemon=True).start()

    elif action == "stop_agent":
        threading.Thread(target=run_and_respond,
                         args=("pkill -f MAIAgentOperation || true", "✅ MAI agent stopped", "❌ Stop failed"),
                         daemon=True).start()

    elif action == "trigger":
        trigger_type = data.get("trigger_type", "COGNITIVE").upper()
        cmd = MANUAL_TRIGGER_CMD.format(trigger_type=trigger_type)
        threading.Thread(target=run_and_respond,
                         args=(cmd, f"✅ Triggered {trigger_type}", "❌ Trigger failed"),
                         daemon=True).start()

    elif action == "get_logs":
        out = get_logs(group, 50)
        await ws.send_json({"type": "logs", "group_id": group_id, "logs": out})

    elif action == "get_mai_logs":
        out = get_mai_logs(group, 50)
        await ws.send_json({"type": "logs", "group_id": group_id, "logs": out})

    elif action == "start_all":
        def run_all():
            cmds = [
                (DOCKER_CMD, "✅ Docker started", "❌ Docker failed"),
                ("sleep 5", "", ""),
                (SERVER_PY_CMD, "✅ server.py started", "❌ server.py failed"),
                (MAI_AGENT_CMD, "✅ MAI agent started", "❌ Agent failed"),
            ]
            for cmd, success_msg, fail_msg in cmds:
                out, err, ok = ssh_run(group, cmd, timeout=60)
                if success_msg:
                    msg = success_msg if ok else f"{fail_msg}: {err}"
                    safe_send(ws, {"type": "log", "group_id": group_id, "message": msg})
        threading.Thread(target=run_all, daemon=True).start()

    elif action == "stop_all":
        cmd = ("pkill -f server.py || true; "
               "pkill -f MAIAgentOperation || true; "
               "cd ~/dev/bazaar/bazaar_server/bazaar_server_lobby && sudo docker compose down")
        threading.Thread(target=run_and_respond,
                         args=(cmd, "✅ All stopped", "❌ Stop failed"),
                         daemon=True).start()


@app.get("/", response_class=HTMLResponse)
async def index():
    html_path = Path(__file__).parent / "dashboard.html"
    if html_path.exists():
        return HTMLResponse(html_path.read_text())
    return HTMLResponse("<h1>dashboard.html not found</h1>")


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8765, reload=False)
