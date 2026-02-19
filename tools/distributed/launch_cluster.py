
import argparse
import json
import subprocess
import threading
import time
import os
import sys
from pathlib import Path

# Try to import paramiko for SSH, but don't fail immediately if missing (might be local run)
try:
    import paramiko
except ImportError:
    paramiko = None

def load_config(config_path):
    if not os.path.exists(config_path):
        return None
    with open(config_path, 'r') as f:
        return json.load(f)

def run_local_worker(script_path, worker_id, env_vars=None):
    """Run a worker process locally."""
    env = os.environ.copy()
    if env_vars:
        env.update(env_vars)
    
    env['WORKER_ID'] = str(worker_id)
    
    cmd = [sys.executable, script_path]
    print(f"[LOCAL] Starting worker {worker_id}: {' '.join(cmd)}")
    
    # We use subprocess.Popen to run in parallel
    process = subprocess.Popen(
        cmd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True
    )
    
    # Simple log streamer
    def stream_logs(pipe, prefix):
        for line in iter(pipe.readline, ''):
            print(f"[{prefix}] {line.strip()}")
            
    threading.Thread(target=stream_logs, args=(process.stdout, f"Worker-{worker_id}"), daemon=True).start()
    threading.Thread(target=stream_logs, args=(process.stderr, f"Worker-{worker_id}-ERR"), daemon=True).start()
    
    return process

def run_remote_worker(node_config, script_path, worker_id, global_env_vars=None):
    """Run a worker process on a remote node via SSH."""
    if not paramiko:
        print("[ERROR] Paramiko not installed. Cannot run remote workers. Install with 'pip install paramiko'.")
        return None

    host = node_config.get('host')
    user = node_config.get('user')
    password = node_config.get('password')
    key_path = node_config.get('key_path')
    repo_path = node_config.get('repo_path', '~/quad99/Scrappers')
    python_path = node_config.get('python_path', 'python3')
    
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    try:
        connect_kwargs = {'hostname': host, 'username': user}
        if password:
            connect_kwargs['password'] = password
        if key_path:
            connect_kwargs['key_filename'] = key_path
            
        print(f"[REMOTE] Connecting to {user}@{host}...")
        client.connect(**connect_kwargs)
        
        # Build command
        # 1. cd to repo
        # 2. set env vars
        # 3. running python script
        
        env_str = ""
        if global_env_vars:
            for k, v in global_env_vars.items():
                env_str += f"export {k}='{v}'; "
        env_str += f"export WORKER_ID='{worker_id}'; "
        
        # Remote command: Use nohup to keep it running? Or just run in foreground of SSH channel?
        # For simplicity, we run in foreground so we can stream logs.
        cmd = f"cd {repo_path} && {env_str} {python_path} {script_path}"
        
        print(f"[REMOTE] {host}: Executing {cmd}")
        stdin, stdout, stderr = client.exec_command(cmd, get_pty=True)
        
        # Stream logs
        def stream_remote_logs(channel, prefix):
            while not channel.exit_status_ready():
                if channel.recv_ready():
                    data = channel.recv(1024).decode('utf-8', errors='ignore')
                    if data:
                        for line in data.splitlines():
                            print(f"[{prefix}] {line.strip()}")
                time.sleep(0.1)
                
        # Paramiko exec_command returns channels that are a bit tricky to stream line-by-line in real-time
        # But let's try a simple loop
        t = threading.Thread(target=stream_remote_logs, args=(stdout.channel, f"{host}-Worker-{worker_id}"), daemon=True)
        t.start()
        
        return client, stdout.channel  # Return client to keep connection open
        
    except Exception as e:
        print(f"[ERROR] Failed to start worker on {host}: {e}")
        return None

def main():
    parser = argparse.ArgumentParser(description="Distributed Scraper Launcher")
    parser.add_argument('--script', required=True, help="Path to the worker script (relative to project root)")
    parser.add_argument('--config', default='tools/distributed/cluster_config.json', help="Path to cluster config file")
    parser.add_argument('--local', action='store_true', help="Run locally (ignore config nodes)")
    parser.add_argument('--workers', type=int, default=1, help="Number of workers (per node OR total local)")
    parser.add_argument('--env', action='append', help="Set env var (KEY=VALUE)")
    
    args = parser.parse_args()
    
    # Parse env vars
    env_vars = {}
    if args.env:
        for item in args.env:
            if '=' in item:
                k, v = item.split('=', 1)
                env_vars[k] = v
                
    processes = []
    ssh_clients = []
    
    if args.local:
        print(f"--- Launching {args.workers} Local Workers ---")
        for i in range(args.workers):
            p = run_local_worker(args.script, i+1, env_vars)
            processes.append(p)
            time.sleep(1) # Stagger start
            
    else:
        config = load_config(args.config)
        if not config:
            print(f"[WARNING] Config file not found at {args.config}. Running locally.")
            # Fallback to local
            for i in range(args.workers):
                 p = run_local_worker(args.script, i+1, env_vars)
                 processes.append(p)
        else:
            global_env = config.get('env_vars', {})
            global_env.update(env_vars)
            
            nodes = config.get('nodes', [])
            total_launched = 0
            
            for node in nodes:
                # Determine workers for this node
                node_workers = node.get('workers', args.workers)
                
                print(f"--- Launching {node_workers} Workers on {node.get('host')} ---")
                
                for i in range(node_workers):
                    # We need separate SSH connections for separate streams/processes usually,
                    # or keep one connection and use multiple channels. 
                    # For simplicity, we open a new connection per worker for now (not efficient but easy).
                    res = run_remote_worker(node, args.script, total_launched+1, global_env)
                    if res:
                        client, channel = res
                        ssh_clients.append((client, channel))
                        total_launched += 1
                    time.sleep(1)
            
    print(f"--- All workers launched. Press Ctrl+C to stop. ---")
    
    try:
        while True:
            time.sleep(1)
            # Check local processes (iterate over copy to allow safe removal)
            for p in processes[:]:
                if p.poll() is not None:
                    print(f"[LOCAL] Worker process {p.pid} exited with code {p.returncode}")
                    processes.remove(p)
            
            # Check remote channels
            for client, channel in ssh_clients[:]:
                if channel.exit_status_ready():
                     print(f"[REMOTE] Remote worker finished with code {channel.recv_exit_status()}")
                     ssh_clients.remove((client, channel))
                     client.close()
            
            if not processes and not ssh_clients:
                print("All workers finished.")
                break
                
    except KeyboardInterrupt:
        print("\n[STOP] Stopping all workers...")
        for p in processes:
            p.terminate()
        for client, channel in ssh_clients:
            client.close() # This kills the SSH session
            
if __name__ == "__main__":
    main()
