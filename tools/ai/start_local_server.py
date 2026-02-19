import os
import sys
import subprocess
import argparse

def main():
    parser = argparse.ArgumentParser(description="Start Local AI Server (OpenAI Compatible)")
    parser.add_argument("--model", required=True, help="Path to GGUF model file (e.g. qwen2.5-7b-instruct-q4_k_m.gguf)")
    parser.add_argument("--port", type=int, default=8000, help="Port to listen on (default: 8000)")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind to (default: 0.0.0.0)")
    parser.add_argument("--gpu-layers", type=int, default=-1, help="Number of GPU layers to offload (-1 for all)")
    parser.add_argument("--ctx-size", type=int, default=4096, help="Context size (default: 4096)")
    
    args = parser.parse_args()

    # Check for llama-cpp-python
    try:
        import llama_cpp
    except ImportError:
        print("[INFO] llama-cpp-python not found. Installing...")
        # Note: Installation command may vary based on hardware (Metal vs CUDA vs CPU)
        # Using default pip install for simplicity, user might need specific CMAKE_ARGS
        subprocess.check_call([sys.executable, "-m", "pip", "install", "llama-cpp-python[server]"])

    print(f"[INFO] Starting local AI server with model: {args.model}")
    print(f"[INFO] API will be available at http://{args.host}:{args.port}/v1")
    
    cmd = [
        sys.executable, "-m", "llama_cpp.server",
        "--model", args.model,
        "--port", str(args.port),
        "--host", args.host,
        "--n_gpu_layers", str(args.gpu_layers),
        "--n_ctx", str(args.ctx_size)
    ]
    
    try:
        subprocess.run(cmd, check=True)
    except KeyboardInterrupt:
        print("\n[INFO] Server stopped.")
    except Exception as e:
        print(f"[ERROR] Server failed: {e}")

if __name__ == "__main__":
    main()
