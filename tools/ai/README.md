
# Local AI Server Setup

This project supports using a local LLM server (OpenAI-compatible) for offline inference.
Recommended model: **Qwen2.5-7B-Instruct** (GGUF format).

## Prerequisites

1.  **Python 3.10+**
2.  **`llama-cpp-python`**:
    -   Install with hardware acceleration support (Metal for Mac, CUDA for NVIDIA).
    -   Example for Mac (Metal):
        ```bash
        CMAKE_ARGS="-O3 -DGGML_METAL=on" pip install --upgrade --force-reinstall --no-cache-dir llama-cpp-python[server]
        ```
    -   Example for Windows (CUDA):
        ```bash
        set CMAKE_ARGS="-DGGML_CUDA=on"
        pip install --upgrade --force-reinstall --no-cache-dir llama-cpp-python[server]
        ```

## Usage

1.  **Download Model**:
    Download the GGUF file for Qwen (e.g., `qwen2.5-7b-instruct-q4_k_m.gguf`) from Hugging Face.
    [Qwen/Qwen2.5-7B-Instruct-GGUF](https://huggingface.co/Qwen/Qwen2.5-7B-Instruct-GGUF)

2.  **Start Server**:
    Run the launcher script:
    ```bash
    python tools/ai/start_local_server.py --model "path/to/qwen2.5-7b-instruct-q4_k_m.gguf"
    ```
    
    This will start an OpenAI-compatible server at `http://localhost:8000/v1`.

3.  **Configure Scraper**:
    Update your `.env` file to point to the local server:
    ```env
    OPENAI_API_BASE=http://localhost:8000/v1
    OPENAI_API_KEY=sk-dummy-key  # Key is ignored by local server but required by client
    OPENAI_MODEL_NAME=path/to/qwen2.5-7b-instruct-q4_k_m.gguf  # Or use a generic name if server maps it
    ```

## Integration

The scrapers using `openai` library will automatically direct requests to the local server when `OPENAI_API_BASE` is set.
