# Tor Proxy Port Mapping

Use the port-specific batch file for each node. The root `start_tor_proxy.bat` has been removed.

## Port → Batch File → Node

| Port | SOCKS | Control | Batch File | Node / Use |
|------|-------|---------|------------|------------|
| 9050 | 9050 | 9051 | `proxies/start_tor_proxy_9050.bat` | Node 1 / Default |
| 9060 | 9060 | 9061 | `proxies/start_tor_proxy_9060.bat` | Node 2 |
| 9070 | 9070 | 9071 | `proxies/start_tor_proxy_9070.bat` | Node 3 |
| 9080 | 9080 | 9081 | `proxies/start_tor_proxy_9080.bat` | Node 4 |
| 9090 | 9090 | 9091 | `proxies/start_tor_proxy_9090.bat` | Node 5 |

## How to Run

From repo root:

```batch
REM Node 1 (default)
proxies\start_tor_proxy_9050.bat

REM Node 2
proxies\start_tor_proxy_9060.bat

REM Node 3
proxies\start_tor_proxy_9070.bat

REM Node 4
proxies\start_tor_proxy_9080.bat

REM Node 5
proxies\start_tor_proxy_9090.bat
```

Or from `proxies/` folder:

```batch
start_tor_proxy_9050.bat
start_tor_proxy_9060.bat
...
```

## Config

Set `TOR_SOCKS_PORT` and `TOR_CONTROL_PORT` in your scraper's env/config to match the node:

- Node 1: `TOR_SOCKS_PORT=9050` `TOR_CONTROL_PORT=9051`
- Node 2: `TOR_SOCKS_PORT=9060` `TOR_CONTROL_PORT=9061`
- etc.

## Argentina

Argentina has its own `scripts/Argentina/start_tor_proxy.bat` (port 9050, uses C:\TorProxy). For multi-node Argentina, use the port-specific bats above and set env accordingly.
