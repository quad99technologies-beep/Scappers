import httpx
import logging
from typing import Optional, Dict

log = logging.getLogger(__name__)

class HttpClient:
    """Wrapper for httpx client with defaults."""
    
    def __init__(self, 
                 timeout: float = 30.0, 
                 retries: int = 3,
                 headers: Optional[Dict[str, str]] = None,
                 cookies: Optional[Dict[str, str]] = None):
        
        self.headers = headers or {}
        if not self.headers.get("User-Agent"):
            self.headers["User-Agent"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/110.0.0.0 Safari/537.36"
            
        self.cookies = cookies
        self.timeout = timeout
        self.retries = retries
        
        # Configure client
        self.client = httpx.AsyncClient(
            headers=self.headers,
            cookies=self.cookies,
            timeout=self.timeout,
            follow_redirects=True,
            limits=httpx.Limits(max_keepalive_connections=50, max_connections=50)
        )

    async def get(self, url: str) -> httpx.Response:
        """Get URL with retry logic."""
        for attempt in range(1, self.retries + 1):
            try:
                resp = await self.client.get(url)
                return resp
            except Exception as e:
                log.warning(f"GET {url} failed attempt {attempt}: {e}")
                if attempt == self.retries:
                    raise
                await httpx.sleep(1.0) # Simple backoff
    
    async def close(self):
        await self.client.aclose()
    
    async def __aenter__(self):
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
