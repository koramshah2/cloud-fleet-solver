#!/usr/bin/env python3
"""
edge_proxy_mesh.py
=============================================================================
Universal Distributed Edge Mesh Proxy Client for AAA AUTO Fleet.
Routes outgoing bot API requests through the 5 Cloudflare Worker edge nodes,
dispersing traffic globally across Cloudflare's anycast residential/edge IP pool.
Prevents multiple IP bans, datacenter ASN blocking, and concurrent request stampedes.
=============================================================================
"""

import os
import json
import time
import random
import requests
from typing import Dict, Any, Optional

CF_EDGE_PROXIES = [
    "https://restore-agy.agorameet.workers.dev/api/fleet-proxy",
    "https://restore-agy.aaa-bot.workers.dev/api/fleet-proxy",
    "https://restore-agy.aaaai2.workers.dev/api/fleet-proxy",
    "https://restore-agy.aaa222.workers.dev/api/fleet-proxy",
    "https://restore-agy.aaaai.workers.dev/api/fleet-proxy"
]

def get_node_for_account(account_index: int = 1) -> str:
    """Returns a deterministic edge proxy node URL for the given account index."""
    idx = max(0, int(account_index) - 1)
    return CF_EDGE_PROXIES[idx % len(CF_EDGE_PROXIES)]

def edge_request(
    url: str,
    method: str = "GET",
    headers: Optional[Dict[str, str]] = None,
    data: Optional[Any] = None,
    account_index: int = 1,
    timeout: int = 20,
    fallback_direct: bool = True
) -> requests.Response:
    """
    Executes an HTTP request via the Cloudflare Edge Mesh Proxy.
    If the edge proxy fails or times out, optionally falls back to direct request.
    """
    node_url = get_node_for_account(account_index)
    payload = {
        "url": url,
        "method": method.upper(),
        "headers": headers or {},
        "data": data
    }
    
    try:
        resp = requests.post(node_url, json=payload, timeout=timeout)
        if resp.status_code == 200:
            res_data = resp.json()
            if res_data.get("ok"):
                synthetic = requests.Response()
                synthetic.status_code = res_data.get("status", 200)
                synthetic.headers.update(res_data.get("headers", {}))
                raw_content = res_data.get("data", "")
                if isinstance(raw_content, (dict, list)):
                    synthetic._content = json.dumps(raw_content).encode("utf-8")
                else:
                    synthetic._content = str(raw_content).encode("utf-8")
                synthetic.encoding = "utf-8"
                return synthetic
    except Exception:
        pass

    if fallback_direct:
        return requests.request(
            method=method.upper(),
            url=url,
            headers=headers,
            json=data if isinstance(data, (dict, list)) else None,
            data=data if isinstance(data, str) else None,
            timeout=timeout
        )
    raise RuntimeError(f"Edge proxy request failed for {url}")

def stagger_delay(min_sec: float = 2.0, max_sec: float = 4.5):
    """Inserts a humanized jitter delay to prevent concurrent IP burst bans."""
    time.sleep(random.uniform(min_sec, max_sec))
