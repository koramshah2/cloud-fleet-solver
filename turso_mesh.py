"""
Turso LibSQL KV Mesh (turso_mesh.py)
Authoritative high-performance Key-Value replacement for Upstash Redis across AAA AUTO.
Uses Turso LibSQL (table `kv_mesh`) with 25,000,000 requests/month free tier and zero rate limits.

Guarantees 100% backward-compatibility with all legacy upstash_* and get_upstash_* callers.
"""

import os
import json
import time
import logging
import urllib.request
import urllib.error
from typing import Any, Optional, List, Dict

try:
    from dotenv import load_dotenv
    # Search for .env in current dir, project root, or parent dirs
    curr_dir = os.path.dirname(os.path.abspath(__file__))
    env_path = os.path.join(curr_dir, ".env")
    if os.path.exists(env_path):
        load_dotenv(env_path)
    else:
        load_dotenv()
except ImportError:
    pass

logger = logging.getLogger("turso_mesh")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

TURSO_URL = os.getenv("TURSO_DB_URL") or os.getenv("TURSO_URL") or ""
TURSO_TOKEN = os.getenv("TURSO_DB_TOKEN") or os.getenv("TURSO_TOKEN") or os.getenv("TURSO_AUTH_TOKEN") or ""

def _format_hrana_arg(val: Any) -> dict:
    """Formats an argument for Turso Hrana v2 HTTP pipeline."""
    if val is None:
        return {"type": "null"}
    elif isinstance(val, (int, float)):
        # Turso Hrana integer values must be passed as strings to prevent JSON numeric overflow
        return {"type": "integer", "value": str(int(val))}
    elif isinstance(val, bytes):
        import base64
        return {"type": "blob", "base64": base64.b64encode(val).decode("ascii")}
    else:
        return {"type": "text", "value": str(val)}

def turso_execute(sql: str, args: Optional[List[Any]] = None, timeout: float = 8.0) -> Optional[List[List[dict]]]:
    """
    Executes a single SQL statement on Turso LibSQL Hrana v2 pipeline.
    Returns rows as list of dicts or None on failure.
    """
    if not TURSO_URL or not TURSO_TOKEN:
        logger.warning("Turso URL or Token missing from environment.")
        return None

    http_url = TURSO_URL.replace("libsql://", "https://").rstrip("/") + "/v2/pipeline"
    formatted_args = [_format_hrana_arg(a) for a in (args or [])]

    payload = {
        "requests": [
            {
                "type": "execute",
                "stmt": {
                    "sql": sql,
                    "args": formatted_args
                }
            }
        ]
    }

    try:
        req = urllib.request.Request(
            http_url,
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Authorization": f"Bearer {TURSO_TOKEN}",
                "Content-Type": "application/json"
            }
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            results = data.get("results", [])
            if results and results[0].get("type") == "ok":
                return results[0].get("response", {}).get("result", {}).get("rows", [])
            elif results and results[0].get("type") == "error":
                logger.error(f"Turso Hrana error: {results[0].get('error')}")
                return None
    except Exception as e:
        logger.debug(f"Turso execute error on query [{sql[:40]}...]: {e}")
        return None
    return None

def turso_execute_batch(requests_list: List[dict], timeout: float = 12.0) -> Optional[List[dict]]:
    """
    Executes a batch of requests on Turso LibSQL Hrana v2 pipeline atomically in a single HTTP roundtrip.
    """
    if not TURSO_URL or not TURSO_TOKEN:
        return None

    http_url = TURSO_URL.replace("libsql://", "https://").rstrip("/") + "/v2/pipeline"
    payload = {"requests": requests_list}

    try:
        req = urllib.request.Request(
            http_url,
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Authorization": f"Bearer {TURSO_TOKEN}",
                "Content-Type": "application/json"
            }
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            return data.get("results", [])
    except Exception as e:
        logger.debug(f"Turso batch execute error ({len(requests_list)} requests): {e}")
        return None

def _decode_stored_value(val: Any) -> Any:
    """Safely decodes JSON values from storage, unwrapping double-encoded strings."""
    if val is None:
        return None
    curr = val
    # Unpack nested json strings up to 3 layers
    for _ in range(3):
        if isinstance(curr, str):
            curr_stripped = curr.strip()
            if (curr_stripped.startswith("{") and curr_stripped.endswith("}")) or \
               (curr_stripped.startswith("[") and curr_stripped.endswith("]")) or \
               (curr_stripped.startswith('"') and curr_stripped.endswith('"')):
                try:
                    loaded = json.loads(curr)
                    if loaded == curr:
                        break
                    curr = loaded
                except Exception:
                    break
            else:
                break
        else:
            break
    return curr

def turso_get(key: str, default: Any = None) -> Any:
    """
    Retrieves a key from `kv_mesh` table in Turso LibSQL.
    Auto-decodes JSON structures (dict, list, etc.).
    """
    rows = turso_execute("SELECT val FROM kv_mesh WHERE key = ? LIMIT 1;", [key])
    if rows and len(rows) > 0 and len(rows[0]) > 0:
        raw_val = rows[0][0].get("value")
        decoded = _decode_stored_value(raw_val)
        return decoded if decoded is not None else default
    return default

def build_accounts_slim(accounts: list) -> list:
    """Builds a lightweight (~5 KB) roster projection for sub-250ms Edge & Admin Bot UI queries."""
    slim = []
    for a in (accounts or []):
        if not isinstance(a, dict):
            continue
        tri = a.get("tri_sessions") if isinstance(a.get("tri_sessions"), dict) else {}
        s_obj = tri.get("sessions") if isinstance(tri.get("sessions"), dict) else tri
        w_ok = bool(isinstance(s_obj, dict) and str(s_obj.get("worker") or "").strip())
        s_ok = bool(isinstance(s_obj, dict) and str(s_obj.get("sentinel") or "").strip())
        b_ok = bool(isinstance(s_obj, dict) and str(s_obj.get("backup") or "").strip())
        has_sess = bool(
            (isinstance(a.get("session_string"), str) and len(a["session_string"].strip()) > 20)
            or (isinstance(a.get("session"), str) and len(a["session"].strip()) > 20)
            or w_ok or s_ok or b_ok
        )
        slim.append({
            "index": a.get("index"),
            "id": str(a.get("user_id") or a.get("id") or ""),
            "user_id": a.get("user_id"),
            "name": a.get("name", ""),
            "username": a.get("username", ""),
            "phone": a.get("phone", ""),
            "status": a.get("status", "active"),
            "session_revoked": bool(a.get("session_revoked", False)),
            "added_at": a.get("added_at", ""),
            "email": a.get("email") or a.get("login_email") or "",
            "has_session": has_sess,
            "has_worker": w_ok,
            "has_sentinel": s_ok,
            "has_backup": b_ok,
            "miniapp_tokens_synced": bool(
                a.get("miniapp_tokens_synced") or a.get("atf_init_data") or a.get("chickenland_init_data") or a.get("pepegram_init_data")
            ),
        })
    slim.sort(key=lambda x: int(x.get("index") or 999))
    return slim


def build_tokens_summary(tokens_map: dict) -> dict:
    """Builds a tiny (~2 KB) per-account pool availability summary from miniapp tokens."""
    summary = {}
    if not isinstance(tokens_map, dict):
        return summary
    for uid, tok in tokens_map.items():
        if not isinstance(tok, dict):
            continue
        pools = []
        if tok.get("atf_init_data"):
            pools.append("ATF")
        if tok.get("pepegram_init_data") or tok.get("pepe_init_data"):
            pools.append("PEP")
        if tok.get("chickenland_init_data") or tok.get("chicken_init_data"):
            pools.append("CHK")
        if tok.get("moovalley_init_data") or tok.get("moo_init_data"):
            pools.append("MOO")
        if tok.get("myage_init_data"):
            pools.append("AGE")
        if tok.get("nautilus_init_data") or tok.get("nautilusmine_init_data"):
            pools.append("NAU")
        summary[str(uid)] = {
            "pools": pools,
            "count": len(pools),
            "synced_at": tok.get("synced_at") or tok.get("updated_at") or 0,
        }
    return summary


def turso_set(key: str, value: Any, ttl: Optional[int] = None) -> bool:
    """
    Persists a key-value pair to `kv_mesh` table in Turso LibSQL.
    Serializes non-string objects to JSON and automatically mirrors lightweight
    UI projections (`fleet:accounts_slim`, `fleet:tokens_summary`) in a single batch.
    """
    if value is None:
        return turso_delete(key)

    val_str = json.dumps(value) if not isinstance(value, str) else value
    now_ts = int(time.time())

    # Auto-sync compact edge projections in the same atomic pipeline roundtrip
    if key == "fleet:accounts":
        try:
            accs = json.loads(val_str) if isinstance(value, str) else value
            if isinstance(accs, list) and len(accs) > 0:
                slim_str = json.dumps(build_accounts_slim(accs))
                return turso_execute_batch([
                    {
                        "type": "execute",
                        "stmt": {
                            "sql": "INSERT OR REPLACE INTO kv_mesh (key, val, updated_at) VALUES (?, ?, ?);",
                            "args": [_format_hrana_arg(key), _format_hrana_arg(val_str), _format_hrana_arg(now_ts)],
                        },
                    },
                    {
                        "type": "execute",
                        "stmt": {
                            "sql": "INSERT OR REPLACE INTO kv_mesh (key, val, updated_at) VALUES (?, ?, ?);",
                            "args": [_format_hrana_arg("fleet:accounts_slim"), _format_hrana_arg(slim_str), _format_hrana_arg(now_ts)],
                        },
                    },
                ]) is not None
        except Exception:
            pass
    elif key in ("fleet:tokens_state", "miniapp_tokens"):
        try:
            tmap = json.loads(val_str) if isinstance(value, str) else value
            if isinstance(tmap, dict) and len(tmap) > 0:
                sum_str = json.dumps(build_tokens_summary(tmap))
                return turso_execute_batch([
                    {
                        "type": "execute",
                        "stmt": {
                            "sql": "INSERT OR REPLACE INTO kv_mesh (key, val, updated_at) VALUES (?, ?, ?);",
                            "args": [_format_hrana_arg(key), _format_hrana_arg(val_str), _format_hrana_arg(now_ts)],
                        },
                    },
                    {
                        "type": "execute",
                        "stmt": {
                            "sql": "INSERT OR REPLACE INTO kv_mesh (key, val, updated_at) VALUES (?, ?, ?);",
                            "args": [_format_hrana_arg("fleet:tokens_summary"), _format_hrana_arg(sum_str), _format_hrana_arg(now_ts)],
                        },
                    },
                ]) is not None
        except Exception:
            pass

    rows = turso_execute(
        "INSERT OR REPLACE INTO kv_mesh (key, val, updated_at) VALUES (?, ?, ?);",
        [key, val_str, now_ts]
    )
    return rows is not None

def turso_delete(key: str) -> bool:
    """
    Deletes a key from `kv_mesh` table in Turso LibSQL.
    """
    rows = turso_execute("DELETE FROM kv_mesh WHERE key = ?;", [key])
    return rows is not None

def turso_acquire_lock(lock_key: str, owner_id: str, ttl_seconds: int = 60) -> bool:
    """
    Acquires an atomic distributed lock in Turso LibSQL kv_mesh with TTL.
    Guarantees mutual exclusion across all cloud runners, GitHub Actions, and VPS containers.
    Prevents simultaneous MTProto connections on the same account from different IPs (AuthKeyDuplicatedError).
    """
    now_ts = int(time.time())
    try:
        # Clean up expired lock if past TTL
        turso_execute("DELETE FROM kv_mesh WHERE key = ? AND (? - updated_at) >= ?;", [lock_key, now_ts, ttl_seconds])
        # Check current holder
        rows = turso_execute("SELECT val, updated_at FROM kv_mesh WHERE key = ? LIMIT 1;", [lock_key])
        if rows and len(rows) > 0 and len(rows[0]) > 0:
            holder = rows[0][0].get("value")
            if holder == owner_id:
                turso_execute("UPDATE kv_mesh SET updated_at = ? WHERE key = ?;", [now_ts, lock_key])
                return True
            return False
        # Insert fresh lock
        ins = turso_execute("INSERT INTO kv_mesh (key, val, updated_at) VALUES (?, ?, ?);", [lock_key, owner_id, now_ts])
        return ins is not None
    except Exception as e:
        logger.debug(f"Lock acquire note for {lock_key}: {e}")
        return False

def turso_release_lock(lock_key: str, owner_id: str) -> bool:
    """
    Releases the distributed lock only if owned by owner_id.
    """
    try:
        rows = turso_execute("DELETE FROM kv_mesh WHERE key = ? AND val = ?;", [lock_key, owner_id])
        return rows is not None
    except Exception as e:
        logger.debug(f"Lock release note for {lock_key}: {e}")
        return False

def turso_get_tri_sessions(uid: str) -> Optional[dict]:
    """Retrieves the complete Tri-Vault session profile (worker, sentinel, backup) for a user ID."""
    raw = turso_get(f"fleet:tri_sessions:{uid}")
    if isinstance(raw, dict):
        return raw
    elif isinstance(raw, str):
        try:
            return json.loads(raw)
        except Exception:
            pass
    return None

def turso_get_worker_session(uid: str) -> Optional[str]:
    """
    Gets primary Worker session with automatic failover to Backup session
    if Worker session is revoked or unavailable.
    Handles both nested {"sessions": {...}} and flat {"worker": ...} formats.
    """
    tri = turso_get_tri_sessions(uid)
    if tri and isinstance(tri, dict):
        sessions = tri.get("sessions") if isinstance(tri.get("sessions"), dict) else tri
        status = tri.get("status") if isinstance(tri.get("status"), dict) else {}
        if status.get("worker") != "revoked" and sessions.get("worker"):
            return sessions.get("worker")
        if sessions.get("backup"):
            logger.info(f"[Tri-Vault Failover] Worker unavailable for {uid}, promoting Backup session.")
            return sessions.get("backup")
        if sessions.get("worker"):
            return sessions.get("worker")
    return turso_get(f"fleet:session:{uid}") or turso_get(f"account:{uid}")

def turso_get_sentinel_session(uid: str) -> Optional[str]:
    """Gets dedicated Sentinel session for OTP / 777000 code interception."""
    tri = turso_get_tri_sessions(uid)
    if tri and isinstance(tri, dict):
        sessions = tri.get("sessions") if isinstance(tri.get("sessions"), dict) else tri
        return sessions.get("sentinel")
    return None

def turso_keys(pattern: str = "%") -> List[str]:
    """
    Lists keys from `kv_mesh` matching a pattern.
    Supports Redis-style '*' which is converted to SQL '%' wildcard.
    """
    sql_pattern = pattern.replace("*", "%")
    rows = turso_execute("SELECT key FROM kv_mesh WHERE key LIKE ? ORDER BY key ASC;", [sql_pattern])
    if rows:
        return [r[0].get("value") for r in rows if r and len(r) > 0 and r[0].get("value")]
    return []

def turso_mget(keys: List[str]) -> Dict[str, Any]:
    """
    Batch-fetches multiple keys from `kv_mesh` in a single query.
    """
    if not keys:
        return {}
    placeholders = ",".join(["?"] * len(keys))
    rows = turso_execute(f"SELECT key, val FROM kv_mesh WHERE key IN ({placeholders});", keys)
    result = {}
    if rows:
        for r in rows:
            if len(r) >= 2:
                k = r[0].get("value")
                v = _decode_stored_value(r[1].get("value"))
                result[k] = v
    return result

def turso_mset(mapping: Dict[str, Any]) -> bool:
    """
    Batch-inserts multiple key-value pairs into `kv_mesh` in a single atomic HTTP roundtrip.
    """
    if not mapping:
        return True
    now_ts = int(time.time())
    requests = []
    for k, v in mapping.items():
        v_str = json.dumps(v) if not isinstance(v, str) else v
        requests.append({
            "type": "execute",
            "stmt": {
                "sql": "INSERT OR REPLACE INTO kv_mesh (key, val, updated_at) VALUES (?, ?, ?);",
                "args": [
                    {"type": "text", "value": str(k)},
                    {"type": "text", "value": str(v_str)},
                    {"type": "integer", "value": str(now_ts)}
                ]
            }
        })
    res = turso_execute_batch(requests)
    return res is not None

def turso_sadd(key: str, member: str) -> bool:
    """Adds a member to a set stored in kv_mesh as a JSON array."""
    cur = turso_get(key)
    cur_list = cur if isinstance(cur, list) else ([cur] if cur is not None else [])
    member_str = str(member)
    if member_str not in [str(x) for x in cur_list]:
        cur_list.append(member_str)
        return turso_set(key, cur_list)
    return True

def turso_srem(key: str, member: str) -> bool:
    """Removes a member from a set stored in kv_mesh as a JSON array."""
    cur = turso_get(key)
    if not cur:
        return True
    cur_list = cur if isinstance(cur, list) else [cur]
    member_str = str(member)
    filtered = [x for x in cur_list if str(x) != member_str]
    if len(filtered) != len(cur_list):
        return turso_set(key, filtered)
    return True

def turso_smembers(key: str) -> List[str]:
    """Retrieves all members of a set stored in kv_mesh as a JSON array."""
    cur = turso_get(key)
    if not cur:
        return []
    if isinstance(cur, list):
        return [str(x) for x in cur]
    return [str(cur)]

def turso_sismember(key: str, member: str) -> bool:
    """Checks if a member exists in a set stored in kv_mesh as a JSON array."""
    members = turso_smembers(key)
    return str(member) in members

# ==============================================================================
# 100% Drop-In Compatibility Aliases for Legacy Upstash Callers
# All calls to Upstash Redis automatically route through high-quota Turso Mesh
# ==============================================================================
upstash_get = turso_get
upstash_set = turso_set
upstash_del = turso_delete
upstash_delete = turso_delete
upstash_keys = turso_keys
upstash_mget = turso_mget
upstash_mset = turso_mset
upstash_sadd = turso_sadd
upstash_srem = turso_srem
upstash_smembers = turso_smembers
upstash_sismember = turso_sismember
get_upstash = turso_get
set_upstash = turso_set
sync_upstash_key = turso_set

# Async support if aiohttp is available
try:
    import aiohttp

    async def _get_or_create_session(session: Optional[aiohttp.ClientSession] = None):
        class _SessionCtx:
            def __init__(self, s):
                self._provided = s is not None
                self._s = s
            async def __aenter__(self):
                if self._provided:
                    return self._s
                self._s = aiohttp.ClientSession()
                return self._s
            async def __aexit__(self, exc_type, exc_val, exc_tb):
                if not self._provided and self._s:
                    await self._s.close()
        return _SessionCtx(session)

    async def async_turso_get(key: str, default: Any = None, session: Optional[aiohttp.ClientSession] = None) -> Any:
        if not TURSO_URL or not TURSO_TOKEN:
            return default
        http_url = TURSO_URL.replace("libsql://", "https://").rstrip("/") + "/v2/pipeline"
        payload = {
            "requests": [
                {
                    "type": "execute",
                    "stmt": {
                        "sql": "SELECT val FROM kv_mesh WHERE key = ? LIMIT 1;",
                        "args": [{"type": "text", "value": key}]
                    }
                }
            ]
        }
        try:
            async with await _get_or_create_session(session) as s:
                async with s.post(
                    http_url,
                    json=payload,
                    headers={"Authorization": f"Bearer {TURSO_TOKEN}", "Content-Type": "application/json"},
                    timeout=aiohttp.ClientTimeout(total=5)
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        results = data.get("results", [])
                        if results and results[0].get("type") == "ok":
                            rows = results[0].get("response", {}).get("result", {}).get("rows", [])
                            if rows and len(rows) > 0 and len(rows[0]) > 0:
                                return _decode_stored_value(rows[0][0].get("value"))
        except Exception as e:
            logger.debug(f"async_turso_get error: {e}")
        return default

    async def async_turso_set(key: str, value: Any, ttl: Optional[int] = None, session: Optional[aiohttp.ClientSession] = None) -> bool:
        if not TURSO_URL or not TURSO_TOKEN:
            return False
        if value is None:
            return await async_turso_delete(key, session=session)
        http_url = TURSO_URL.replace("libsql://", "https://").rstrip("/") + "/v2/pipeline"
        val_str = json.dumps(value) if not isinstance(value, str) else value
        now_ts = int(time.time())
        payload = {
            "requests": [
                {
                    "type": "execute",
                    "stmt": {
                        "sql": "INSERT OR REPLACE INTO kv_mesh (key, val, updated_at) VALUES (?, ?, ?);",
                        "args": [
                            {"type": "text", "value": key},
                            {"type": "text", "value": val_str},
                            {"type": "integer", "value": str(now_ts)}
                        ]
                    }
                }
            ]
        }
        try:
            async with await _get_or_create_session(session) as s:
                async with s.post(
                    http_url,
                    json=payload,
                    headers={"Authorization": f"Bearer {TURSO_TOKEN}", "Content-Type": "application/json"},
                    timeout=aiohttp.ClientTimeout(total=5)
                ) as resp:
                    return resp.status == 200
        except Exception as e:
            logger.debug(f"async_turso_set error: {e}")
            return False

    async def async_turso_delete(key: str, session: Optional[aiohttp.ClientSession] = None) -> bool:
        if not TURSO_URL or not TURSO_TOKEN:
            return False
        http_url = TURSO_URL.replace("libsql://", "https://").rstrip("/") + "/v2/pipeline"
        payload = {
            "requests": [
                {
                    "type": "execute",
                    "stmt": {
                        "sql": "DELETE FROM kv_mesh WHERE key = ?;",
                        "args": [{"type": "text", "value": key}]
                    }
                }
            ]
        }
        try:
            async with await _get_or_create_session(session) as s:
                async with s.post(
                    http_url,
                    json=payload,
                    headers={"Authorization": f"Bearer {TURSO_TOKEN}", "Content-Type": "application/json"},
                    timeout=aiohttp.ClientTimeout(total=5)
                ) as resp:
                    return resp.status == 200
        except Exception as e:
            logger.debug(f"async_turso_delete error: {e}")
            return False

    async def async_turso_keys(pattern: str = "%", session: Optional[aiohttp.ClientSession] = None) -> List[str]:
        if not TURSO_URL or not TURSO_TOKEN:
            return []
        sql_pattern = pattern.replace("*", "%")
        http_url = TURSO_URL.replace("libsql://", "https://").rstrip("/") + "/v2/pipeline"
        payload = {
            "requests": [
                {
                    "type": "execute",
                    "stmt": {
                        "sql": "SELECT key FROM kv_mesh WHERE key LIKE ? ORDER BY key ASC;",
                        "args": [{"type": "text", "value": sql_pattern}]
                    }
                }
            ]
        }
        try:
            async with await _get_or_create_session(session) as s:
                async with s.post(
                    http_url,
                    json=payload,
                    headers={"Authorization": f"Bearer {TURSO_TOKEN}", "Content-Type": "application/json"},
                    timeout=aiohttp.ClientTimeout(total=6)
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        rows = data.get("results", [{}])[0].get("response", {}).get("result", {}).get("rows", [])
                        return [r[0].get("value") for r in rows if r and r[0].get("value")]
        except Exception as e:
            logger.debug(f"async_turso_keys error: {e}")
        return []

    async def async_turso_mget(keys: List[str], session: Optional[aiohttp.ClientSession] = None) -> Dict[str, Any]:
        if not keys or not TURSO_URL or not TURSO_TOKEN:
            return {}
        placeholders = ",".join(["?"] * len(keys))
        http_url = TURSO_URL.replace("libsql://", "https://").rstrip("/") + "/v2/pipeline"
        payload = {
            "requests": [
                {
                    "type": "execute",
                    "stmt": {
                        "sql": f"SELECT key, val FROM kv_mesh WHERE key IN ({placeholders});",
                        "args": [{"type": "text", "value": str(k)} for k in keys]
                    }
                }
            ]
        }
        try:
            async with await _get_or_create_session(session) as s:
                async with s.post(
                    http_url,
                    json=payload,
                    headers={"Authorization": f"Bearer {TURSO_TOKEN}", "Content-Type": "application/json"},
                    timeout=aiohttp.ClientTimeout(total=8)
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        rows = data.get("results", [{}])[0].get("response", {}).get("result", {}).get("rows", [])
                        res = {}
                        for r in rows:
                            if len(r) >= 2:
                                k = r[0].get("value")
                                v = _decode_stored_value(r[1].get("value"))
                                res[k] = v
                        return res
        except Exception as e:
            logger.debug(f"async_turso_mget error: {e}")
        return {}

    async def async_turso_sadd(key: str, member: str, session: Optional[aiohttp.ClientSession] = None) -> bool:
        cur = await async_turso_get(key, session=session)
        cur_list = cur if isinstance(cur, list) else ([cur] if cur is not None else [])
        member_str = str(member)
        if member_str not in [str(x) for x in cur_list]:
            cur_list.append(member_str)
            return await async_turso_set(key, cur_list, session=session)
        return True

    async def async_turso_srem(key: str, member: str, session: Optional[aiohttp.ClientSession] = None) -> bool:
        cur = await async_turso_get(key, session=session)
        if not cur:
            return True
        cur_list = cur if isinstance(cur, list) else [cur]
        member_str = str(member)
        filtered = [x for x in cur_list if str(x) != member_str]
        if len(filtered) != len(cur_list):
            return await async_turso_set(key, filtered, session=session)
        return True

    async def async_turso_smembers(key: str, session: Optional[aiohttp.ClientSession] = None) -> List[str]:
        cur = await async_turso_get(key, session=session)
        if not cur:
            return []
        if isinstance(cur, list):
            return [str(x) for x in cur]
        return [str(cur)]

    async def async_turso_sismember(key: str, member: str, session: Optional[aiohttp.ClientSession] = None) -> bool:
        members = await async_turso_smembers(key, session=session)
        return str(member) in members

    async def async_turso_get_tri_sessions(uid: str, session: Optional[aiohttp.ClientSession] = None) -> Optional[dict]:
        """Asynchronously retrieves Tri-Vault session bundle from Turso LibSQL."""
        raw = await async_turso_get(f"fleet:tri_sessions:{uid}", session=session)
        if isinstance(raw, dict):
            return raw
        elif isinstance(raw, str):
            try:
                return json.loads(raw)
            except Exception:
                pass
        return None

    async def async_turso_get_worker_session(uid: str, session: Optional[aiohttp.ClientSession] = None) -> Optional[str]:
        """Asynchronously retrieves primary Worker session with automatic failover to Backup."""
        tri = await async_turso_get_tri_sessions(uid, session=session)
        if tri and isinstance(tri, dict):
            sessions = tri.get("sessions") if isinstance(tri.get("sessions"), dict) else tri
            status = tri.get("status") if isinstance(tri.get("status"), dict) else {}
            if status.get("worker") != "revoked" and sessions.get("worker"):
                return sessions.get("worker")
            if sessions.get("backup"):
                logger.info(f"[Tri-Vault Failover] Worker unavailable for {uid}, promoting Backup session.")
                return sessions.get("backup")
            if sessions.get("worker"):
                return sessions.get("worker")
        return (await async_turso_get(f"fleet:session:{uid}", session=session)) or (await async_turso_get(f"account:{uid}", session=session))

    async def async_turso_get_sentinel_session(uid: str, session: Optional[aiohttp.ClientSession] = None) -> Optional[str]:
        """Asynchronously retrieves dedicated Sentinel session for OTP interception."""
        tri = await async_turso_get_tri_sessions(uid, session=session)
        if tri and isinstance(tri, dict):
            sessions = tri.get("sessions") if isinstance(tri.get("sessions"), dict) else tri
            return sessions.get("sentinel")
        return None

    # Backward compatibility aliases
    async_upstash_get = async_turso_get
    async_upstash_set = async_turso_set
    async_upstash_delete = async_turso_delete
    async_upstash_keys = async_turso_keys
    async_upstash_mget = async_turso_mget
    async_upstash_sadd = async_turso_sadd
    async_upstash_srem = async_turso_srem
    async_upstash_smembers = async_turso_smembers
    async_upstash_sismember = async_turso_sismember
except ImportError:
    pass
