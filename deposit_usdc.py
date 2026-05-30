"""
deposit_usdc.py — 完整充值流程（纯 Python，无需 web3）

  USDC (EOA) → wrap() → pUSD → approve(CLOB Exchange V2) → sync cache

依赖:
    pip install httpx eth-abi fastecdsa eth_hash

用法:
    python deposit_usdc.py <private_key> [usdc_amount] [--no-sync]

示例:
    python deposit_usdc.py 0xabc123... 10.5
    python deposit_usdc.py 0xabc123... 10.5 --no-sync
    python deposit_usdc.py 0xabc123...          # 查询余额后全部充值

环境变量（可选）:
    POLYGON_RPC_URL   RPC 节点（默认 https://polygon-rpc.com）
    CLOB_HOST         CLOB API 地址（默认 https://clob.polymarket.com）
    API_KEY           CLOB L2 api_key（不提供则跳过 cache sync）
    API_SECRET        CLOB L2 api_secret
    API_PASSPHRASE    CLOB L2 api_passphrase
"""

import sys
import os
import time
import argparse
from decimal import Decimal

# Load .env if present (only affects POLYGON_RPC_URL)
_dotenv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
if os.path.exists(_dotenv_path):
    for line in open(_dotenv_path):
        line = line.strip()
        if "=" in line and not line.startswith("#"):
            k, _, v = line.partition("=")
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

# ── 配置 ────────────────────────────────────────────────────────
POLYGON_RPC_URLS = [
    os.getenv("POLYGON_RPC_URL") or "https://polygon-bor.publicnode.com",
    "https://polygon-bor.publicnode.com",
    "https://polygon-rpc.com",
    "https://polygon.publicnode.com",
    "https://1rpc.io/matic",
]
COLLATERAL_ONRAMP    = "0x93070a847efEf7F70739046A929D47a521F5B8ee"
POLYGON_USDC_NATIVE  = "0x3c499c542cEF5E3811e1192CE70d8cC03d5c3359"
POLYGON_PUSD         = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"
CLOB_EXCHANGE_V2     = "0xE111180000d2663C0091e4f400237545B87B996B"
CHAIN_ID             = 137
CLOB_HOST            = os.getenv("CLOB_HOST", "https://clob.polymarket.com")
# ────────────────────────────────────────────────────────────────

import httpx
import eth_abi

# ── Keccak-256 ─────────────────────────────────────────────────
try:
    from eth_hash.auto import keccak as _keccak
except ImportError:
    from eth_utils import keccak as _keccak

def _sel(sig: str) -> bytes:
    return _keccak(sig.encode())[:4]

_APPROVE_SEL = _sel("approve(address,uint256)")
_WRAP_SEL    = _sel("wrap(address,address,uint256)")
_BALANCE_SEL = _sel("balanceOf(address)")

# ── RPC ────────────────────────────────────────────────────────
_rpc_idx = 0

def _rpc(method: str, params: list = None) -> str:
    global _rpc_idx
    for i in range(len(POLYGON_RPC_URLS)):
        url = POLYGON_RPC_URLS[(_rpc_idx + i) % len(POLYGON_RPC_URLS)]
        try:
            r = httpx.post(url,
                           json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params or []},
                           timeout=15)
            r.raise_for_status()
            j = r.json()
            err = j.get("error")
            if err:
                raise RuntimeError(f"RPC error: {err}")
            result = j.get("result")
            if result is None:
                raise RuntimeError(f"RPC returned null result")
            if i > 0:
                print(f"  [OK] switched to: {url}")
                _rpc_idx = (_rpc_idx + i) % len(POLYGON_RPC_URLS)
            return result
        except Exception as exc:
            print(f"  [WARN] {url}: {exc}")
    raise RuntimeError("All RPC endpoints failed")

def _from_hex(h: str) -> int:
    h = h.lstrip("0x") or "0"
    return int(h, 16) if h != "0" else 0

def _pad32(b: bytes) -> bytes:
    return b.rjust(32, b"\x00")

def get_balance(holder: str, token: str = POLYGON_USDC_NATIVE) -> int:
    data = "0x" + _BALANCE_SEL.hex() + eth_abi.encode(["address"], [holder.lower()]).hex()
    return _from_hex(_rpc("eth_call",
                           [{"to": token.lower(), "data": data}, "latest"]))

def get_nonce(addr: str) -> int:
    try:
        return int(_rpc("eth_getTransactionCount", [addr.lower(), "pending"]), 16)
    except Exception:
        print(f"  [WARN] pending nonce failed, trying latest")
        return int(_rpc("eth_getTransactionCount", [addr.lower(), "latest"]), 16)

def get_gas_price() -> int:
    try:
        result = _rpc("eth_gasPrice")
        return int(result, 16) if result.startswith("0x") else int(result)
    except Exception:
        print(f"  [WARN] eth_gasPrice failed, using 35 gwei")
        return 35_000_000_000

def estimate_gas(tx: dict) -> int:
    rpc_tx = {
        "from": tx["from"],
        "to": tx["to"],
        "value": hex(tx["value"]),
        "data": tx["data"],
    }
    try:
        result = _rpc("eth_estimateGas", [rpc_tx])
        return max(int(result, 16) if result.startswith("0x") else int(result), 21_000)
    except RuntimeError as e:
        # estimateGas reverted — parse revert data from error
        msg = str(e)
        import re
        m = re.search(r"'data': '([^']+)'", msg)
        if m:
            raw = m.group(1)
            reason = _parse_revert(raw)
            print(f"  [DEBUG] eth_estimateGas reverted: {reason}")
        else:
            print(f"  [DEBUG] eth_estimateGas reverted: {e}")
        return 200_000

def send_raw_tx(raw: str) -> str:
    return _rpc("eth_sendRawTransaction", [raw])

def wait_receipt(tx_hash: str, timeout: int = 120) -> dict:
    for _ in range(timeout // 2):
        time.sleep(2)
        r = httpx.post(POLYGON_RPC_URLS[_rpc_idx % len(POLYGON_RPC_URLS)],
                       json={"jsonrpc": "2.0", "id": 1,
                             "method": "eth_getTransactionReceipt",
                             "params": [tx_hash]},
                       timeout=30).json()
        result = r.get("result")
        if result and result.get("blockNumber"):
            return result
    raise RuntimeError(f"tx {tx_hash} not confirmed after {timeout}s")

def get_chain_id() -> int:
    try:
        return _from_hex(_rpc("eth_chainId"))
    except Exception:
        return CHAIN_ID

# ── Calldata ────────────────────────────────────────────────────
def _build_approve(spender: str, amount: int) -> str:
    enc = eth_abi.encode(["address", "uint256"], [spender.lower(), amount])
    return "0x" + _APPROVE_SEL.hex() + enc.hex()

def _build_wrap(asset: str, recipient: str, amount: int) -> str:
    enc = eth_abi.encode(["address", "address", "uint256"],
                         [asset.lower(), recipient.lower(), amount])
    return "0x" + _WRAP_SEL.hex() + enc.hex()

def _try_call(to: str, data: str, value: int = 0) -> str:
    """Try eth_call to see raw revert data. Returns hex result or error string."""
    try:
        return _rpc("eth_call", [{"to": to, "data": data, "value": hex(value)}, "latest"])
    except Exception as e:
        return f"call failed: {e}"

    # ── 地址推导 & 签名（使用 eth-account，无需编译） ───────────────
from eth_account import Account
from eth_utils import to_checksum_address

def derive_address(pk_hex: str) -> str:
    return Account.from_key(pk_hex).address

def sign_transaction(
    nonce: int, to: str, data: str, value: int,
    gas: int, gas_price: int, chain_id: int, pk_hex: str,
) -> str:
    tx = {
        "nonce": nonce,
        "gasPrice": gas_price,
        "gas": gas,
        "to": to_checksum_address(to),
        "value": value,
        "data": data,
        "chainId": chain_id,
        # Force EIP-155 so Polygon chain_id 137 is valid
        "type": 1,
    }
    signed = Account.sign_transaction(tx, pk_hex)
    return "0x" + signed.raw_transaction.hex()

# ── 发送交易 ────────────────────────────────────────────────────
def send_tx(wallet: str, pk: str, to: str, data: str, value: int = 0,
            gas_fixed: int = 0) -> str:
    cid        = get_chain_id()
    nonce      = get_nonce(wallet)
    gas_price  = get_gas_price()
    unsigned   = {"from": wallet, "to": to, "data": data, "value": value}
    gas_limit = gas_fixed if gas_fixed else int(estimate_gas(unsigned) * 1.3)
    print(f"  nonce={nonce} gas_limit={gas_limit} gas_price={gas_price}")

    raw = sign_transaction(
        nonce=nonce, to=to, data=data, value=value,
        gas=gas_limit, gas_price=gas_price, chain_id=cid, pk_hex=pk,
    )
    tx_hash = send_raw_tx(raw)
    print(f"  tx sent: {tx_hash}")
    receipt = wait_receipt(tx_hash)
    status = int(receipt.get("status", "0x0"), 16)
    if status != 1:
        revert_data = receipt.get("revertData", "")
        revert_reason = _parse_revert(revert_data)
        raise RuntimeError(f"tx FAILED (status={status}): {tx_hash} — {revert_reason}")
    print(f"  confirmed in block {receipt['blockNumber']}")
    return tx_hash

# ── Revert reason parser ───────────────────────────────────────
_REVERT_SELECTOR = bytes.fromhex("08c379a0")  # Error(string)

def _parse_revert(data: str) -> str:
    if not data or len(data) < 10:
        return data or "(no revert data)"
    try:
        d = bytes.fromhex(data[2:] if data.startswith("0x") else data)
        if d[:4] == _REVERT_SELECTOR:
            # Error(string) — skip 4-byte selector + 3 words (offset=32, length=32, ...)
            # After selector: 32-byte offset (should be 32), 32-byte string length, then raw bytes
            pos = 4 + 32  # skip selector + offset
            str_len = int.from_bytes(d[pos:pos+32], "big")
            pos += 32
            return "revert: " + d[pos:pos+str_len].decode("utf-8", errors="replace")
    except Exception:
        pass
    return f"raw: {data[:20]}..."

# ── CLOB cache sync ─────────────────────────────────────────────
def sync_clob_cache(private_key: str) -> bool:
    api_key      = os.getenv("API_KEY",      "").strip()
    api_secret   = os.getenv("API_SECRET",   "").strip()
    api_passphrase = os.getenv("API_PASSPHRASE", "").strip()

    if not (api_key and api_secret and api_passphrase):
        print("\n[Step 3] SKIP — API_KEY/API_SECRET/API_PASSPHRASE not set")
        print("         After on-chain steps complete, sync manually via dashboard")
        return False

    print(f"\n[Step 3] Syncing CLOB balance cache...")
    try:
        from app.clob_l1_auth import build_l2_auth_headers
    except ImportError:
        print("  [WARN] app.clob_l1_auth not in path, trying standalone HMAC...")
        # Inline minimal L2 auth
        api_key      = os.getenv("API_KEY",       "").strip()
        api_secret   = os.getenv("API_SECRET",    "").strip()
        api_passphrase = os.getenv("API_PASSPHRASE", "").strip()
        if not all([api_key, api_secret, api_passphrase]):
            print("  [SKIP] L2 credentials not configured")
            return False
        try:
            from py_clob_client_v2.headers.headers import create_level_2_headers
            from py_clob_client_v2.clob_types import ApiCreds
            import time as _time
            ts  = str(int(_time.time() * 1000))
            nonce = os.urandom(16).hex()
            creds = ApiCreds(api_key=api_key, api_secret=api_secret, api_passphrase=api_passphrase)
            headers = create_level_2_headers(
                creds=creds,
                method="GET",
                path="/balance-allowance/update",
                body="",
                timestamp=ts,
                nonce=nonce,
            )
        except ImportError:
            print("  [WARN] py_clob_client_v2 not installed — skipping cache sync")
            return False

    try:
        headers = build_l2_auth_headers(
            private_key=private_key,
            host=CLOB_HOST,
            api_key=api_key,
            api_secret=api_secret,
            api_passphrase=api_passphrase,
            method="GET",
            path="/balance-allowance/update",
        )
    except Exception as exc:
        print(f"  [WARN] build_l2_auth_headers failed: {exc}")
        return False

    try:
        r = httpx.get(
            CLOB_HOST + "/balance-allowance/update",
            headers=headers,
            timeout=20,
        )
        if r.status_code in (200, 201):
            print(f"  CLOB cache synced OK: {r.status_code}")
            return True
        else:
            print(f"  [WARN] CLOB cache sync returned {r.status_code}: {r.text[:200]}")
            return False
    except Exception as exc:
        print(f"  [WARN] CLOB cache sync request failed: {exc}")
        return False

# ── 主流程 ──────────────────────────────────────────────────────
def deposit_all(private_key: str, usdc_amount: float, skip_sync: bool = False) -> None:
    wallet = derive_address(private_key)
    amount_raw = int(Decimal(str(usdc_amount)) * Decimal("1_000_000"))
    print(f"\nWallet : {wallet}")
    print(f"Amount : {usdc_amount} USDC ({amount_raw} raw)")

    # 实际链上余额校验
    actual = get_balance(wallet)
    print(f"Chain USDC balance: {actual / 1_000_000:.6f} (raw={actual})")
    if actual < amount_raw:
        raise RuntimeError(
            f"USDC 余额不足：需要 {amount_raw}，链上实际 {actual}"
        )

    # ── Step 0: Approve CollateralOnramp to pull USDC ────────────
    print("\n[Step 0] USDC.approve() — allow CollateralOnramp to pull USDC ...")
    tx0 = send_tx(wallet, private_key, POLYGON_USDC_NATIVE,
                  _build_approve(COLLATERAL_ONRAMP, 2**96), gas_fixed=50000)
    print(f"  approve OK: https://polygonscan.com/tx/{tx0}")

    # ── Step 1: Wrap USDC → pUSD ──────────────────────────────
    print("\n[Step 1] CollateralOnramp.wrap() — USDC → pUSD ...")
    wrap_data = _build_wrap(POLYGON_USDC_NATIVE, wallet, amount_raw)
    tx1 = send_tx(wallet, private_key, COLLATERAL_ONRAMP, wrap_data, gas_fixed=300000)
    print(f"  wrap OK: https://polygonscan.com/tx/{tx1}")

    # ── Step 2: Approve CLOB Exchange V2 to spend pUSD ────────
    print("\n[Step 2] pUSD.approve() — allow CLOB Exchange V2 to pull pUSD ...")
    tx2 = send_tx(wallet, private_key, POLYGON_PUSD,
                  _build_approve(CLOB_EXCHANGE_V2, amount_raw))
    print(f"  approve OK: https://polygonscan.com/tx/{tx2}")

    # ── Step 3: Sync CLOB cache ──────────────────────────────
    if skip_sync:
        print("\n[Step 3] SKIPPED (--no-sync)")
    else:
        sync_clob_cache(private_key)

    print("\n[ALL DONE] pUSD is now ready for CLOB trading")

def main():
    parser = argparse.ArgumentParser(description="Deposit USDC to Polymarket CLOB")
    parser.add_argument("private_key", nargs="?", help="Wallet private key (0x prefix optional)")
    parser.add_argument("usdc_amount", nargs="?", type=float, help="USDC amount (default: full balance)")
    parser.add_argument("--no-sync", action="store_true", help="Skip CLOB cache sync")
    args = parser.parse_args()

    if not args.private_key:
        print(__doc__)
        sys.exit(1)

    pk = args.private_key.strip()
    if not pk.startswith("0x"):
        pk = "0x" + pk

    if args.usdc_amount is None:
        wallet = derive_address(pk)
        bal = get_balance(wallet)
        if bal <= 0:
            print(f"[ERROR] Wallet {wallet} has 0 native USDC")
            sys.exit(1)
        usdc_amount = bal / 1_000_000
        print(f"Wallet USDC balance: {usdc_amount:.6f} (raw={bal})")
    else:
        usdc_amount = args.usdc_amount

    print(f"\n=== Deposit {usdc_amount} USDC → pUSD → CLOB ===")
    try:
        deposit_all(pk, usdc_amount, skip_sync=args.no_sync)
    except RuntimeError as exc:
        print(f"\n[FAILED] {exc}")
        sys.exit(1)

if __name__ == "__main__":
    main()
