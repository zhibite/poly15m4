"""
polygon_usdc.py — Polygon PoS pUSD + USDC.e + native USDC balance via JSON-RPC eth_call.

v2 Collateral (v2, post-migration):
  - pUSD (Polymarket USD) is the standard collateral token on Polygon.
  - USDC.e is still supported for backward compat but pUSD is preferred.
  - Wrapping: use Collateral Onramp contract wrap() function.
"""

from dataclasses import dataclass
from decimal import ROUND_DOWN, Decimal
import logging

import httpx
from eth_abi import encode
from eth_utils import to_checksum_address

logger = logging.getLogger(__name__)

# V2 token addresses (Polygon mainnet) — shared with live_trader.py
POLYGON_PUSD         = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"   # pUSD (ERC-20)
POLYGON_USDC_E       = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"   # bridged USDC
POLYGON_USDC_NATIVE  = "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359"  # Circle USDC
CTF_ADDRESS          = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
NEG_RISK_ADAPTER     = "0xe2222d279d744050d28e00520010520000310F59"
COLLATERAL_ONRAMP    = "0x93070a847efEf7F70739046A929D47a521F5B8ee"
CLOB_EXCHANGE_V2      = "0xE111180000d2663C0091e4f400237545B87B996B"  # CTF Exchange V2 (pUSD spender)

# ABI function signatures (4-byte selectors)
_BALANCE_OF_SELECTOR = bytes.fromhex("70a08231")
_TRANSFER_SELECTOR   = bytes.fromhex("a9059cbb")  # transfer(address,uint256)
_APPROVE_SELECTOR    = bytes.fromhex("095ea7b3")
_WRAP_SELECTOR       = bytes.fromhex("62355638")   # wrap(address,address,uint256) — 3 params


def _balance_of_calldata(holder: str) -> str:
    word = encode(["address"], [to_checksum_address(holder)])
    return "0x" + _BALANCE_OF_SELECTOR.hex() + word.hex()


def _erc20_balance_raw(holder: str, token: str, rpc_url: str, *, timeout: float = 12.0) -> int | None:
    url = (rpc_url or "").strip()
    if not url:
        return None
    data = _balance_of_calldata(holder)
    payload = {
        "jsonrpc": "2.0", "id": 1, "method": "eth_call",
        "params": [{"to": to_checksum_address(token), "data": data}, "latest"],
    }
    try:
        r = httpx.post(url, json=payload, timeout=timeout)
        r.raise_for_status()
        j = r.json()
        if j.get("error"):
            return None
        hexval = j.get("result")
        if not hexval or not isinstance(hexval, str):
            return None
        if hexval == "0x":
            return 0
        return int(hexval, 16)
    except Exception:
        return None


@dataclass(frozen=True)
class PolygonUsdcOnChain:
    usdc_e_raw: int | None
    usdc_native_raw: int | None

    @property
    def total_raw(self) -> int | None:
        if self.usdc_e_raw is None and self.usdc_native_raw is None:
            return None
        return (self.usdc_e_raw or 0) + (self.usdc_native_raw or 0)


def polygon_usdc_combined_balances(holder: str, rpc_url: str, *, timeout: float = 12.0) -> PolygonUsdcOnChain:
    """Returns USDC.e + native USDC on-chain balances."""
    e = _erc20_balance_raw(holder, POLYGON_USDC_E, rpc_url, timeout=timeout)
    n = _erc20_balance_raw(holder, POLYGON_USDC_NATIVE, rpc_url, timeout=timeout)
    return PolygonUsdcOnChain(usdc_e_raw=e, usdc_native_raw=n)


def _erc20_call(method: str, token: str, rpc_url: str, calldata: str, *, timeout: float = 12.0) -> str | None:
    """Generic eth_call to any ERC-20 function. Returns hex result or None."""
    url = (rpc_url or "").strip()
    if not url:
        return None
    payload = {
        "jsonrpc": "2.0", "id": 1, "method": "eth_call",
        "params": [{"to": to_checksum_address(token), "data": calldata}, "latest"],
    }
    try:
        r = httpx.post(url, json=payload, timeout=timeout)
        r.raise_for_status()
        j = r.json()
        if j.get("error"):
            return None
        return j.get("result")
    except Exception:
        return None


def _build_approve_calldata(spender: str, amount_raw: int) -> str:
    """Build approve() calldata."""
    word = encode(["address", "uint256"], [to_checksum_address(spender), amount_raw])
    return "0x" + _APPROVE_SELECTOR.hex() + word.hex()


def _build_transfer_calldata(to: str, amount_raw: int) -> str:
    """Build transfer() calldata."""
    word = encode(["address", "uint256"], [to_checksum_address(to), amount_raw])
    return "0x" + _TRANSFER_SELECTOR.hex() + word.hex()


def _build_wrap_calldata(asset: str, recipient: str, amount_raw: int) -> str:
    """Build CollateralOnramp.wrap(asset, recipient, amount) calldata."""
    word = encode(["address", "address", "uint256"],
                  [to_checksum_address(asset), to_checksum_address(recipient), amount_raw])
    return "0x" + _WRAP_SELECTOR.hex() + word.hex()


def revoke_allowance(
    private_key: str,
    rpc_url: str,
    token: str,
    spender: str,
) -> dict:
    """
    Revoke a spender's ERC-20 allowance by approving 0.
    Used as a safety rollback when a previous approve() succeeded but the
    subsequent operation (e.g. wrap) failed, leaving a lingering approval.

    Returns:
        dict with keys: success (bool), tx_hash (str), error (str)
    """
    from web3 import Web3

    w3 = Web3(Web3.HTTPProvider(rpc_url))
    if not w3.is_connected():
        return {"success": False, "tx_hash": "", "error": "Web3 not connected"}

    try:
        account = w3.eth.account.from_key(private_key)
    except Exception as exc:
        return {"success": False, "tx_hash": "", "error": f"bad private key: {exc}"}

    wallet = account.address
    # Approve 0 to revoke the spender's allowance
    revoke_calldata = _build_approve_calldata(to_checksum_address(spender), 0)
    try:
        tx_hash = _send_raw_tx(w3, token, revoke_calldata, wallet, private_key)
        return {"success": True, "tx_hash": tx_hash, "error": ""}
    except Exception as exc:
        return {"success": False, "tx_hash": "", "error": f"revoke failed: {exc}"}


def _send_raw_tx(w3, to: str, data: str, from_addr: str, private_key: str) -> str:
    """Sign and send a raw transaction, return tx hash."""
    import threading
    nonce_lock = getattr(w3, "_nonce_lock", None)
    if nonce_lock is None:
        nonce_lock = threading.Lock()
        w3._nonce_lock = nonce_lock
    with nonce_lock:
        nonce = w3.eth.get_transaction_count(from_addr)
        gas_price = w3.eth.gas_price
        tx = {
            "to": to,
            "data": data,
            "value": 0,
            "gas": 200000,
            "maxFeePerGas": gas_price,
            "maxPriorityFeePerGas": w3.eth.max_priority_fee,
            "nonce": nonce,
            "chainId": 137,
        }
        tx["gas"] = int(w3.eth.estimate_gas(tx) * 1.3)
        signed = w3.eth.account.sign_transaction(tx, private_key)
        tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
    receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
    if receipt["status"] != 1:
        raise RuntimeError(f"tx failed: {tx_hash.hex()}")
    return tx_hash.hex()


def _allowance_raw(holder: str, spender: str, token: str, rpc_url: str) -> int:
    """Returns allowance in raw units (0 if error)."""
    word = encode(["address", "address"], [to_checksum_address(holder), to_checksum_address(spender)])
    # allowance(address owner, address spender) — 4-byte selector: keccak256("allowance(address,address)")[0:4]
    allowance_sel = bytes.fromhex("dd62ed3e")
    calldata = "0x" + allowance_sel.hex() + word.hex()
    result = _erc20_call("allowance", token, rpc_url, calldata)
    if not result or result == "0x":
        return 0
    try:
        return int(result, 16)
    except Exception:
        return 0


def polygon_pusd_balance_micro(holder: str, rpc_url: str, *, timeout: float = 12.0) -> int | None:
    """Returns pUSD on-chain balance in raw micro-units."""
    return _erc20_balance_raw(holder, POLYGON_PUSD, rpc_url, timeout=timeout)


def wrap_usdc_e_to_pusd(
    private_key: str,
    rpc_url: str,
    usdc_e_amount: float,
    *,
    onramp_address: str = COLLATERAL_ONRAMP,
) -> dict:
    """
    Wrap USDC.e into pUSD via the Collateral Onramp contract.

    Steps:
      1. Approve CollateralOnramp to pull USDC.e from caller.
      2. Call CollateralOnramp.wrap(recipient=caller, amount).

    Args:
        private_key:  Wallet private key (0x-prefixed).
        rpc_url:      Polygon JSON-RPC URL.
        usdc_e_amount: Amount of USDC.e to wrap (in standard units, e.g. 10.0).

    Returns:
        dict with keys: success (bool), tx_hash (str), error (str)
    """
    from web3 import Web3

    w3 = Web3(Web3.HTTPProvider(rpc_url))
    if not w3.is_connected():
        return {"success": False, "tx_hash": "", "error": "Web3 not connected"}

    try:
        account = w3.eth.account.from_key(private_key)
    except Exception as exc:
        return {"success": False, "tx_hash": "", "error": f"bad private key: {exc}"}

    wallet = account.address
    amount_raw = int(Decimal(str(usdc_e_amount)) * Decimal("1_000_000"))

    # Step 1: approve USDC.e → CollateralOnramp
    approve_calldata = _build_approve_calldata(onramp_address, amount_raw)
    try:
        _send_raw_tx(w3, POLYGON_USDC_E, approve_calldata, wallet, private_key)
    except Exception as exc:
        return {"success": False, "tx_hash": "", "error": f"approve USDC.e failed: {exc}"}

    # Step 2: wrap USDC.e → pUSD
    wrap_calldata = _build_wrap_calldata(POLYGON_USDC_E, wallet, amount_raw)
    try:
        tx_hash = _send_raw_tx(w3, onramp_address, wrap_calldata, wallet, private_key)
        return {"success": True, "tx_hash": tx_hash, "error": ""}
    except Exception as exc:
        # CRITICAL FIX (R7): wrap failed — revoke the approval to avoid leaving a lingering
        # allowance. Without this, the CollateralOnramp could spend the approved USDC.e
        # in a subsequent call (e.g. if the bot retries the same amount).
        logger.warning("wrap failed: {} — revoking USDC.e approval to prevent dangling allowance", exc)
        try:
            revoke_calldata = _build_approve_calldata(onramp_address, 0)
            _send_raw_tx(w3, POLYGON_USDC_E, revoke_calldata, wallet, private_key)
            logger.info("USDC.e approval revoked after failed wrap")
        except Exception as revoke_exc:
            logger.error("Failed to revoke USDC.e approval after wrap failure: {}", revoke_exc)
        return {"success": False, "tx_hash": "", "error": f"wrap failed: {exc}"}


def wrap_usdc_native_to_pusd(
    private_key: str,
    rpc_url: str,
    usdc_amount: float,
    *,
    onramp_address: str = COLLATERAL_ONRAMP,
) -> dict:
    """
    Wrap native USDC (Circle USDC) into pUSD via the Collateral Onramp contract.

    Steps:
      1. Approve CollateralOnramp to pull native USDC from caller.
      2. Call CollateralOnramp.wrap(native_usdc_addr, recipient, amount).

    Args:
        private_key:  Wallet private key (0x-prefixed).
        rpc_url:      Polygon JSON-RPC URL.
        usdc_amount:  Amount of native USDC to wrap (in standard units, e.g. 10.0).

    Returns:
        dict with keys: success (bool), tx_hash (str), error (str)
    """
    from web3 import Web3

    w3 = Web3(Web3.HTTPProvider(rpc_url))
    if not w3.is_connected():
        return {"success": False, "tx_hash": "", "error": "Web3 not connected"}

    try:
        account = w3.eth.account.from_key(private_key)
    except Exception as exc:
        return {"success": False, "tx_hash": "", "error": f"bad private key: {exc}"}

    wallet = account.address
    amount_raw = int(Decimal(str(usdc_amount)) * Decimal("1_000_000"))
    logger.info("wrap_usdc_native_to_pusd: wallet={} amount={:.6f} raw={}", wallet, usdc_amount, amount_raw)

    # Step 1: approve native USDC → CollateralOnramp
    approve_calldata = _build_approve_calldata(onramp_address, amount_raw)
    try:
        _send_raw_tx(w3, POLYGON_USDC_NATIVE, approve_calldata, wallet, private_key)
        logger.info("wrap_usdc_native_to_pusd: approve tx sent")
    except Exception as exc:
        logger.error("wrap_usdc_native_to_pusd: approve failed: {}", exc)
        return {"success": False, "tx_hash": "", "error": f"approve native USDC failed: {exc}"}

    # Step 2: wrap native USDC → pUSD
    wrap_calldata = _build_wrap_calldata(POLYGON_USDC_NATIVE, wallet, amount_raw)
    try:
        tx_hash = _send_raw_tx(w3, onramp_address, wrap_calldata, wallet, private_key)
        logger.info("wrap_usdc_native_to_pusd: wrap tx confirmed: {}", tx_hash)
        return {"success": True, "tx_hash": tx_hash, "error": ""}
    except Exception as exc:
        # CRITICAL FIX (R7): wrap failed — revoke the approval to avoid leaving a lingering
        # allowance. Without this, the CollateralOnramp could spend the approved native USDC
        # in a subsequent call.
        logger.warning("wrap_usdc_native_to_pusd: wrap failed: {} — revoking approval", exc)
        try:
            revoke_calldata = _build_approve_calldata(onramp_address, 0)
            _send_raw_tx(w3, POLYGON_USDC_NATIVE, revoke_calldata, wallet, private_key)
            logger.info("wrap_usdc_native_to_pusd: native USDC approval revoked after failed wrap")
        except Exception as revoke_exc:
            logger.error("Failed to revoke native USDC approval after wrap failure: {}", revoke_exc)
        return {"success": False, "tx_hash": "", "error": f"wrap failed: {exc}"}


def approve_pusd_to_exchange(
    private_key: str,
    rpc_url: str,
    amount: float,
    *,
    pusd_address: str = POLYGON_PUSD,
    exchange_address: str = CLOB_EXCHANGE_V2,
) -> dict:
    """
    Approve the CLOB Exchange V2 contract to spend pUSD on behalf of the wallet.
    For EOA mode only — Safe/Proxy does not need this step.

    Returns:
        dict with keys: success (bool), tx_hash (str), error (str)
    """
    from web3 import Web3

    w3 = Web3(Web3.HTTPProvider(rpc_url))
    if not w3.is_connected():
        return {"success": False, "tx_hash": "", "error": "Web3 not connected"}

    try:
        account = w3.eth.account.from_key(private_key)
    except Exception as exc:
        return {"success": False, "tx_hash": "", "error": f"bad private key: {exc}"}

    wallet = account.address
    amount_raw = int(Decimal(str(amount)) * Decimal("1_000_000"))
    approve_calldata = _build_approve_calldata(exchange_address, amount_raw)
    try:
        tx_hash = _send_raw_tx(w3, pusd_address, approve_calldata, wallet, private_key)
        return {"success": True, "tx_hash": tx_hash, "error": ""}
    except Exception as exc:
        return {"success": False, "tx_hash": "", "error": f"approve pUSD failed: {exc}"}


def sync_clob_balance_cache(
    private_key: str,
    host: str,
    api_key: str,
    api_secret: str,
    api_passphrase: str,
    signature_type: int,
    amount: float = 0.0,
) -> dict:
    """
    Trigger a CLOB balance cache refresh via /balance-allowance/update.
    Uses L2 HMAC auth (requires L1 signer for EOA mode).

    Args:
        private_key:     Wallet private key.
        host:            CLOB API host (e.g. https://clob.polymarket.com).
        api_key, api_secret, api_passphrase: CLOB L2 credentials.
        signature_type:   0=EOA, 1=Proxy, 2=Safe.
        amount:          Amount of pUSD to sync (standard units, e.g. 10.0).
                        Pass 0 to just trigger a cache refresh without amount hint.

    Returns:
        dict with keys: ok (bool), error (str)
    """
    from app.clob_l1_auth import build_l2_auth_headers
    import httpx

    amount_raw = int(Decimal(str(amount)) * Decimal("1_000_000"))

    try:
        headers = build_l2_auth_headers(
            private_key=private_key,
            host=host,
            api_key=api_key,
            api_secret=api_secret,
            api_passphrase=api_passphrase,
            method="GET",
            path="/balance-allowance/update",
        )
    except Exception as exc:
        return {"ok": False, "error": f"auth headers failed: {exc}"}

    params = {
        "asset_type": "COLLATERAL",
        "signature_type": signature_type,
        "amount": str(amount_raw),
    }
    try:
        r = httpx.get(f"{host}/balance-allowance/update", headers=headers, params=params, timeout=30)
        r.raise_for_status()
        return {"ok": True, "error": ""}
    except httpx.HTTPStatusError as exc:
        body = exc.response.text[:200]
        return {"ok": False, "error": f"CLOB API error {exc.response.status_code}: {body}"}
    except Exception as exc:
        return {"ok": False, "error": f"CLOB API request failed: {exc}"}
