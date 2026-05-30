"""
create_api_key.py
只需提供 EOA 钱包私钥，自动调用 ClobClient.create_or_derive_api_key()
生成并打印 API Key / Secret / Passphrase。

.env 配置：
  - PRIVATE_KEY=0x...   （EOA 私钥，带 0x 前缀）
  - 可选：CLOB_HOST=https://clob.polymarket.com
"""
import getpass
import os
import sys

from dotenv import load_dotenv
from py_clob_client_v2 import ClobClient


def prompt_private_key():
    while True:
        pk = getpass.getpass("请输入 EOA 钱包私钥（0x + 64位hex，不显示输入）：").strip()
        if not pk:
            print("私钥不能为空，重新输入。")
            continue
        pk_hex = pk[2:] if pk.startswith("0x") or pk.startswith("0X") else pk
        if len(pk_hex) != 64:
            print(f"私钥长度错误（期望64位，实际{len(pk_hex)}），重新输入。")
            continue
        return pk if pk.startswith("0x") else "0x" + pk


def main():
    load_dotenv()

    pk = prompt_private_key()

    host = os.getenv("CLOB_HOST", "https://clob.polymarket.com")

    print(f"初始化 ClobClient（host={host}）...")
    client = ClobClient(host=host, chain_id=137, key=pk)

    print("调用 create_or_derive_api_key()...")
    creds = client.create_or_derive_api_key()

    print()
    print("=" * 60)
    print(" 凭证生成成功！")
    print("=" * 60)
    print()
    print("将以下内容填入 .env（覆盖原有 API_KEY / API_SECRET / API_PASSPHRASE）：")
    print()
    print(f"API_KEY={creds.api_key}")
    print(f"API_SECRET={creds.api_secret}")
    print(f"API_PASSPHRASE={creds.api_passphrase}")
    print()


if __name__ == "__main__":
    main()
