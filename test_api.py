import httpx, time, asyncio, json

async def test():
    c = httpx.AsyncClient(timeout=httpx.Timeout(15))
    now = int(time.time())
    win = 900
    base = (now // win) * win
    candidates = [base, base - win, base - 2 * win, base + win]
    for ts in candidates:
        slug = f'btc-updown-15m-{ts}'
        resp = await c.get('https://gamma-api.polymarket.com/events', params={'slug': slug})
        txt = resp.text.strip()
        print(f'slug={slug} status={resp.status_code} len={len(txt)}')
        if resp.status_code == 200 and txt:
            data = resp.json()
            if isinstance(data, dict):
                ev_slug = data.get("slug", "")
                mkt_count = len(data.get("markets", []))
                print(f"  resp_slug={ev_slug} markets={mkt_count}")
                if mkt_count > 0:
                    m = data["markets"][0]
                    print(f"  first_market: condition_id={m.get('condition_id')} clobTokenIds={m.get('clobTokenIds')} outcomePrices={m.get('outcomePrices')}")
                    break
    await c.aclose()

asyncio.run(test())
