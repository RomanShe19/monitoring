#!/usr/bin/env python3
"""
Send test logs to Grafana Loki via the Push API.

Works with the docker-compose in this folder (default Loki URL: http://95.163.228.62:3100).
No external dependencies (stdlib only).
"""

from __future__ import annotations

import argparse
import datetime as _dt
import json
import random
import sys
import time
import urllib.error
import urllib.request
import uuid
from typing import Dict, Iterable, List, Mapping, MutableMapping, Sequence, Tuple


def parse_kv_pairs(items: list[str]) -> Dict[str, str]:
    labels: Dict[str, str] = {}
    for item in items:
        if "=" not in item:
            raise ValueError(f"Invalid label '{item}'. Expected key=value.")
        k, v = item.split("=", 1)
        k, v = k.strip(), v.strip()
        if not k:
            raise ValueError(f"Invalid label '{item}'. Empty key.")
        labels[k] = v
    return labels


def _labels_key(labels: Mapping[str, str]) -> Tuple[Tuple[str, str], ...]:
    return tuple(sorted((str(k), str(v)) for k, v in labels.items()))


def build_payload_streams(streams: Sequence[Tuple[Mapping[str, str], Sequence[Tuple[int, str]]]]) -> bytes:
    """
    Build Loki push payload for one or more streams.

    streams: [(labels, [(ts_ns, line), ...]), ...]
    """
    payload = {"streams": []}
    for labels, values in streams:
        payload["streams"].append(
            {
                "stream": dict(labels),
                "values": [[str(ts_ns), line] for ts_ns, line in values],
            }
        )
    return json.dumps(payload, ensure_ascii=False).encode("utf-8")


def post_loki_push(loki_url: str, payload: bytes, timeout_s: float = 5.0) -> Tuple[int, str]:
    url = loki_url.rstrip("/") + "/loki/api/v1/push"
    req = urllib.request.Request(
        url=url,
        data=payload,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            return resp.status, body
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace") if hasattr(e, "read") else str(e)
        return e.code, body


def _iso_now() -> str:
    # UTC timestamp, human friendly for Grafana Explore
    return _dt.datetime.now(tz=_dt.timezone.utc).isoformat(timespec="milliseconds")


def _choose_weighted(rng: random.Random, items: Sequence[Tuple[str, float]]) -> str:
    total = sum(w for _, w in items)
    if total <= 0:
        return items[0][0]
    r = rng.random() * total
    upto = 0.0
    for item, weight in items:
        upto += weight
        if r <= upto:
            return item
    return items[-1][0]


def _latency_ms(rng: random.Random, base: float, jitter: float, slow_rate: float) -> int:
    # Mix of "normal" and occasional "slow" outliers to make p95/p99 interesting
    v = max(0.0, rng.gauss(base, jitter))
    if rng.random() < slow_rate:
        v = v + rng.uniform(base * 5, base * 25)
    return int(round(v))


def _rand_ip(rng: random.Random) -> str:
    return f"{rng.randint(10, 250)}.{rng.randint(0, 255)}.{rng.randint(0, 255)}.{rng.randint(1, 254)}"


def _split_csv(s: str) -> List[str]:
    return [x.strip() for x in s.split(",") if x.strip()]


def _money(rng: random.Random, lo: float, hi: float, *, dp: int = 2) -> float:
    return round(rng.uniform(lo, hi), dp)


def _price_step(rng: random.Random, price: float, *, vol: float) -> float:
    # Simple geometric random walk with occasional volatility bursts
    burst = 1.0
    if rng.random() < 0.01:
        burst = rng.uniform(2.0, 6.0)
    pct = rng.gauss(0.0, vol * burst)
    new_price = max(0.00000001, price * (1.0 + pct))
    return new_price


def _init_market(rng: random.Random, symbols: Sequence[str]) -> Dict[str, Dict[str, float]]:
    base_prices = {
        "BTCUSDT": 42000.0,
        "ETHUSDT": 2400.0,
        "SOLUSDT": 110.0,
        "BNBUSDT": 320.0,
        "ADAUSDT": 0.55,
        "DOGEUSDT": 0.12,
        "XRPUSDT": 0.62,
    }
    market: Dict[str, Dict[str, float]] = {}
    for s in symbols:
        p0 = base_prices.get(s.upper(), rng.uniform(0.05, 500.0))
        p0 *= rng.uniform(0.92, 1.08)
        market[s] = {"price": p0, "vol": rng.uniform(0.0003, 0.0025)}
    return market


def _pick_component(rng: random.Random) -> str:
    return _choose_weighted(
        rng,
        [
            ("api", 6.0),
            ("matching-engine", 2.2),
            ("wallet", 1.8),
            ("risk", 0.8),
            ("price-feed", 1.8),
            ("web-frontend", 1.6),
        ],
    )


def _mk_labels(
    *,
    app: str,
    env: str,
    region: str,
    component: str,
    instance: str,
    kind: str,
    level: str,
) -> Dict[str, str]:
    return {
        "app": app,
        "env": env,
        "region": region,
        "component": component,
        "instance": instance,
        "kind": kind,
        "level": level,
    }


def _make_market_tick(
    rng: random.Random,
    *,
    app: str,
    env: str,
    region: str,
    instance: str,
    symbol: str,
    market: Dict[str, Dict[str, float]],
) -> Tuple[Dict[str, str], str]:
    st = market[symbol]
    prev = st["price"]
    st["price"] = _price_step(rng, prev, vol=st["vol"])
    price = st["price"]
    spread = max(price * rng.uniform(0.00002, 0.00025), 0.00000001)
    bid = price - spread / 2
    ask = price + spread / 2
    v = _money(rng, 0.1, 50.0, dp=4)
    direction = "up" if price >= prev else "down"

    event = {
        "ts": _iso_now(),
        "kind": "market",
        "level": "info",
        "msg": "Market tick",
        "symbol": symbol,
        "price": round(price, 8),
        "bid": round(bid, 8),
        "ask": round(ask, 8),
        "spread": round(spread, 8),
        "volume": v,
        "direction": direction,
        "volatility": round(st["vol"], 6),
    }
    labels = _mk_labels(app=app, env=env, region=region, component="price-feed", instance=instance, kind="market", level="info")
    labels["symbol"] = symbol
    return labels, json.dumps(event, ensure_ascii=False)


def _make_api_request(
    rng: random.Random,
    *,
    app: str,
    env: str,
    region: str,
    instance: str,
    component: str,
    symbol: str,
    error_rate: float,
    slow_rate: float,
) -> Tuple[Dict[str, str], str]:
    route = _choose_weighted(
        rng,
        [
            ("/api/v1/auth/login", 1.1),
            ("/api/v1/market/ticker", 5.0),
            ("/api/v1/market/depth", 2.8),
            ("/api/v1/orders", 3.2),
            ("/api/v1/orders/cancel", 1.2),
            ("/api/v1/wallet/balance", 2.0),
            ("/api/v1/wallet/withdraw", 0.8),
            ("/api/v1/user/kyc", 0.7),
        ],
    )
    method = "GET" if route in ("/api/v1/market/ticker", "/api/v1/market/depth", "/api/v1/wallet/balance") else "POST"
    status = 200
    level = "info"
    err_code = None

    if rng.random() < error_rate:
        status = int(_choose_weighted(rng, [("400", 3.0), ("401", 1.6), ("403", 0.8), ("429", 1.0), ("500", 1.2), ("502", 0.7), ("503", 0.5)]))
        level = "error" if status >= 500 else "warn"
        err_code = _choose_weighted(rng, [("RATE_LIMIT", 1.0), ("INVALID_SIGNATURE", 0.8), ("INSUFFICIENT_FUNDS", 0.6), ("UPSTREAM_TIMEOUT", 0.7), ("UNKNOWN", 0.4)])

    latency = _latency_ms(rng, base=35.0, jitter=22.0, slow_rate=slow_rate)
    if status >= 500:
        latency += rng.randint(50, 600)
    if status == 429:
        latency += rng.randint(0, 150)

    trace_id = uuid.uuid4().hex
    user_id = rng.randint(10_000, 99_999)
    event = {
        "ts": _iso_now(),
        "kind": "api",
        "level": level,
        "msg": "HTTP request",
        "method": method,
        "route": route,
        "status": status,
        "latency_ms": latency,
        "trace_id": trace_id,
        "user_id": user_id,
        "client_ip": _rand_ip(rng),
        "symbol": symbol if "market" in route or "orders" in route else None,
        "error_code": err_code,
    }
    labels = _mk_labels(app=app, env=env, region=region, component=component, instance=instance, kind="api", level=level)
    return labels, json.dumps(event, ensure_ascii=False)


def _make_frontend_event(
    rng: random.Random,
    *,
    app: str,
    env: str,
    region: str,
    instance: str,
    symbol: str,
    error_rate: float,
) -> Tuple[Dict[str, str], str]:
    action = _choose_weighted(
        rng,
        [
            ("page_view", 2.2),
            ("ws_connect", 1.1),
            ("ws_reconnect", 0.35),
            ("place_order_click", 0.9),
            ("render_chart", 1.6),
            ("toast_error", 0.2),
        ],
    )
    level = "info"
    ok = True
    if action in ("ws_reconnect", "toast_error") or rng.random() < error_rate * 0.3:
        ok = rng.random() > 0.5
        if not ok:
            level = "warn"

    fcp_ms = _latency_ms(rng, base=850.0, jitter=280.0, slow_rate=0.02)
    ws_rtt = _latency_ms(rng, base=55.0, jitter=25.0, slow_rate=0.03)
    session_id = uuid.uuid4().hex[:16]
    user_id = rng.randint(10_000, 99_999)

    event = {
        "ts": _iso_now(),
        "kind": "frontend",
        "level": level,
        "msg": "Frontend event",
        "action": action,
        "ok": ok,
        "fcp_ms": fcp_ms if action == "page_view" else None,
        "ws_rtt_ms": ws_rtt if action.startswith("ws_") else None,
        "session_id": session_id,
        "user_id": user_id,
        "page": _choose_weighted(rng, [("/trade", 3.0), ("/wallet", 1.4), ("/portfolio", 1.0), ("/settings", 0.5)]),
        "symbol": symbol,
        "browser": _choose_weighted(rng, [("Chrome", 6.0), ("Firefox", 1.2), ("Safari", 1.0), ("Edge", 0.8)]),
    }
    labels = _mk_labels(app=app, env=env, region=region, component="web-frontend", instance=instance, kind="frontend", level=level)
    return labels, json.dumps(event, ensure_ascii=False)


def _make_trade_event(
    rng: random.Random,
    *,
    app: str,
    env: str,
    region: str,
    instance: str,
    symbol: str,
    market: Dict[str, Dict[str, float]],
    error_rate: float,
) -> Tuple[Dict[str, str], str]:
    side = _choose_weighted(rng, [("buy", 1.0), ("sell", 1.0)])
    order_type = _choose_weighted(rng, [("market", 1.7), ("limit", 1.0), ("stop", 0.25)])
    user_id = rng.randint(10_000, 99_999)
    order_id = f"ORD-{rng.randint(100_000_000, 999_999_999)}"
    trace_id = uuid.uuid4().hex

    px = market[symbol]["price"]
    limit_price = px * (1.0 + (rng.uniform(-0.004, 0.004) if order_type != "market" else 0.0))
    qty = _money(rng, 0.001, 1.2, dp=6) if symbol.startswith("BTC") else _money(rng, 0.01, 25.0, dp=5)
    notional = float(round(qty * (px if order_type == "market" else limit_price), 2))

    level = "info"
    status = _choose_weighted(rng, [("accepted", 2.5), ("filled", 2.8), ("partially_filled", 0.9), ("rejected", 0.25)])
    reject_reason = None
    if status == "rejected" or rng.random() < error_rate * 0.25:
        status = "rejected"
        level = "warn"
        reject_reason = _choose_weighted(rng, [("INSUFFICIENT_MARGIN", 1.2), ("RISK_LIMIT", 0.8), ("PRICE_OUT_OF_BOUNDS", 0.7), ("KYC_REQUIRED", 0.3)])

    fee = float(round(notional * rng.uniform(0.0002, 0.0012), 4))
    event = {
        "ts": _iso_now(),
        "kind": "trading",
        "level": level,
        "msg": "Order event",
        "symbol": symbol,
        "side": side,
        "order_type": order_type,
        "order_id": order_id,
        "user_id": user_id,
        "status": status,
        "qty": qty,
        "price": round(px, 8),
        "limit_price": round(limit_price, 8) if order_type != "market" else None,
        "notional": notional,
        "fee": fee,
        "fee_asset": "USDT",
        "trace_id": trace_id,
        "reject_reason": reject_reason,
        "matching_latency_ms": _latency_ms(rng, base=8.0, jitter=6.0, slow_rate=0.01),
    }
    labels = _mk_labels(app=app, env=env, region=region, component="matching-engine", instance=instance, kind="trading", level=level)
    labels["symbol"] = symbol
    labels["side"] = side
    return labels, json.dumps(event, ensure_ascii=False)


def _make_wallet_event(
    rng: random.Random,
    *,
    app: str,
    env: str,
    region: str,
    instance: str,
    error_rate: float,
) -> Tuple[Dict[str, str], str]:
    action = _choose_weighted(rng, [("deposit_detected", 1.2), ("deposit_confirmed", 1.0), ("withdrawal_requested", 1.0), ("withdrawal_sent", 0.9), ("withdrawal_failed", 0.25)])
    asset = _choose_weighted(rng, [("BTC", 0.6), ("ETH", 0.9), ("USDT", 2.4), ("SOL", 0.8), ("BNB", 0.6)])
    network = _choose_weighted(rng, [("bitcoin", 0.6), ("ethereum", 0.9), ("tron", 1.0), ("solana", 0.7), ("bsc", 0.6)])
    user_id = rng.randint(10_000, 99_999)
    amount = _money(rng, 5.0, 2500.0, dp=2) if asset == "USDT" else _money(rng, 0.0005, 2.5, dp=6)
    txid = uuid.uuid4().hex
    confirmations = rng.randint(0, 25)
    required = 2 if network in ("solana",) else (6 if network in ("bitcoin",) else 12)

    level = "info"
    ok = True
    error_code = None
    if action == "withdrawal_failed" or rng.random() < error_rate * 0.2:
        if action != "withdrawal_failed":
            action = "withdrawal_failed"
        ok = False
        level = "error"
        error_code = _choose_weighted(rng, [("ADDRESS_BLACKLISTED", 0.6), ("AML_FLAG", 0.9), ("INSUFFICIENT_FUNDS", 0.7), ("NODE_TIMEOUT", 0.6)])

    event = {
        "ts": _iso_now(),
        "kind": "wallet",
        "level": level,
        "msg": "Wallet event",
        "action": action,
        "user_id": user_id,
        "asset": asset,
        "network": network,
        "amount": amount,
        "txid": txid,
        "confirmations": confirmations if "deposit" in action else None,
        "confirmations_required": required if "deposit" in action else None,
        "ok": ok,
        "error_code": error_code,
        "trace_id": uuid.uuid4().hex,
    }
    labels = _mk_labels(app=app, env=env, region=region, component="wallet", instance=instance, kind="wallet", level=level)
    labels["asset"] = asset
    labels["network"] = network
    return labels, json.dumps(event, ensure_ascii=False)


def _make_risk_event(
    rng: random.Random,
    *,
    app: str,
    env: str,
    region: str,
    instance: str,
) -> Tuple[Dict[str, str], str]:
    rule = _choose_weighted(rng, [("velocity_withdrawals", 1.2), ("ip_mismatch", 0.8), ("device_fingerprint", 0.6), ("large_order", 0.7), ("sanctions_screening", 0.5)])
    severity = _choose_weighted(rng, [("low", 1.8), ("medium", 1.0), ("high", 0.35)])
    level = "warn" if severity in ("medium", "high") else "info"
    if severity == "high":
        level = "error"
    user_id = rng.randint(10_000, 99_999)
    decision = _choose_weighted(rng, [("allow", 2.2), ("review", 0.8), ("block", 0.25 if severity != "low" else 0.05)])
    score = int(round(rng.uniform(0, 100)))
    event = {
        "ts": _iso_now(),
        "kind": "risk",
        "level": level,
        "msg": "Risk check",
        "rule": rule,
        "severity": severity,
        "decision": decision,
        "score": score,
        "user_id": user_id,
        "trace_id": uuid.uuid4().hex,
    }
    labels = _mk_labels(app=app, env=env, region=region, component="risk", instance=instance, kind="risk", level=level)
    labels["decision"] = decision
    labels["severity"] = severity
    return labels, json.dumps(event, ensure_ascii=False)


def _make_exception(
    rng: random.Random,
    *,
    app: str,
    env: str,
    region: str,
    component: str,
    instance: str,
) -> Tuple[Dict[str, str], str]:
    err_type = _choose_weighted(rng, [("TimeoutError", 1.3), ("ConnectionError", 1.0), ("ValueError", 0.7), ("KeyError", 0.5), ("RuntimeError", 0.4)])
    msg = _choose_weighted(rng, [("upstream timeout", 1.0), ("node not responding", 0.8), ("db deadlock", 0.5), ("signature verification failed", 0.6)])
    stack = f"{err_type}: {msg}\n  at handler()\n  at service()\n  at call_upstream()"
    event = {
        "ts": _iso_now(),
        "kind": "exception",
        "level": "error",
        "msg": "Unhandled exception",
        "component": component,
        "error_type": err_type,
        "error": msg,
        "stacktrace": stack,
        "trace_id": uuid.uuid4().hex,
    }
    labels = _mk_labels(app=app, env=env, region=region, component=component, instance=instance, kind="exception", level="error")
    return labels, json.dumps(event, ensure_ascii=False)


def _iter_crypto_events(
    rng: random.Random,
    *,
    app: str,
    env: str,
    regions: Sequence[str],
    instances_per_component: int,
    symbols: Sequence[str],
    profile: str,
    error_rate: float,
    slow_rate: float,
) -> Iterable[Tuple[Dict[str, str], str]]:
    market = _init_market(rng, symbols)

    components = ["api", "matching-engine", "wallet", "risk", "price-feed", "web-frontend"]
    instances: Dict[str, List[str]] = {c: [f"{c}-{i+1:02d}" for i in range(instances_per_component)] for c in components}

    while True:
        region = rng.choice(list(regions))
        symbol = rng.choice(list(symbols))

        # event mix by profile
        if profile == "market":
            kind = _choose_weighted(rng, [("market", 6.0), ("api", 1.2), ("frontend", 0.6)])
        elif profile == "trading":
            kind = _choose_weighted(rng, [("trading", 3.2), ("api", 1.6), ("risk", 0.6), ("market", 1.0)])
        elif profile == "wallet":
            kind = _choose_weighted(rng, [("wallet", 3.0), ("api", 1.2), ("risk", 0.5), ("exception", 0.2)])
        elif profile == "frontend":
            kind = _choose_weighted(rng, [("frontend", 3.2), ("api", 1.0), ("market", 1.0), ("exception", 0.1)])
        elif profile == "errors":
            kind = _choose_weighted(rng, [("exception", 2.2), ("wallet", 0.8), ("api", 0.8), ("risk", 0.4), ("trading", 0.4)])
        else:  # crypto (default)
            kind = _choose_weighted(rng, [("market", 3.0), ("api", 2.0), ("trading", 1.4), ("wallet", 0.8), ("frontend", 1.0), ("risk", 0.35), ("exception", 0.12)])

        if kind == "market":
            inst = rng.choice(instances["price-feed"])
            yield _make_market_tick(rng, app=app, env=env, region=region, instance=inst, symbol=symbol, market=market)
        elif kind == "api":
            component = _pick_component(rng)
            inst = rng.choice(instances.get(component, instances["api"]))
            yield _make_api_request(rng, app=app, env=env, region=region, instance=inst, component=component, symbol=symbol, error_rate=error_rate, slow_rate=slow_rate)
        elif kind == "trading":
            inst = rng.choice(instances["matching-engine"])
            yield _make_trade_event(rng, app=app, env=env, region=region, instance=inst, symbol=symbol, market=market, error_rate=error_rate)
        elif kind == "wallet":
            inst = rng.choice(instances["wallet"])
            yield _make_wallet_event(rng, app=app, env=env, region=region, instance=inst, error_rate=error_rate)
        elif kind == "frontend":
            inst = rng.choice(instances["web-frontend"])
            yield _make_frontend_event(rng, app=app, env=env, region=region, instance=inst, symbol=symbol, error_rate=error_rate)
        elif kind == "risk":
            inst = rng.choice(instances["risk"])
            yield _make_risk_event(rng, app=app, env=env, region=region, instance=inst)
        else:
            component = _pick_component(rng)
            inst = rng.choice(instances.get(component, instances["api"]))
            yield _make_exception(rng, app=app, env=env, region=region, component=component, instance=inst)


def _send_batch(
    *,
    loki_url: str,
    base_labels: Mapping[str, str],
    events: Sequence[Tuple[Mapping[str, str], str]],
    timeout_s: float,
    dry_run: bool,
) -> Tuple[int, str]:
    """
    Group events by labels into multiple streams and push in one request.
    """
    by_stream: MutableMapping[Tuple[Tuple[str, str], ...], Tuple[Dict[str, str], List[Tuple[int, str]]]] = {}
    for labels, line in events:
        merged = dict(base_labels)
        merged.update(dict(labels))
        key = _labels_key(merged)
        if key not in by_stream:
            by_stream[key] = (dict(merged), [])
        by_stream[key][1].append((time.time_ns(), line))

    payload = build_payload_streams([(lbls, vals) for (lbls, vals) in by_stream.values()])
    if dry_run:
        # Print a compact preview, but still show label diversity
        streams = len(by_stream)
        lines = sum(len(vals) for _, vals in by_stream.values())
        print(f"DRY-RUN: would push {lines} lines across {streams} streams to {loki_url}")
        for (lbls, vals) in list(by_stream.values())[:3]:
            print(f"  labels={lbls} sample_line={vals[-1][1]}")
        return 200, "dry-run"

    return post_loki_push(loki_url, payload, timeout_s=timeout_s)


def main() -> int:
    p = argparse.ArgumentParser(description="Generate crypto-app logs and send to Grafana Loki (Push API).")
    p.add_argument("--loki-url", default="http://95.163.228.62:3100", help="Base Loki URL (default: http://95.163.228.62:3100)")
    p.add_argument(
        "--label",
        action="append",
        default=[],
        help="Stream label in key=value form (repeatable). Example: --label job=test --label env=dev",
    )
    p.add_argument("--message", default="hello from crypto-app", help="Log line to send (plain mode).")
    p.add_argument("--count", type=int, default=0, help="How many events to generate/send. 0 means unlimited (default: 0).")
    p.add_argument("--interval", type=float, default=0.15, help="Seconds to sleep between events when --rate is 0 (default: 0.15).")
    p.add_argument("--generate", action="store_true", help="Generate crypto-app logs (JSON). If not set, sends --message as plain text.")
    p.add_argument("--profile", default="crypto", choices=["crypto", "market", "trading", "wallet", "frontend", "errors"], help="Crypto log mix profile (default: crypto).")
    p.add_argument("--app", default="crypto-demo", help="App name label (default: crypto-demo).")
    p.add_argument("--env", default="dev", help="Environment label (default: dev).")
    p.add_argument("--regions", default="ru-msk,ru-spb,eu-fr", help="Comma-separated regions (default: ru-msk,ru-spb,eu-fr).")
    p.add_argument("--instances-per-component", type=int, default=2, help="How many instances per component to simulate (default: 2).")
    p.add_argument("--symbols", default="BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT,ADAUSDT", help="Comma-separated symbols (default: BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT,ADAUSDT).")
    p.add_argument("--error-rate", type=float, default=0.03, help="Probability of warnings/errors (default: 0.03).")
    p.add_argument("--slow-rate", type=float, default=0.02, help="Probability of slow latency outliers for API (default: 0.02).")
    p.add_argument("--duration", type=float, default=0.0, help="Run for N seconds. 0 means unlimited unless --count > 0 (default: 0).")
    p.add_argument("--rate", type=float, default=0.0, help="Target events per second. 0 means use --interval (default: 0).")
    p.add_argument("--batch-size", type=int, default=25, help="How many events to push per HTTP request (default: 25).")
    p.add_argument("--seed", type=int, default=0, help="Random seed for reproducible logs (0 = random).")
    p.add_argument("--stdout", action="store_true", help="Print every generated event line to stdout (useful to see continuous output).")
    p.add_argument("--dry-run", action="store_true", help="Do not send to Loki; print a preview instead.")
    p.add_argument("--timeout", type=float, default=5.0, help="HTTP timeout seconds (default: 5).")

    args = p.parse_args()

    base_labels = {"job": "test-logger", "app": args.app, "env": args.env}
    base_labels.update(parse_kv_pairs(args.label))

    if args.count < 0:
        print("ERROR: --count must be >= 0", file=sys.stderr)
        return 2
    if args.interval < 0:
        print("ERROR: --interval must be >= 0", file=sys.stderr)
        return 2
    if args.instances_per_component < 1:
        print("ERROR: --instances-per-component must be >= 1", file=sys.stderr)
        return 2
    if args.batch_size < 1:
        print("ERROR: --batch-size must be >= 1", file=sys.stderr)
        return 2
    if args.error_rate < 0 or args.error_rate > 1:
        print("ERROR: --error-rate must be in [0, 1]", file=sys.stderr)
        return 2
    if args.slow_rate < 0 or args.slow_rate > 1:
        print("ERROR: --slow-rate must be in [0, 1]", file=sys.stderr)
        return 2
    if args.duration < 0:
        print("ERROR: --duration must be >= 0", file=sys.stderr)
        return 2
    if args.rate < 0:
        print("ERROR: --rate must be >= 0", file=sys.stderr)
        return 2

    if not args.generate:
        # Backward-compatible: send plain text lines with a single stream
        loops = args.count if args.count > 0 else 1
        for i in range(loops):
            line = args.message
            if loops > 1:
                line = f"{line} ({i + 1}/{loops})"

            status, body = _send_batch(
                loki_url=args.loki_url,
                base_labels=base_labels,
                events=[({}, line)],
                timeout_s=args.timeout,
                dry_run=args.dry_run,
            )
            if status < 200 or status >= 300:
                print(f"ERROR: Loki push failed: HTTP {status}", file=sys.stderr)
                if body:
                    print(body, file=sys.stderr)
                return 1

            print(f"OK: sent to {args.loki_url} labels={base_labels} line={line}")
            if args.interval and i + 1 < loops:
                time.sleep(args.interval)
        return 0

    regions = _split_csv(args.regions)
    if not regions:
        print("ERROR: --regions must have at least one value", file=sys.stderr)
        return 2
    symbols = _split_csv(args.symbols)
    if not symbols:
        print("ERROR: --symbols must have at least one value", file=sys.stderr)
        return 2

    rng = random.Random(args.seed if args.seed != 0 else None)
    interval_s = args.interval
    if args.rate and args.rate > 0:
        interval_s = 1.0 / args.rate

    events_iter = _iter_crypto_events(
        rng,
        app=args.app,
        env=args.env,
        regions=regions,
        instances_per_component=args.instances_per_component,
        symbols=symbols,
        profile=args.profile,
        error_rate=args.error_rate,
        slow_rate=args.slow_rate,
    )

    sent = 0
    batch: List[Tuple[Mapping[str, str], str]] = []
    deadline = time.time() + args.duration if args.duration and args.duration > 0 else None

    while True:
        if deadline is not None and time.time() >= deadline:
            break
        if args.count and args.count > 0 and sent >= args.count:
            break

        labels, line = next(events_iter)
        if args.stdout:
            print(line)
        batch.append((labels, line))
        sent += 1

        if len(batch) >= args.batch_size:
            status, body = _send_batch(
                loki_url=args.loki_url,
                base_labels=base_labels,
                events=batch,
                timeout_s=args.timeout,
                dry_run=args.dry_run,
            )
            if status < 200 or status >= 300:
                print(f"ERROR: Loki push failed: HTTP {status}", file=sys.stderr)
                if body:
                    print(body, file=sys.stderr)
                return 1
            print(f"OK: pushed {len(batch)} events (total={sent}) profile={args.profile} app={args.app} env={args.env} to {args.loki_url}")
            batch = []

        if interval_s and interval_s > 0:
            time.sleep(interval_s)

    # Flush
    if batch:
        status, body = _send_batch(
            loki_url=args.loki_url,
            base_labels=base_labels,
            events=batch,
            timeout_s=args.timeout,
            dry_run=args.dry_run,
        )
        if status < 200 or status >= 300:
            print(f"ERROR: Loki push failed: HTTP {status}", file=sys.stderr)
            if body:
                print(body, file=sys.stderr)
            return 1
        print(f"OK: pushed {len(batch)} events (total={sent}) profile={args.profile} app={args.app} env={args.env} to {args.loki_url}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())


