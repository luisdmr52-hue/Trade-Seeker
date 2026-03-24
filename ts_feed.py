"""
ts_feed.py — TSFeed module for Trade Seeker v4.0
Replaces REST polling with Timescale queries for the fast detection loop.

Strategy:
- get_snapshot(): one query, all active symbols in last 10s → dict[symbol]
- Caller maintains prev_prices dict between iterations to compute delta
- buy_vol_ratio: buy_vol / vol_24h → directional bias for the day (filter only)
- Delta price between snapshots is the primary signal
"""

import time
import psycopg2
import psycopg2.extras
from typing import Dict, Optional

# DSN — matches docker-compose stack, Timescale is on host network
DSN = "host=localhost port=5432 dbname=tsdb user=postgres password=postgres"

# How far back to look for "active" symbols per snapshot query
SNAPSHOT_WINDOW_S = 10


class TSFeed:
    def __init__(self, dsn: str = DSN):
        self.dsn = dsn
        self.conn: Optional[psycopg2.extensions.connection] = None
        self._connect()

    def _connect(self):
        try:
            self.conn = psycopg2.connect(self.dsn)
            self.conn.autocommit = True
            print("[TSFeed] connected to Timescale")
        except Exception as e:
            print(f"[TSFeed] connection error: {e}")
            self.conn = None

    def _ensure_conn(self) -> bool:
        """Reconnect if connection dropped."""
        if self.conn is None or self.conn.closed:
            print("[TSFeed] reconnecting...")
            self._connect()
        return self.conn is not None and not self.conn.closed

    def get_snapshot(self) -> Dict[str, dict]:
        """
        One query — returns latest tick per active symbol in last SNAPSHOT_WINDOW_S seconds.
        Returns dict keyed by symbol:
            {price, buy_vol, vol_24h, ts}
        Returns empty dict on error (caller should handle gracefully).
        """
        if not self._ensure_conn():
            return {}

        sql = """
            SELECT DISTINCT ON (symbol)
                symbol,
                price,
                buy_vol,
                vol_24h,
                quote_vol_24h,
                ts
            FROM metrics_ext
            WHERE ts >= NOW() - INTERVAL '{window} seconds'
            ORDER BY symbol, ts DESC
        """.format(window=SNAPSHOT_WINDOW_S)

        try:
            with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql)
                rows = cur.fetchall()
            return {row["symbol"]: dict(row) for row in rows}
        except Exception as e:
            print(f"[TSFeed] snapshot error: {e}")
            self.conn = None  # force reconnect next call
            return {}

    def close(self):
        if self.conn and not self.conn.closed:
            self.conn.close()


class PriceTracker:
    """
    Tracks previous prices and computes delta % between snapshots.
    Stateless relative to DB — lives in process memory.
    """

    def __init__(self):
        self.prev: Dict[str, float] = {}  # symbol → last seen price

    def update(self, symbol: str, price: float) -> Optional[float]:
        """
        Returns delta % vs previous snapshot, or None if first time seeing symbol.
        Updates internal state.
        """
        prev_price = self.prev.get(symbol)
        self.prev[symbol] = price

        if prev_price is None or prev_price == 0:
            return None

        return (price - prev_price) / prev_price * 100.0

    def reset(self, symbol: str):
        self.prev.pop(symbol, None)


def buy_vol_ratio(buy_vol: float, quote_vol_24h: float) -> float:
    """
    buy_vol / vol_24h — taker buy as fraction of total 24h volume.
    > 0.55 → mild buy bias
    > 0.65 → strong buy bias
    Returns 0.5 if vol_24h is zero (neutral, no bias info).
    """
    if not quote_vol_24h:
        return 0.5
    return buy_vol / quote_vol_24h


# ---------------------------------------------------------------------------
# Standalone test — run directly to validate feed before integrating
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import time

    feed = TSFeed()
    tracker = PriceTracker()

    # Thresholds for standalone test
    PUMP_PCT = 0.3       # % price move between snapshots to flag
    MIN_VOL = 50_000     # min vol_24h in USDT to filter noise
    BV_RATIO_MIN = 0.55  # min buy_vol ratio to confirm bias

    print(f"\n[TEST] Polling every 2s — flagging moves >= {PUMP_PCT}% with vol >= {MIN_VOL} USDT\n")

    try:
        while True:
            t0 = time.time()
            snapshot = feed.get_snapshot()
            elapsed = time.time() - t0

            if not snapshot:
                print("[TEST] empty snapshot — check Timescale connection")
                time.sleep(2)
                continue

            hits = []
            for sym, data in snapshot.items():
                price = data["price"]
                vol = data["vol_24h"] or 0
                bv = data["buy_vol"] or 0

                if vol < MIN_VOL:
                    continue

                delta = tracker.update(sym, price)
                if delta is None:
                    continue

                bvr = buy_vol_ratio(bv, vol)

                if abs(delta) >= PUMP_PCT and bvr >= BV_RATIO_MIN:
                    direction = "PUMP" if delta > 0 else "DUMP"
                    hits.append((direction, sym, delta, bvr, price))

            hits.sort(key=lambda x: abs(x[2]), reverse=True)
            for direction, sym, delta, bvr, price in hits[:5]:
                print(f"  {direction} | {sym:<14} | delta={delta:+.3f}% | bv_ratio={bvr:.2f} | price={price}")

            print(f"[snapshot] {len(snapshot)} symbols | query={elapsed*1000:.1f}ms | hits={len(hits)}")
            time.sleep(2)

    except KeyboardInterrupt:
        print("\n[TEST] stopped")
        feed.close()
