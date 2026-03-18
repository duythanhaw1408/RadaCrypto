PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS instrument_dim (
    instrument_key TEXT PRIMARY KEY,
    venue TEXT NOT NULL,
    market_type TEXT NOT NULL,
    symbol TEXT NOT NULL,
    base TEXT NOT NULL,
    quote TEXT NOT NULL,
    is_perp INTEGER NOT NULL DEFAULT 0,
    tick_size REAL,
    lot_size REAL,
    status TEXT NOT NULL DEFAULT 'ACTIVE',
    listing_ts INTEGER
);

CREATE TABLE IF NOT EXISTS stream_watermark (
    stream_key TEXT PRIMARY KEY,
    last_seq INTEGER,
    last_event_ts INTEGER,
    snapshot_ref TEXT,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS order_state (
    canonical_order_id TEXT PRIMARY KEY,
    venue TEXT NOT NULL,
    account_id TEXT NOT NULL,
    venue_order_id TEXT,
    client_order_id TEXT,
    instrument_key TEXT NOT NULL,
    side TEXT NOT NULL,
    order_type TEXT NOT NULL,
    tif TEXT,
    post_only INTEGER NOT NULL DEFAULT 0,
    reduce_only INTEGER NOT NULL DEFAULT 0,
    px REAL,
    qty REAL NOT NULL,
    leaves_qty REAL NOT NULL,
    cum_qty REAL NOT NULL DEFAULT 0,
    avg_fill_px REAL,
    status TEXT NOT NULL,
    created_ts INTEGER NOT NULL,
    updated_ts INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS fill_fact (
    fill_id TEXT PRIMARY KEY,
    canonical_order_id TEXT NOT NULL,
    venue TEXT NOT NULL,
    account_id TEXT NOT NULL,
    instrument_key TEXT NOT NULL,
    side TEXT NOT NULL,
    liquidity_flag TEXT,
    fill_px REAL NOT NULL,
    fill_qty REAL NOT NULL,
    fee_ccy TEXT,
    fee_amt REAL,
    venue_ts INTEGER NOT NULL,
    recv_ts INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS position_state (
    venue TEXT NOT NULL,
    account_id TEXT NOT NULL,
    instrument_key TEXT NOT NULL,
    side TEXT NOT NULL,
    qty REAL NOT NULL,
    entry_px REAL,
    mark_px REAL,
    unreal_pnl REAL,
    leverage REAL,
    liquidation_px REAL,
    updated_ts INTEGER NOT NULL,
    PRIMARY KEY (venue, account_id, instrument_key, side)
);

CREATE TABLE IF NOT EXISTS thesis (
    thesis_id TEXT PRIMARY KEY,
    instrument_key TEXT NOT NULL,
    setup TEXT NOT NULL,
    direction TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    regime_bucket TEXT NOT NULL,
    stage TEXT NOT NULL,
    score REAL NOT NULL,
    confidence REAL NOT NULL,
    coverage REAL NOT NULL,
    invalidation_px REAL,
    opened_ts INTEGER NOT NULL,
    closed_ts INTEGER,
    entry_px REAL
);

CREATE TABLE IF NOT EXISTS thesis_outcome (
    thesis_id TEXT NOT NULL,
    horizon TEXT NOT NULL,
    target_ts INTEGER NOT NULL,
    realized_px REAL,
    realized_high REAL,
    realized_low REAL,
    status TEXT NOT NULL DEFAULT 'PENDING',
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (thesis_id, horizon),
    FOREIGN KEY (thesis_id) REFERENCES thesis(thesis_id)
);

CREATE TABLE IF NOT EXISTS thesis_event (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    thesis_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    delta_score REAL,
    reason_json TEXT,
    event_ts INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS alert_log (
    alert_id TEXT PRIMARY KEY,
    thesis_id TEXT NOT NULL,
    channel TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    sent_ts INTEGER NOT NULL,
    dedup_key TEXT NOT NULL,
    status TEXT NOT NULL
);
