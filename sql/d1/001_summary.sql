CREATE TABLE IF NOT EXISTS latest_thesis_cards (
    thesis_id TEXT PRIMARY KEY,
    instrument_key TEXT NOT NULL,
    setup TEXT NOT NULL,
    direction TEXT NOT NULL,
    stage TEXT NOT NULL,
    score REAL NOT NULL,
    confidence REAL NOT NULL,
    coverage REAL NOT NULL,
    payload_json TEXT NOT NULL,
    updated_ts INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS daily_expectancy_summary (
    trading_day TEXT NOT NULL,
    setup TEXT NOT NULL,
    venue TEXT NOT NULL,
    sample_count INTEGER NOT NULL,
    avg_score REAL NOT NULL,
    avg_confidence REAL NOT NULL,
    PRIMARY KEY (trading_day, setup, venue)
);
