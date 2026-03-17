CREATE INDEX IF NOT EXISTS idx_fill_fact_order ON fill_fact(canonical_order_id);
CREATE INDEX IF NOT EXISTS idx_fill_fact_instr_ts ON fill_fact(instrument_key, venue_ts);
CREATE INDEX IF NOT EXISTS idx_order_state_instr ON order_state(instrument_key);
CREATE INDEX IF NOT EXISTS idx_position_state_instr ON position_state(instrument_key);
CREATE INDEX IF NOT EXISTS idx_thesis_instr_stage ON thesis(instrument_key, stage);
CREATE INDEX IF NOT EXISTS idx_thesis_event_thesis_ts ON thesis_event(thesis_id, event_ts);
CREATE INDEX IF NOT EXISTS idx_alert_log_dedup ON alert_log(dedup_key);
