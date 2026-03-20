import asyncio
import os
import pytest
import sqlite3
import socket
from pathlib import Path
from uuid import uuid4
from cfte.storage.sqlite_writer import ThesisSQLiteStore

@pytest.mark.asyncio
async def test_database_writer_lock_blocks_competitor(tmp_path):
    db_path = tmp_path / "test_lock.db"
    store1 = ThesisSQLiteStore(db_path)
    await store1.migrate_schema()
    
    run_id1 = "run-1"
    pid1 = os.getpid()
    host = socket.gethostname()
    
    # Acquire lock for first "process"
    await store1.acquire_writer_lock(run_id1, pid1, host)
    
    # Try to acquire with different run_id from "same" PID (simulating a logic error or quick restart)
    store2 = ThesisSQLiteStore(db_path)
    run_id2 = "run-2"
    
    with pytest.raises(RuntimeError, match="Database đang được phiên khác sử dụng"):
        await store2.acquire_writer_lock(run_id2, pid1, host)
    
    # Release and re-acquire
    await store1.release_writer_lock(run_id1, pid1)
    await store2.acquire_writer_lock(run_id2, pid1, host)
    
@pytest.mark.asyncio
async def test_database_lock_cleans_stale_pid(tmp_path):
    db_path = tmp_path / "test_stale.db"
    store = ThesisSQLiteStore(db_path)
    await store.migrate_schema()
    
    # Manually insert a stale lock (PID 999999 which hopefully doesn't exist)
    with sqlite3.connect(db_path) as db:
        db.execute(
            "INSERT INTO system_writer_lock (lock_id, run_id, pid, host, acquired_at) VALUES (?, ?, ?, ?, ?)",
            ("primary_writer", "stale-run", 999999, "old-host", "2026-03-20T00:00:00Z")
        )
        db.commit()
        
    # Should be able to acquire because 999999 is dead (most likely)
    await store.acquire_writer_lock("new-run", os.getpid(), socket.gethostname())
