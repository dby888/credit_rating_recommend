# snowflake.py
import time
import threading

# Custom epoch: 2020-01-01
EPOCH = int(time.mktime(time.strptime("2020-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"))) * 1000

# Global configuration
DATACENTER_ID = 1  # 0~31
WORKER_ID = 1      # 0~31

# Internal state
_lock = threading.Lock()
_sequence = 0
_last_timestamp = -1

def _timestamp():
    return int(time.time() * 1000)

def get_next_id():
    """Generate a globally unique 64-bit Snowflake ID."""
    global _sequence, _last_timestamp
    with _lock:
        ts = _timestamp()
        if ts == _last_timestamp:
            _sequence = (_sequence + 1) & 0xFFF  # 12-bit sequence
            if _sequence == 0:
                # Sequence overflow, wait for next millisecond
                while ts <= _last_timestamp:
                    ts = _timestamp()
        else:
            _sequence = 0
        _last_timestamp = ts

        # 41 bits timestamp | 5 bits datacenter | 5 bits worker | 12 bits sequence
        return ((ts - EPOCH) << 22) | (DATACENTER_ID << 17) | (WORKER_ID << 12) | _sequence


