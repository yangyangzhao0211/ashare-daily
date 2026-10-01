"""Optional compatibility command: summarize only verified V5 data."""
from sync_daily import rebuild_summary

if __name__ == "__main__":
    print(f"Verified source-universe trading days: {rebuild_summary()}")
