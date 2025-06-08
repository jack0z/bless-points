# Bless Uptime Tracker

This folder contains the Bless uptime tracker.

## Files
- `bless_points_tracker.py`: Main tracker script
- `bless_tokens.json`: Account tokens (JWT)
- `proxy.txt`: List of proxies (one per line)
- `.env`: Environment variables (MongoDB URI)

## Usage
1. Add your account tokens to `bless_tokens.json`.
2. Add your proxies to `proxy.txt` (optional).
3. Create a `.env` file in this folder with:
   ```
   MONGODB_URI=your-mongodb-uri-here
   ```
4. Add `.env` to your `.gitignore` to keep secrets safe.
5. Run the tracker with `python bless_points_tracker.py`. 