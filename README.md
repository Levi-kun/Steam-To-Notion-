# Steam to Notion

![Sync Status](https://github.com/Levi-kun/Steam-To-Notion-/actions/workflows/game-tracker.yml/badge.svg)

Automatically syncs your Steam library into a Notion database, so you always have an up-to-date catalog of your games with playtime, achievement progress, and cost-per-hour stats. New games get added on their own; existing entries get refreshed. All you have to do is rate them.

## How it works

1. **Fetch** your owned games from the Steam Web API (`IPlayerService/GetOwnedGames`).
2. **Validate** each title: filters out DLC, software, and non-game store entries (utilities, video production tools, etc.) using store metadata, genres, and gameplay indicators.
3. **Enrich** in parallel batches: pulls store details (genres, developers, price) and your achievement completion percentage per game, with retries and exponential backoff on rate limits.
4. **Sync** to Notion: creates pages for new games, updates hours played, achievement completion, and session counts for games already in the database.

Everything runs concurrently with `asyncio`/`aiohttp` (20 parallel Steam requests, 10 for Notion) and respects both APIs' rate limits.

## Notion database schema

Your database needs these properties (names must match exactly):

| Property | Type | Description |
|---|---|---|
| Game Name | Title | Game title from the Steam store |
| App ID | Number | Steam app ID, used to match existing entries |
| Hours Played | Number | Total playtime in hours |
| Session Count | Number | Tracked sessions |
| Achievement Completion | Number | % of achievements unlocked |
| Status | Select | e.g. `Owned` |
| Platform | Multi-select | e.g. `Steam` |
| Last Played | Date | Last session date |
| Genres | Multi-select | Top 5 genres from the Steam store |
| Price | Number | Current store price (USD) |
| Cost Per Hour | Number | Price divided by hours played |
| Developer | Rich text | Developer name(s) |

## Setup

### 1. Get your credentials

- **Steam API key**: https://steamcommunity.com/dev/apikey
- **Steam ID**: your 17-digit Steam ID (find it via https://steamid.io)
- **Notion integration token**: create an internal integration at https://www.notion.so/my-account/integrations, then share your database with it
- **Notion database ID**: the ID from your database URL, with the properties above

### 2. Run it locally

```bash
pip install -r requirements.txt

export STEAM_API_KEY="your_steam_api_key"
export STEAM_ID="your_steam_id"
export NOTION_TOKEN="your_notion_token"
export NOTION_DATABASE_ID="your_database_id"

python gaming_tracker.py --batch-mode
```

### 3. Run it on a schedule (GitHub Actions)

The included workflow (`.github/workflows/game-tracker.yml`) syncs automatically **7 times a day** on `ubuntu-latest` with Python 3.11. To enable it, add these repository secrets under **Settings > Secrets and variables > Actions**:

| Secret | Value |
|---|---|
| `STEAM_API_KEY` | Your Steam API key |
| `STEAM_ID` | Your Steam ID |
| `NOTION_TOKEN` | Your Notion integration token |
| `NOTION_DATABASE_ID` | Your Notion database ID |

You can also trigger a run manually from the **Actions** tab (`workflow_dispatch`), with options to toggle achievement processing or force a full sync.

## CLI options

```
python gaming_tracker.py [--batch-mode] [--log-level LEVEL] [--include-achievements]
```

| Flag | Default | Description |
|---|---|---|
| `--batch-mode` | off | Run the full batch sync pipeline |
| `--log-level` | `INFO` | Logging verbosity (`DEBUG`, `INFO`, `WARNING`, ...) |
| `--include-achievements` | on | Fetch achievement completion (slower, can be disabled via `INCLUDE_ACHIEVEMENTS=false`) |

Logs go to stdout and `gaming_tracker.log`.

## Project structure

```
├── gaming_tracker.py              # Main sync script (async batch processor)
├── requirements.txt               # Python dependencies
└── .github/workflows/
    └── game-tracker.yml           # Scheduled sync: 7x daily + manual trigger
```

## Notes

- Steam profile and game details must be public for the API calls to succeed.
- Games with private achievement stats are skipped gracefully and logged at debug level.
- Notion's API rate limits are respected with batched writes and short delays between batches.
