# SLC Ice Times 🏒

All Salt Lake County ice rink open sessions in one place — drop-in hockey, stick & puck, and public skate schedules from four rinks, updated automatically.

## Rinks Covered

- **Acord Ice Center** — West Valley City
- **County Ice Center** — Murray
- **SLC Sports Complex** — Salt Lake City
- **Cottonwood Heights Rec Center** — Cottonwood Heights

## How It Works

1. A **GitHub Action** runs twice daily (8am & 6pm MT)
2. A **Python scraper** fetches the latest schedule PDFs from QuickScores and scrapes Cottonwood Heights' website
3. Schedules are parsed into a single `data/schedules.json` file
4. If data changed, it's committed and **GitHub Pages** auto-deploys the updated site

The frontend is a single `index.html` — no build step, no framework, no dependencies.

## Local Development

```bash
# Run the scraper locally
pip install -r scraper/requirements.txt
python scraper/scrape.py

# Serve the site
python -m http.server 8000
# Open http://localhost:8000
```

## Setup

1. Fork/clone this repo
2. Enable **GitHub Pages** (Settings → Pages → Source: GitHub Actions)
3. The Action will run on push and on the cron schedule
4. Optionally trigger manually via the Actions tab

## Data Format

`data/schedules.json` contains all rinks and sessions:

```json
{
  "last_updated": "2026-03-16T14:00:00+00:00",
  "rinks": [
    {
      "id": "acord",
      "name": "Acord Ice Center",
      "sessions": [
        {
          "date": "2026-03-20",
          "type": "stick_and_puck",
          "start": "09:00",
          "end": "10:00"
        }
      ]
    }
  ]
}
```

Session types: `stick_and_puck`, `drop_in`, `public_skate`

## Disclaimers

Schedules are subject to change without notice. Always confirm with the rink before heading out. This is an unofficial community project — not affiliated with Salt Lake County Parks & Recreation or Cottonwood Heights Parks & Recreation.

## License

MIT
