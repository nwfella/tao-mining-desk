# TAO Mining Desk

A miner's view of every Bittensor subnet. Not a price dashboard — a seat-by-seat accounting of **what a miner UID is actually paid**, what it costs to sit down, and whether the chair pays at all.

**Live:** https://nwfella.github.io/tao-mining-desk/

---

## What it answers

| Question | Where |
| --- | --- |
| Which subnets pay their miners, and how much per paying seat? | `Income / winning miner` column, sorted |
| What fraction of registered UIDs on a subnet earn anything? | `Winning miners` (e.g. 2 / 242) |
| What does it cost to register, and how long to earn that back? | `Burn to join`, `Payback` |
| Is this subnet about to be deregistered? | `Prune risk` |
| What does the top of the emission curve look like? | Subnet drawer → top 10 miners by emission |

The uncomfortable headline the data keeps producing: **29,049 miner UIDs are registered, 6,063 earn anything.** On most competitive subnets more than 90% of UIDs sit at zero emission. An "average income" leaderboard without that denominator is a lie, so the denominator is a column, not a footnote.

## Metrics (all arithmetic on measured chain state)

```
epochs/day        = 86400 / (tempo × 12s)
miner α/day       = Σ per-UID emission_α × epochs/day
miner pool $/day  = miner α/day × α_price_TAO × TAO/USD
income / winner   = miner pool $/day ÷ miners with emission > 0
payback (days)    = burn_TAO × TAO/USD ÷ income / winner
```

Per-UID emission comes from the chain's own yield table for that subnet, so the miner/validator split is measured rather than assumed from the default 41/41/18 emission weights.

**Payback excludes** hardware, hosting, electricity, dev time, deregistration loss and alpha price movement on exit. It is the gross-emission recovery time of the registration burn alone — an upper bound on attractiveness, never a return.

**Income per winning miner** is the mean across miners that earn something. It is not the median miner's income and it is not what a new UID would earn: emission is metered by validator weights, and a new UID starts at zero weight. The `Median winning miner` column and the `Winning miners` count exist to keep that visible.

## Data

Public, keyless [metagraph.sh](https://metagraph.sh) API, read at bake time:

| Endpoint | Used for |
| --- | --- |
| `/api/v1/economics` | registration cost, miner/validator counts, α price + 1h/1d/7d/1m windows, open seats, market cap |
| `/api/v1/chain/burn` | live registration burn per subnet |
| `/api/v1/chain/emission-pipeline` | post-gate share of block emission per subnet |
| `/api/v1/chain/deregistration-ranking` | prune rank / immunity |
| `/api/v1/subnets` | registry identity, tempo, categories, repo, hardware floor |
| `/api/v1/subnets/{netuid}/yield` | per-UID stake + emission — the mining table, ×129 |
| `/api/v1/network/tao-usd` | TAO/USD reference rate |

Roughly 135 requests, ~5 MB, ~20 s. No key, no account, no cost.

## Architecture

- `index.html` — one file, zero dependencies, **data baked in**. No `fetch`, no CORS, no third-party script. It renders identically on a locked-down corporate network with the network cable pulled.
- `scripts/refresh.py` — stdlib-only rebake: fetch → compile → write `data/subnets.json` → inject JSON into `index.html` between `/*DATA_START*/` and `/*DATA_END*/`.
- `.github/workflows/build-and-deploy.yml` — hourly: rebake, commit the artifact only when the values actually moved, deploy `_site` to Pages.

## Run it

```bash
python3 scripts/refresh.py --dry-run   # fetch + compile, print a summary, write nothing
python3 scripts/refresh.py             # fetch + bake into index.html and data/subnets.json
open index.html                        # no server required
```

`data/subnets.json` is a stable machine-readable export (`schema: tao-mining-desk/1`) if you want the compiled metrics without the HTML.

## Licence

MIT. Data belongs to the chain; the API wrapper belongs to metagraph.sh.

Not financial advice. Bittensor subnets are high-variance: a UID can earn nothing for months and a subnet can be deregistered out from under you.
