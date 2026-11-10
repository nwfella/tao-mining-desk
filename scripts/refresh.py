#!/usr/bin/env python3
"""
Rebake the TAO Mining Desk artifact.

Fetches the public, keyless metagraph.sh API, compiles a single JSON payload of
per-subnet mining economics, writes data/subnets.json for machine consumers and
injects the same payload into index.html between the DATA markers.

Everything is derived from measured chain state:
  * /economics                     -> registration cost, miner/validator counts,
                                      alpha price + price change windows, open slots
  * /chain/burn                    -> live registration burn per subnet (TAO)
  * /chain/emission-pipeline       -> per-subnet share of block emission
  * /chain/deregistration-ranking  -> prunability rank / immunity
  * /subnets                       -> registry identity, tempo, hardware floor
  * /subnets/{netuid}/yield        -> per-UID stake + emission (the mining table)
  * /network/tao-usd               -> TAO/USD reference rate

No API key. Stdlib only so it runs on a stock GitHub Actions runner.

Usage:
    python3 scripts/refresh.py            # fetch, compile, bake
    python3 scripts/refresh.py --dry-run  # compile only, print a summary
"""

from __future__ import annotations

import argparse
import concurrent.futures as futures
import json
import os
import statistics
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

API = "https://api.metagraph.sh/api/v1"
UA = "tao-mining-desk/1.0 (+https://github.com/nwfella/tao-mining-desk)"
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INDEX = os.path.join(ROOT, "index.html")
DATA_JSON = os.path.join(ROOT, "data", "subnets.json")
BLOCK_SECONDS = 12.0


# --------------------------------------------------------------------------- #
# transport
# --------------------------------------------------------------------------- #
def get(path: str, tries: int = 3, timeout: int = 90) -> dict:
    last: Exception | None = None
    for attempt in range(tries):
        try:
            req = urllib.request.Request(
                API + path, headers={"User-Agent": UA, "Accept": "application/json"}
            )
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8", "replace"))
        except Exception as exc:  # noqa: BLE001 - retried, then surfaced
            last = exc
            time.sleep(1.5 * (attempt + 1))
    raise RuntimeError(f"GET {path} failed: {type(last).__name__}: {last}")


def get_many(paths: list[tuple[int, str]], workers: int = 8) -> dict[int, dict]:
    out: dict[int, dict] = {}
    errors: list[str] = []

    def one(item: tuple[int, str]):
        netuid, path = item
        try:
            return netuid, get(path)
        except Exception as exc:  # noqa: BLE001
            return netuid, {"__error__": str(exc)}

    with futures.ThreadPoolExecutor(max_workers=workers) as pool:
        for netuid, payload in pool.map(one, paths):
            if "__error__" in payload:
                errors.append(f"{netuid}: {payload['__error__']}")
            else:
                out[netuid] = payload

    if errors:
        print(f"  ! {len(errors)} subnet feed(s) failed: {errors[:3]}", file=sys.stderr)
    return out


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
def num(value, default=None):
    """Coerce to float, mapping None/''/NaN-ish to default."""
    if value is None or value == "" or isinstance(value, bool):
        return default
    try:
        f = float(value)
    except (TypeError, ValueError):
        return default
    return f if f == f and abs(f) != float("inf") else default


def rows_of(data, *names):
    """Pull the row collection out of an endpoint envelope."""
    if isinstance(data, list):
        return data
    for name in names:
        if isinstance(data.get(name), list):
            return data[name]
    raise KeyError(f"no row collection in {list(data)}")


def pct(value, digits=1):
    return None if value is None else round(value * 100, digits)


# Registry tags that describe curation state, not what the subnet does.
NOISE_TAGS = {
    "baseline-augmented", "baseline-curated", "identity-reviewed", "identity-drift",
    "official-api", "official-docs", "official-source-repo", "official-website",
    "official-dashboard", "community-docs", "maintainer-reviewed", "verified-compute",
    "first-party", "curated", "probed", "active",
}

DOMAIN_MAP = {
    "inference": "inference", "llm-serving": "inference", "language-models": "inference",
    "model-serving": "inference", "text-generation": "inference", "model-artifacts": "inference",
    "compute": "compute", "gpu": "compute", "gpu-rental": "compute", "serverless": "compute",
    "decentralized-compute": "compute",
    "training": "training", "decentralized-training": "training", "fine-tuning": "training",
    "agents": "agents", "agent-frameworks": "agents", "agent-marketplace": "agents",
    "data": "data", "datasets": "data", "scraping": "data", "indexing": "data",
    "finance": "finance", "trading": "finance", "defi": "finance", "quant": "finance",
    "prediction": "prediction", "prediction-markets": "prediction", "forecasting": "prediction",
    "media": "media", "image-generation": "media", "video": "media", "audio": "media", "music": "media",
    "privacy": "privacy", "tee": "privacy", "confidential-compute": "privacy",
    "security": "security", "verification": "security", "auditing": "security", "cybersecurity": "security",
    "storage": "storage", "file-storage": "storage",
    "robotics": "robotics",
    "science": "science", "research": "science", "biology": "science", "health": "science",
    "search": "search", "retrieval": "search",
}


def domain_of(reg: dict) -> tuple[str, list[str]]:
    """Bucket a subnet into a domain from the registry's own tags.

    Only the canonical buckets are used for grouping so the filter stays usable;
    the raw registry tags are kept alongside so the drawer can show them
    verbatim. A subnet the registry does not classify stays 'other' — that is
    an absence of a label, not a guess.
    """
    if reg.get("subnet_type") == "root":
        return "root", []
    tags = [t for t in (reg.get("categories") or []) if t not in NOISE_TAGS]
    derived = reg.get("derived_categories") or []
    if derived:
        return derived[0], tags
    for t in tags:
        if t in DOMAIN_MAP:
            return DOMAIN_MAP[t], tags
    return "other", tags

# --------------------------------------------------------------------------- #
# compile
# --------------------------------------------------------------------------- #
def compile_payload(raw: dict) -> dict:
    registry = {s["netuid"]: s for s in rows_of(raw["registry"]["data"], "subnets")}
    economics = {r["netuid"]: r for r in rows_of(raw["economics"]["data"], "subnets", "rows")}
    burn = {r["netuid"]: num(r.get("burn_tao")) for r in rows_of(raw["burn"]["data"], "subnets", "rows")}

    pipeline_rows = {r["netuid"]: r for r in rows_of(raw["pipeline"]["data"], "subnets")}
    pipeline = raw["pipeline"]["data"]

    dereg = raw["deregistration"]["data"]
    ranked = {r["netuid"]: r for r in rows_of(dereg, "ranked")}
    immune = {r["netuid"]: r for r in (dereg.get("immune") or []) if isinstance(r, dict)}

    tao_usd_info = raw["tao_usd"]["data"].get("latest") or {}
    tao_usd = num(tao_usd_info.get("usd_per_tao")) or 0.0

    subnets = []
    for netuid in sorted(registry):
        reg = registry[netuid]
        eco = economics.get(netuid, {})
        y = (raw["yield"].get(netuid) or {}).get("data") or {}
        pipe = pipeline_rows.get(netuid, {})
        dr = ranked.get(netuid) or immune.get(netuid) or {}

        tempo = num(reg.get("tempo")) or num(eco.get("tempo")) or 360
        epochs_day = 86400.0 / (tempo * BLOCK_SECONDS)

        price_tao = num(eco.get("alpha_price_tao"))
        alpha_usd = (price_tao or 0.0) * tao_usd

        neurons = y.get("neurons") or []
        miners = sorted(
            (n for n in neurons if n.get("role") == "miner"),
            key=lambda n: num(n.get("emission_tao")) or 0.0,
            reverse=True,
        )
        validators = [n for n in neurons if n.get("role") == "validator"]

        miner_epoch = sum(num(n.get("emission_tao")) or 0.0 for n in miners)
        val_epoch = sum(num(n.get("emission_tao")) or 0.0 for n in validators)
        earning = [n for n in miners if (num(n.get("emission_tao")) or 0.0) > 0]

        miner_alpha_day = miner_epoch * epochs_day
        miner_pool_usd = miner_alpha_day * alpha_usd
        validator_pool_usd = val_epoch * epochs_day * alpha_usd

        per_earning_usd = miner_pool_usd / len(earning) if earning else 0.0
        earn_emissions = [(num(n.get("emission_tao")) or 0.0) * epochs_day * alpha_usd for n in earning]
        median_earn_usd = statistics.median(earn_emissions) if earn_emissions else 0.0
        p90_earn_usd = (
            sorted(earn_emissions)[max(0, int(len(earn_emissions) * 0.9) - 1)] if earn_emissions else 0.0
        )
        top_earn_usd = earn_emissions[0] if earn_emissions else 0.0
        top5_share = (
            sum((num(n.get("emission_tao")) or 0.0) for n in miners[:5]) / miner_epoch if miner_epoch else None
        )

        burn_tao = burn.get(netuid, num(eco.get("registration_cost_tao")))
        burn_usd = (burn_tao or 0.0) * tao_usd
        payback_days = burn_usd / per_earning_usd if per_earning_usd > 0 else None

        emission_share = num(pipe.get("final_share")) or num(eco.get("emission_share"))
        subnet_emission_usd_day = (num(y.get("total_emission_alpha")) or 0.0) * epochs_day * alpha_usd

        cat, raw_tags = domain_of(reg)
        top_miners = [
            {
                "uid": n.get("uid"),
                "hotkey": n.get("hotkey"),
                "alpha_day": round((num(n.get("emission_tao")) or 0.0) * epochs_day, 6),
                "usd_day": round((num(n.get("emission_tao")) or 0.0) * epochs_day * alpha_usd, 4),
                "stake_tao": round(num(n.get("stake_tao")) or 0.0, 4),
                "yield": round(num(n.get("yield")) or 0.0, 6),
                "share": round(((num(n.get("emission_tao")) or 0.0) / miner_epoch) if miner_epoch else 0.0, 4),
            }
            for n in miners[:10]
        ]

        subnets.append(
            {
                "netuid": netuid,
                "name": reg.get("name") or f"SN{netuid}",
                "symbol": reg.get("symbol"),
                "type": reg.get("subnet_type"),
                "tags": raw_tags[:5],
                "cat": cat,
                "website": reg.get("website_url"),
                "repo": reg.get("source_repo"),
                "logo": reg.get("logo_url"),
                "stars": reg.get("github_stars"),
                "gpu": reg.get("gpu_required"),
                "vram": reg.get("min_vram_gb"),
                "tempo": tempo,
                "epoch_min": round(tempo * BLOCK_SECONDS / 60.0, 1),
                "epochs_day": round(epochs_day, 4),
                "max_uids": eco.get("max_uids") or reg.get("participant_count"),
                "max_validators": eco.get("max_validators"),
                "price_tao": price_tao,
                "price_usd": round(alpha_usd, 8),
                "chg_1h": num(eco.get("alpha_price_change_1h")),
                "chg_1d": num(eco.get("alpha_price_change_1d")),
                "chg_7d": num(eco.get("alpha_price_change_7d")),
                "chg_1m": num(eco.get("alpha_price_change_1m")),
                "burn_tao": num(burn_tao),
                "burn_usd": round(burn_usd, 4),
                "reg_allowed": eco.get("registration_allowed"),
                "open_slots": eco.get("open_slots"),
                "miner_count": len(miners) if miners else (eco.get("miner_count") or 0),
                "validator_count": len(validators) if validators else (eco.get("validator_count") or 0),
                "earning_miners": len(earning),
                "zero_pct": round(1 - (len(earning) / len(miners)), 4) if miners else None,
                "miner_alpha_day": round(miner_alpha_day, 4),
                "miner_pool_usd": round(miner_pool_usd, 2),
                "validator_pool_usd": round(validator_pool_usd, 2),
                "per_miner_usd": round(per_earning_usd, 4),
                "median_miner_usd": round(median_earn_usd, 4),
                "p90_miner_usd": round(p90_earn_usd, 4),
                "top_miner_usd": round(top_earn_usd, 4),
                "top5_share": round(top5_share, 4) if top5_share is not None else None,
                "payback_days": round(payback_days, 2) if payback_days is not None else None,
                "emission_share": round(emission_share, 6) if emission_share else None,
                "emission_usd_day": round(subnet_emission_usd_day, 2),
                "mcap_tao": num(eco.get("alpha_market_cap_tao")),
                "mcap_usd": round((num(eco.get("alpha_market_cap_tao")) or 0.0) * tao_usd, 0),
                "volume_tao": num(eco.get("subnet_volume_tao")),
                "dereg_rank": dr.get("rank"),
                "blocks_to_prune": dr.get("blocks_until_prunable"),
                "immune": dr.get("immune"),
                "miner_readiness": eco.get("miner_readiness"),
                "top_miners": top_miners,
            }
        )

    app = [s for s in subnets if s["type"] != "root"]
    earning_rows = [s for s in app if s["per_miner_usd"] > 0]
    top10 = sorted(app, key=lambda s: -(s["emission_share"] or 0))[:10]

    payload = {
        "schema": "tao-mining-desk/1",
        "baked_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "chain": {
            "block": pipeline.get("chain_state", {}).get("block"),
            "block_emission_tao": num(pipeline.get("block_emission_tao")),
            "network_immunity_blocks": pipeline.get("chain_state", {}).get("network_immunity_period"),
            "total_issuance_tao": num(pipeline.get("chain_state", {}).get("total_issuance_tao")),
            "pipeline_verified": (pipeline.get("verification") or {}).get("verified"),
        },
        "tao_usd": tao_usd,
        "tao_usd_observed_at": tao_usd_info.get("observed_at"),
        "tao_usd_stale": tao_usd_info.get("stale"),
        "tao_usd_basis": tao_usd_info.get("price_basis"),
        "network": {
            "subnet_count": len(app),
            "uid_total": sum(s["miner_count"] + s["validator_count"] for s in app),
            "miner_total": sum(s["miner_count"] for s in app),
            "validator_total": sum(s["validator_count"] for s in app),
            "earning_miners": sum(s["earning_miners"] for s in app),
            "miner_pool_usd_day": round(sum(s["miner_pool_usd"] for s in app), 2),
            "validator_pool_usd_day": round(sum(s["validator_pool_usd"] for s in app), 2),
            "emission_usd_day": round(sum(s["emission_usd_day"] for s in app), 2),
            "median_earning_miner_usd": round(
                statistics.median([s["per_miner_usd"] for s in earning_rows]), 4
            )
            if earning_rows
            else 0.0,
            "open_slots_total": sum(int(s["open_slots"] or 0) for s in app),
            "cheapest_burn_tao": min((s["burn_tao"] for s in app if s["burn_tao"] is not None), default=None),
            "median_burn_tao": round(
                statistics.median([s["burn_tao"] for s in app if s["burn_tao"] is not None]), 6
            ),
            "top10_emission_share": round(sum(s["emission_share"] or 0 for s in top10), 4),
            "subnets_with_earning_miners": len(earning_rows),
            "zero_emission_blocks": sum(s["earning_miners"] == 0 for s in app),
        },
        "subnets": subnets,
    }
    return payload


def summarise(payload: dict) -> None:
    net = payload["network"]
    print(f"  TAO ${payload['tao_usd']:,.2f}  block {payload['chain']['block']}")
    print(f"  subnets {net['subnet_count']}  miners {net['miner_total']:,}  earning {net['earning_miners']:,}")
    print(f"  miner pool ${net['miner_pool_usd_day']:,.0f}/day  median earning miner ${net['median_earning_miner_usd']:,.2f}/day")
    top = sorted(payload["subnets"], key=lambda s: -(s["per_miner_usd"] or 0))[:5]
    for s in top:
        print(f"    SN{s['netuid']:<3} {s['name'][:22]:<22} ${s['per_miner_usd']:>9,.2f}/miner/day  burn {s['burn_tao']} TAO")


def bake(payload: dict) -> None:
    # "<\/" is a valid JSON escape and keeps the blob safe inside <script>
    blob = json.dumps(payload, separators=(",", ":"), ensure_ascii=False, allow_nan=False).replace("</", "<\\/")
    os.makedirs(os.path.dirname(DATA_JSON), exist_ok=True)
    with open(DATA_JSON, "w", encoding="utf-8", newline="\n") as fh:
        json.dump(payload, fh, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
        fh.write("\n")

    with open(INDEX, encoding="utf-8") as fh:
        html = fh.read()
    start, end = "/*DATA_START*/", "/*DATA_END*/"
    if start not in html or end not in html:
        raise SystemExit("index.html is missing the /*DATA_START*/ … /*DATA_END*/ markers")
    head, rest = html.split(start, 1)
    _, tail = rest.split(end, 1)
    html = head + start + blob + end + tail
    with open(INDEX, "w", encoding="utf-8", newline="\n") as fh:
        fh.write(html)
    print(f"  baked {len(blob):,} bytes into index.html + data/subnets.json")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="compile and summarise, do not write")
    args = ap.parse_args()

    print("fetching metagraph.sh …")
    raw = {
        "tao_usd": get("/network/tao-usd"),
        "registry": get("/subnets?limit=1000"),
        "economics": get("/economics?limit=1000"),
        "burn": get("/chain/burn"),
        "pipeline": get("/chain/emission-pipeline"),
        "deregistration": get("/chain/deregistration-ranking"),
    }
    netuids = sorted(s["netuid"] for s in rows_of(raw["registry"]["data"], "subnets"))
    print(f"  {len(netuids)} subnets; pulling per-UID yield tables …")
    t0 = time.time()
    raw["yield"] = get_many([(n, f"/subnets/{n}/yield") for n in netuids])
    print(f"  yield tables in {time.time() - t0:.1f}s")

    expected = {n for n in netuids if raw["yield"].get(n, {}).get("data", {}).get("neurons") is not None}
    missing = sorted(set(netuids) - expected)
    if len(missing) > max(3, len(netuids) * 0.1):
        raise SystemExit(f"too many subnets without a yield table ({len(missing)}): {missing[:10]}")

    payload = compile_payload(raw)
    print("compiled:")
    summarise(payload)
    if not args.dry_run:
        bake(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
