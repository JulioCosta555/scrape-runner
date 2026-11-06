#!/usr/bin/env python3
"""
pipeline.py - runs your daily scrape on GitHub Actions, with everything stored in Google Drive.

    python pipeline.py plan [--only "USA/MLS,USA/USL"]   -> the list of leagues, for GitHub
    python pipeline.py run                                 -> one league (LEAGUE_JSON env var)

Your scripts are NOT in this repository. Every run downloads them from Google Drive:

    My Drive/scrape/_code/
        leaguetest5.py  fotmobshots.py  scrape_merger3.py
        scrape_leagues.csv
        Events.csv  Opta_Ids.csv           (your Opta definitions; any subfolder is fine)
        requirements.txt                   (optional: extra pip packages, e.g. LanusStats)

Each league lives in My Drive/scrape/<country>/<league>/:
    output.tar   all the Scoresway JSON, packed into one file (fast to move every day)
    shots.tar    all the fotmob shots JSON, packed the same way
    merged/      the parquet files, as normal files you can open
    merged_narrow/  only if events_shape = narrow

One league, one run:
    1. download its output.tar / shots.tar and unpack, so the scrapers skip matches already done
    2. leaguetest5.py, then fotmobshots.py if the league has a fotmob id
    3. scrape_merger3.py
    4. upload whatever changed
The scrapers are stopped before GitHub's 6-hour limit so progress is always saved;
an unfinished league carries on the next day.
"""
from __future__ import annotations

import argparse
import csv
import gzip
import json
import os
import re
import shlex
import signal
import subprocess
import sys
import tarfile
import threading
import time
from dataclasses import asdict, dataclass
from pathlib import Path


def _env(name: str, default: str) -> str:
    value = os.environ.get(name)
    return value if value not in (None, "") else default


# ----------------------------------------------------------------------------- settings
REMOTE = _env("DRIVE_REMOTE", "gdrive")          # rclone remote name
ROOT = _env("DRIVE_ROOT", "scrape")              # top folder in My Drive
LEAGUES_CSV = _env("LEAGUES_CSV", "scrape_leagues.csv")
HERE = Path(__file__).resolve().parent
CODE = Path(_env("CODE_DIR", str(HERE / "code")))   # scripts downloaded from Drive
WORK = Path(_env("WORK_DIR", str(HERE / "work")))   # league data while the job runs
RCLONE = _env("RCLONE_BIN", "rclone")

DEFAULT_SEASON = _env("DEFAULT_SEASON", "2026")
FOTMOB_TRANSPORT = _env("FOTMOB_TRANSPORT", "page")
FOTMOB_LAYOUT = _env("FOTMOB_LAYOUT", "flat")    # shots/round_NN/, the depth the merger reads
MERGE_FORMAT = _env("MERGE_FORMAT", "parquet")
# Time budget inside GitHub's 6-hour job limit (minutes from the start of the job)
EVENTS_DEADLINE_MIN = float(_env("EVENTS_DEADLINE_MIN", "210"))
SHOTS_DEADLINE_MIN = float(_env("SHOTS_DEADLINE_MIN", "270"))

REQUIRED_SCRIPTS = ("leaguetest5.py", "fotmobshots.py", "scrape_merger3.py")
ARCHIVED = ("output", "shots")

COLUMN_ALIASES = {
    "folder": ["folder"],
    "country": ["country"],
    "league": ["league", "league_name", "name"],
    "scoresway_url": ["scoresway_url", "scoresway", "scoresway_league"],
    "fotmob_league": ["fotmob_league", "fotmob_id", "fotmob_league_id"],
    "fotmob_slug": ["fotmob_slug", "slug", "league_slug"],
    "season": ["season", "fotmob_season"],
    "events_shape": ["events_shape", "shape"],
    "active": ["active", "enabled"],
}
NULLS = {"", "nan", "none", "null", "na", "n/a", "-"}
FALSY = {"0", "false", "no", "n", "off"}


def log(msg: str) -> None:
    print(msg, file=sys.stderr, flush=True)


def gh(kind: str, msg: str) -> None:
    """GitHub annotation: shows on the run's summary page."""
    print(f"::{kind}::{msg}", flush=True)


# ----------------------------------------------------------------------------- leagues
@dataclass
class League:
    name: str               # Drive folder, e.g. "Chile/Primera Division"
    scoresway_league: str   # "mls-2026/6i6n0jkbh9zzij6s8htfjh2j8"
    fotmob_id: str          # "" = no shots
    fotmob_slug: str
    season: str
    events_shape: str       # "" or "narrow"

    @property
    def has_fotmob(self) -> bool:
        return bool(self.fotmob_id)


_ID_RE = re.compile(r"^[A-Za-z0-9]{20,}$")


def parse_scoresway(value: str) -> str:
    path = re.sub(r"^https?://[^/]+", "", value.strip()).split("?")[0].split("#")[0]
    parts = [p for p in path.split("/") if p]
    for i, part in enumerate(parts):
        if i > 0 and _ID_RE.match(part):
            return f"{parts[i - 1]}/{part}"
    raise ValueError(f"can't find '<competition-slug>/<id>' in scoresway_url {value!r}")


def _clean(value) -> str:
    value = ("" if value is None else str(value)).strip()
    return "" if value.lower() in NULLS else value


def _intish(value) -> str:
    value = _clean(value)
    return value[:-2] if re.fullmatch(r"\d+\.0", value) else value


def _safe_path(value: str) -> str:
    parts = [re.sub(r'[\\:*?"<>|]+', "", p).strip().strip(".") for p in value.split("/")]
    return "/".join(p for p in parts if p)


def fotmob_season(season: str) -> str:
    """CSV '2026-2027' -> Fotmob's label '2026/2027'."""
    m = re.fullmatch(r"(\d{4})\s*-\s*(\d{4})", season.strip())
    return f"{m.group(1)}/{m.group(2)}" if m else season


def load_leagues(path: Path) -> list[League]:
    with open(path, newline="", encoding="utf-8-sig") as f:
        sample = f.read(4096)
        f.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=",;\t")
        except csv.Error:
            dialect = csv.excel
        reader = csv.DictReader(f, dialect=dialect)
        headers = {h.strip().lower(): h for h in (reader.fieldnames or []) if h}
        col = {k: next((headers[a] for a in al if a in headers), None) for k, al in COLUMN_ALIASES.items()}
        if not col["scoresway_url"]:
            raise SystemExit(f"{path.name}: no scoresway_url column. Headers: {list(headers)}")

        leagues, seen = [], set()
        for lineno, row in enumerate(reader, start=2):
            def get(key: str) -> str:
                return _clean(row.get(col[key])) if col[key] else ""

            if col["active"] and get("active").lower() in FALSY:
                continue
            raw_sw = get("scoresway_url")
            if not raw_sw:
                if any(_clean(v) for v in row.values()):
                    gh("warning", f"{path.name} line {lineno}: no scoresway_url - skipped")
                continue
            try:
                sw = parse_scoresway(raw_sw)
            except ValueError as e:
                gh("warning", f"{path.name} line {lineno}: {e} - skipped")
                continue
            fid, slug = _intish(get("fotmob_league")), get("fotmob_slug")
            if fid and not slug:
                gh("warning", f"{path.name} line {lineno}: fotmob_league={fid} but no fotmob_slug - skipped")
                continue
            name = _safe_path(get("folder") or "/".join(p for p in (get("country"), get("league")) if p)
                              or sw.split("/")[0])
            if name in seen:
                gh("warning", f"{path.name} line {lineno}: duplicate folder '{name}' - skipped")
                continue
            seen.add(name)
            leagues.append(League(name, sw, fid, slug if fid else "",
                                  _intish(get("season")) or DEFAULT_SEASON, get("events_shape")))
    if not leagues:
        raise SystemExit(f"{path.name}: no usable league rows")
    return leagues


# ----------------------------------------------------------------------------- rclone / Drive
RCLONE_FLAGS = ["--retries", "5", "--low-level-retries", "20", "--transfers", "8",
                "--stats", "60s", "--stats-one-line"]


def rclone(*args: str, ok_codes: tuple[int, ...] = (0,), capture: bool = False) -> subprocess.CompletedProcess:
    cmd = [RCLONE, *args, *RCLONE_FLAGS]
    log("rclone " + shlex.join(args))
    res = subprocess.run(cmd, text=True, capture_output=capture)
    if res.returncode not in ok_codes:
        detail = (res.stderr or "").strip()[-2000:] if capture else ""
        raise RuntimeError(f"rclone {args[0]} failed (exit {res.returncode}) {detail}")
    return res


def remote(*parts: str) -> str:
    return f"{REMOTE}:" + "/".join(p.strip("/") for p in (ROOT, *parts) if p)


def remote_listing(path: str) -> dict[str, bool]:
    """name -> is_dir for the direct children of a Drive folder ({} if it doesn't exist yet)."""
    res = rclone("lsjson", "--max-depth", "1", path, ok_codes=(0, 3), capture=True)
    if res.returncode == 3:
        return {}
    return {e["Name"]: e["IsDir"] for e in json.loads(res.stdout or "[]")}


def fetch_code(only: str | None = None) -> None:
    CODE.mkdir(parents=True, exist_ok=True)
    args = ["copy", remote("_code"), str(CODE)]
    if only:
        args += ["--include", only]
    rclone(*args)


def find_code_file(name: str) -> Path:
    hits = [p for p in CODE.rglob(name) if not p.name.startswith("._")]
    if not hits:
        raise SystemExit(f"'{name}' not found in Drive folder {ROOT}/_code")
    return hits[0]


# ----------------------------------------------------------------------------- local files
def data_files(d: Path) -> list[Path]:
    if not d.is_dir():
        return []
    return sorted(p for p in d.rglob("*")
                  if p.is_file() and not any(part.startswith(".") for part in p.relative_to(d).parts)
                  and not p.name.endswith(".part"))


def changed_since(d: Path, t: float) -> bool:
    return any(p.stat().st_mtime >= t for p in data_files(d))


def unpack(tar_path: Path, into: Path) -> None:
    with tarfile.open(tar_path) as tf:
        try:
            tf.extractall(into, filter="data")
        except TypeError:  # python < 3.12
            tf.extractall(into)


def pack(league_dir: Path, part: str, tar_path: Path) -> int:
    files = data_files(league_dir / part)
    with tarfile.open(tar_path, "w") as tf:
        for p in files:
            tf.add(p, arcname=str(p.relative_to(league_dir)))
    return len(files)


def repair_partial_writes(d: Path, since: float) -> int:
    """A scraper stopped mid-write can leave a truncated .json(.gz) that it would later
    treat as complete. Check every file written in this run; delete broken ones."""
    removed = 0
    for p in data_files(d):
        if p.stat().st_mtime < since or not p.name.endswith((".json", ".json.gz")):
            continue
        try:
            raw = gzip.open(p, "rb").read() if p.name.endswith(".gz") else p.read_bytes()
            json.loads(raw)
        except Exception:
            p.unlink()
            removed += 1
    if removed:
        gh("warning", f"removed {removed} incomplete file(s) in {d.name}; they will be fetched again")
    return removed


# ----------------------------------------------------------------------------- running scripts
class StepFailed(RuntimeError):
    pass


def run(cmd: list[str], title: str, deadline: float | None = None) -> str:
    """Run a script, streaming output into the GitHub log. Returns 'ok' or 'timeout'."""
    print(f"::group::{title}", flush=True)
    print("$ " + shlex.join(cmd), flush=True)
    proc = subprocess.Popen(cmd, cwd=str(CODE), stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            text=True, errors="replace", env={**os.environ, "PYTHONUNBUFFERED": "1"})

    def pump():
        assert proc.stdout is not None
        for line in proc.stdout:
            print(line, end="", flush=True)

    t = threading.Thread(target=pump, daemon=True)
    t.start()
    status = "ok"
    try:
        timeout = None if deadline is None else max(1.0, deadline - time.time())
        rc = proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        status = "timeout"
        proc.send_signal(signal.SIGINT)  # lets the scraper close Chrome cleanly
        try:
            proc.wait(timeout=90)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
        rc = 0
    t.join(timeout=10)
    print("::endgroup::", flush=True)
    if status == "timeout":
        gh("warning", f"{title}: stopped at the time limit - progress is saved, it continues tomorrow")
    elif rc != 0:
        raise StepFailed(f"{title} failed with exit code {rc}")
    return status


# ----------------------------------------------------------------------------- commands
def cmd_plan(args) -> None:
    fetch_code(only=LEAGUES_CSV)
    leagues = load_leagues(find_code_file(LEAGUES_CSV))
    wanted = [t.strip().strip("/").lower() for t in (args.only or "").split(",") if t.strip()]
    if wanted:
        def match(lg: League, t: str) -> bool:
            n = lg.name.lower()
            return n == t or n.startswith(t + "/")
        unknown = [t for t in wanted if not any(match(lg, t) for lg in leagues)]
        if unknown:
            raise SystemExit(f"Unknown league(s): {', '.join(unknown)}. "
                             f"Names look like: {', '.join(l.name for l in leagues[:4])}")
        leagues = [lg for lg in leagues if any(match(lg, t) for t in wanted)]
    log(f"{len(leagues)} league(s): " + ", ".join(
        f"{l.name}{' +shots' if l.has_fotmob else ''}" for l in leagues))
    print("matrix=" + json.dumps([asdict(l) for l in leagues]))


def cmd_run(args) -> None:
    start = time.time()
    lg = League(**json.loads(args.league or os.environ["LEAGUE_JSON"]))
    log(f"League: {lg.name}  scoresway={lg.scoresway_league}  "
        f"fotmob={lg.fotmob_id or '-'} {lg.fotmob_slug}  season={lg.season}")

    # --- your scripts + Opta definitions from Drive
    fetch_code()
    for s in REQUIRED_SCRIPTS:
        find_code_file(s)
    events_def, quals_def = find_code_file("Events.csv"), find_code_file("Opta_Ids.csv")
    extra_req = CODE / "requirements.txt"
    if extra_req.exists():
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", "-r", str(extra_req)], check=True)

    # --- this league's existing data, so the scrapers only fetch new matches
    league_dir = WORK / lg.name
    league_dir.mkdir(parents=True, exist_ok=True)
    tmp = WORK / "_tmp"
    tmp.mkdir(exist_ok=True)
    listing = remote_listing(remote(lg.name))
    parts = ["output"] + (["shots"] if lg.has_fotmob else [])
    before: dict[str, int] = {}
    migrated: list[str] = []
    for part in parts:
        if listing.get(f"{part}.tar") is False:
            rclone("copyto", remote(lg.name, f"{part}.tar"), str(tmp / f"{part}.tar"))
            unpack(tmp / f"{part}.tar", league_dir)
            (tmp / f"{part}.tar").unlink()
        elif listing.get(part) is True:  # files you uploaded as a normal folder: converted once
            rclone("copy", remote(lg.name, part), str(league_dir / part))
            migrated.append(part)
        before[part] = len(data_files(league_dir / part))
        log(f"{part}: {before[part]} file(s) already in Drive")

    problems: list[str] = []
    s = lambda m: str(league_dir / m)  # noqa: E731

    # --- 1. Scoresway events
    try:
        run([sys.executable, "leaguetest5.py", "--league", lg.scoresway_league,
             "--out", s("output"), "--gzip"],
            "leaguetest5.py (events)", deadline=start + EVENTS_DEADLINE_MIN * 60)
    except StepFailed as e:
        problems.append(str(e))
    repair_partial_writes(league_dir / "output", start)

    # --- 2. fotmob shots
    has_shots = False
    if lg.has_fotmob:
        deadline = start + SHOTS_DEADLINE_MIN * 60
        if deadline - time.time() < 120:
            gh("warning", "no time left for shots today - they continue tomorrow")
        else:
            try:
                run([sys.executable, "fotmobshots.py", "--league", lg.fotmob_id, "--slug", lg.fotmob_slug,
                     "--season", fotmob_season(lg.season), "--out", s("shots"),
                     "--transport", FOTMOB_TRANSPORT, "--layout", FOTMOB_LAYOUT],
                    "fotmobshots.py (shots)", deadline=deadline)
            except StepFailed as e:
                problems.append(str(e))
            repair_partial_writes(league_dir / "shots", start)
        has_shots = any(not p.name.startswith("._") for p in (league_dir / "shots").rglob("*shots*.json"))
        if not has_shots:
            log("no shot files - merging without shots")

    # --- 3. merge (an empty --shots-dir means "no shots"; leaving it out would use the
    #        script's built-in default folder)
    merged_dirs: list[Path] = []
    if data_files(league_dir / "output"):
        base = [sys.executable, "scrape_merger3.py", "--raw-dir", s("output"),
                "--shots-dir", s("shots") if has_shots else "",
                "--events-def", str(events_def), "--qualifiers-def", str(quals_def),
                "--format", MERGE_FORMAT]
        variants = [("merged", [])]
        if lg.events_shape:
            variants.append((f"merged_{lg.events_shape}", ["--events-shape", lg.events_shape]))
        for folder, extra in variants:
            try:
                run(base + ["--outdir", s(folder), *extra], f"scrape_merger3.py -> {folder}")
                merged_dirs.append(league_dir / folder)
            except StepFailed as e:
                problems.append(str(e))
    else:
        problems.append("no Scoresway files at all - nothing to merge")

    # --- 4. save back to Drive
    for part in parts:
        n = len(data_files(league_dir / part))
        if n == 0:
            continue
        if n < before[part]:
            problems.append(f"{part}: {n} files now vs {before[part]} before - NOT uploaded, to be safe")
            continue
        if part not in migrated and n == before[part] and not changed_since(league_dir / part, start):
            log(f"{part}: nothing new")
            continue
        count = pack(league_dir, part, tmp / f"{part}.tar")
        dest = remote(lg.name, f"{part}.tar")
        rclone("copyto", str(tmp / f"{part}.tar"), dest + ".uploading")
        # delete-then-rename instead of overwriting, so Drive doesn't keep old versions
        # of a big file that would eat your storage
        rclone("deletefile", "--drive-use-trash=false", dest, ok_codes=(0, 3, 4))
        rclone("moveto", dest + ".uploading", dest)
        (tmp / f"{part}.tar").unlink()
        log(f"{part}: saved {count} file(s) as {part}.tar (+{n - before[part]} new)")
        if part in migrated:  # the loose-file copy is now inside the .tar
            rclone("purge", remote(lg.name, part), ok_codes=(0, 3))
            log(f"{part}: moved your uploaded folder into {part}.tar (old folder is in Drive's trash)")

    for d in merged_dirs:
        if not data_files(d):
            continue
        dest = remote(lg.name, d.name)
        rclone("purge", "--drive-use-trash=false", dest + ".uploading", ok_codes=(0, 3))
        rclone("copy", str(d), dest + ".uploading")
        rclone("purge", "--drive-use-trash=false", dest, ok_codes=(0, 3))
        rclone("moveto", dest + ".uploading", dest)
        log(f"{d.name}: uploaded {len(data_files(d))} file(s)")

    mins = (time.time() - start) / 60
    if problems:
        for p in problems:
            gh("error", f"{lg.name}: {p}")
        sys.exit(1)
    log(f"{lg.name}: done in {mins:.0f} min")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("plan")
    p.add_argument("--only", default="", help="comma-separated league folders, e.g. 'USA/MLS,Brazil'")
    r = sub.add_parser("run")
    r.add_argument("--league", help="league JSON (default: $LEAGUE_JSON)")
    args = ap.parse_args()
    {"plan": cmd_plan, "run": cmd_run}[args.cmd](args)


if __name__ == "__main__":
    main()
