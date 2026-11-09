#!/usr/bin/env python3
"""
pipeline.py - runs your daily scrape on GitHub Actions, with everything stored in Google Drive.

    python pipeline.py plan [--only "USA/MLS,USA/USL"]   -> the list of leagues, for GitHub
    python pipeline.py run                                 -> scrape + merge one league (LEAGUE_JSON)
    python pipeline.py xg                                  -> xG model over every league
    python pipeline.py wide                                -> wide table for one league (LEAGUE_JSON)

Your scripts are NOT in this repository. Every run downloads them from Google Drive:

    My Drive/scrape/_code/
        leaguetest5.py  fotmobshots.py  scrape_merger3.py  build_wide_events_loc.py
        scrape_leagues.csv
        Events.csv  Opta_Ids.csv           (your Opta definitions; any subfolder is fine)
        xG_model/run_xg.py  xG_model/xg/...  (the xG model, as in your zip)
        requirements.txt                   (optional: extra pip packages, e.g. LanusStats)

Each league lives in My Drive/scrape/<country>/<league>/:
    output.tar      all the Scoresway JSON, packed into one file (fast to move every day)
    shots.tar       all the fotmob shots JSON, packed the same way
    merged/         scrape_merger3 output (replaced by every scrape)
    merged_narrow/  only if events_shape = narrow
    xG.parquet      this league's shots with model xG        (written by `xg`)
    xt_grid.json    the league's fitted xT surface, reused   (shots leagues, written by `wide`)
    wide_events/    the final wide table                     (written by `wide`)
and the pooled model output in My Drive/scrape/_xG/.

Daily order: run (every league) -> xg (once, all leagues pooled) -> wide (every league).
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
# xG model: extra run_xg.py options, and where its pooled output goes.
# Set XG_EXTRA_ARGS as a GitHub variable (Settings -> Secrets and variables -> Actions ->
# Variables), e.g. "--model lgbm" or "--model lgbm --folds 10". Empty = run_xg.py's
# defaults (--model hgb), the same as your command on the Mac.
XG_EXTRA_ARGS = _env("XG_EXTRA_ARGS", "")
XG_DRIVE_FOLDER = _env("XG_DRIVE_FOLDER", "_xG")
# info tables: the formation-place -> position lookup, kept in Drive _code
INFO_POSITIONS_CSV = _env("INFO_POSITIONS_CSV", "correct_OG_lineups_place_inca.csv")
# analytics: which tools to run, comma-separated (empty = all):
#   pmr, drf, sequences, dashboards, grades, vaep, processdata
ANALYTICS_STEPS = _env("ANALYTICS_STEPS", "")

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
    # Optional extra packages listed in Drive _code/requirements.txt, for every job
    # (scrape, xG, wide), so nothing has to change in GitHub to add one.
    extra_req = CODE / "requirements.txt"
    if not only and extra_req.exists():
        log("installing extra packages from Drive _code/requirements.txt")
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", "-r", str(extra_req)], check=True)


def find_code_file(name: str) -> Path:
    hits = [p for p in CODE.rglob(name)
            if not p.name.startswith("._") and "__MACOSX" not in p.parts]
    if not hits:
        raise SystemExit(f"'{name}' not found in Drive folder {ROOT}/_code")
    return hits[0]


def download_if_exists(parts: tuple[str, ...], name: str, dest: Path) -> bool:
    """Copy Drive file <parts>/<name> to dest. False if it isn't there."""
    if remote_listing(remote(*parts)).get(name) is not False:
        return False
    dest.parent.mkdir(parents=True, exist_ok=True)
    rclone("copyto", remote(*parts, name), str(dest))
    return True


def replace_file(local: Path, dest: str) -> None:
    """Upload, then delete-and-rename instead of overwriting, so Drive doesn't keep
    old versions of big files that would eat your storage."""
    rclone("copyto", str(local), dest + ".uploading")
    rclone("deletefile", "--drive-use-trash=false", dest, ok_codes=(0, 3, 4))
    rclone("moveto", dest + ".uploading", dest)


def replace_dir(local: Path, dest: str) -> int:
    n = len(data_files(local))
    if not n:
        return 0
    rclone("purge", "--drive-use-trash=false", dest + ".uploading", ok_codes=(0, 3))
    rclone("copy", str(local), dest + ".uploading")
    rclone("purge", "--drive-use-trash=false", dest, ok_codes=(0, 3))
    rclone("moveto", dest + ".uploading", dest)
    return n


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


def run(cmd: list[str], title: str, deadline: float | None = None, cwd: Path | None = None) -> str:
    """Run a script, streaming output into the GitHub log. Returns 'ok' or 'timeout'."""
    print(f"::group::{title}", flush=True)
    print("$ " + shlex.join(cmd), flush=True)
    proc = subprocess.Popen(cmd, cwd=str(cwd or CODE), stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
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
        replace_file(tmp / f"{part}.tar", remote(lg.name, f"{part}.tar"))
        (tmp / f"{part}.tar").unlink()
        log(f"{part}: saved {count} file(s) as {part}.tar (+{n - before[part]} new)")
        if part in migrated:  # the loose-file copy is now inside the .tar
            rclone("purge", remote(lg.name, part), ok_codes=(0, 3))
            log(f"{part}: moved your uploaded folder into {part}.tar (old folder is in Drive's trash)")

    for d in merged_dirs:
        n = replace_dir(d, remote(lg.name, d.name))
        if n:
            log(f"{d.name}: uploaded {n} file(s)")

    mins = (time.time() - start) / 60
    if problems:
        for p in problems:
            gh("error", f"{lg.name}: {p}")
        sys.exit(1)
    log(f"{lg.name}: done in {mins:.0f} min")


def model_file_stem(lg: League) -> str:
    """'USA/MLS' + '2026' -> 'USA__MLS_final_events_2026'.

    run_xg.py reads the season and a fallback league name out of the file name
    (season_from_path / league_from_path), and records the name in `source_file`,
    which is how each shot finds its way back to its league folder."""
    base = re.sub(r"[^\w.-]+", "-", lg.name.replace("/", "__")).strip("-")
    return f"{base}_final_events_{lg.season.replace('/', '-')}"


def label_competition_by_country(path: Path, lg: League) -> dict[str, str]:
    """In the model's copy of final_events, matchCompetition -> "<matchCountry> | <name>".

    run_xg.py treats matchCompetition as "the league", and Bolivia, Chile and
    Venezuela are all "Primera División" there, so without this they are one
    league in the leave-one-league-out test, the calibration tables and
    by_league/. Only this temporary copy changes; the files in Drive don't.
    Returns {new label: Opta's original name} so it can be put back afterwards.
    """
    import pandas as pd

    df = pd.read_parquet(path)
    folder_country, folder_league = lg.name.split("/")[0], lg.name.split("/")[-1]
    if "matchCompetition" in df.columns:
        comp = df["matchCompetition"].astype("string").str.strip()
        comp = comp.where(comp.fillna("").ne(""), folder_league)
    else:  # no name at all: the folder from your CSV stands in
        comp = pd.Series(folder_league, index=df.index, dtype="string")
    if "matchCountry" in df.columns:
        country = df["matchCountry"].astype("string").str.strip()
        country = country.where(country.fillna("").ne(""), folder_country)
    else:
        country = pd.Series(folder_country, index=df.index, dtype="string")
    label = country + " | " + comp
    df["matchCompetition"] = label
    df.to_parquet(path, index=False)
    pairs = pd.DataFrame({"label": label, "orig": comp}).drop_duplicates("label")
    log(f"{lg.name}: model sees it as " + ", ".join(f"'{v}'" for v in pairs["label"].head(3)))
    return dict(zip(pairs["label"], pairs["orig"]))


def cmd_xg(args) -> None:
    """Pool every league's merged/final_events.parquet and run your xG model on it."""
    import pandas as pd

    fetch_code()
    run_xg = find_code_file("run_xg.py")
    leagues = load_leagues(find_code_file(LEAGUES_CSV))
    stage, out, split = WORK / "_xg_input", WORK / "_xg_out", WORK / "_xg_split"

    sources: dict[str, League] = {}
    original_name: dict[str, str] = {}   # "Chile | Primera División" -> "Primera División"
    for lg in leagues:
        f = stage / f"{model_file_stem(lg)}.parquet"
        if download_if_exists((lg.name, "merged"), "final_events.parquet", f):
            sources[f.name] = lg
            original_name.update(label_competition_by_country(f, lg))
        else:
            log(f"{lg.name}: no merged/final_events.parquet yet - not in today's model")
    if not sources:
        raise SystemExit("No league has merged/final_events.parquet in Drive yet - scrape first.")
    log(f"xG model over {len(sources)} league(s)")

    try:
        run([sys.executable, run_xg.name, "--data", str(stage / "*.parquet"),
             "--out", str(out), "--split-output", *shlex.split(XG_EXTRA_ARGS)],
            "run_xg.py (all leagues pooled)", cwd=run_xg.parent)
    except StepFailed as e:
        gh("error", f"xG model: {e}")
        sys.exit(1)

    # Each league's own shots -> <league>/xG.parquet, matched by source file name.
    # (run_xg's by_league/ split is by competition name, and several of your
    # leagues share one, e.g. "Primera División" in Chile, Bolivia and Venezuela.)
    table = pd.read_parquet(out / "all_leagues_xG.parquet")
    for fname, lg in sources.items():
        part = table[table["source_file"] == fname]
        if part.empty:
            gh("warning", f"{lg.name}: the model found no shots in this league")
            continue
        if "matchCompetition" in part.columns:
            # this league's file keeps Opta's own name; the combined one goes alongside
            part = part.copy()
            part["league_label"] = part["matchCompetition"]
            part["matchCompetition"] = part["matchCompetition"].map(
                lambda v: original_name.get(v, v) if isinstance(v, str) else v)
        p = split / lg.name / "xG.parquet"
        p.parent.mkdir(parents=True, exist_ok=True)
        part.to_parquet(p, index=False)
        replace_file(p, remote(lg.name, "xG.parquet"))
        log(f"{lg.name}: {len(part):,} shots, {int(part['goal'].sum())} goals, "
            f"{part['xg'].sum():.1f} xG -> xG.parquet")
    n = replace_dir(out, remote(XG_DRIVE_FOLDER))
    log(f"pooled model output: {n} file(s) -> {ROOT}/{XG_DRIVE_FOLDER}/")


def cmd_wide(args) -> None:
    """build_wide_events_loc.py for one league, with your flags for shots / no-shots leagues."""
    lg = League(**json.loads(args.league or os.environ["LEAGUE_JSON"]))
    fetch_code()
    script = find_code_file("build_wide_events_loc.py")
    merger = find_code_file("scrape_merger3.py")
    if merger.parent != script.parent:  # --enrich imports scrape_merger3 from beside the script
        (script.parent / merger.name).write_bytes(merger.read_bytes())

    merged = WORK / lg.name / "merged"
    events = merged / "final_events.parquet"
    if not download_if_exists((lg.name, "merged"), "final_events.parquet", events):
        gh("notice", f"{lg.name}: no merged/final_events.parquet yet - nothing to widen")
        return

    cmd = [sys.executable, script.name, "--events", str(events)]
    grid, had_grid = merged / "xt_grid.json", False
    if lg.has_fotmob:
        # shots league: fotmob xG is already in final_events
        had_grid = download_if_exists((lg.name,), "xt_grid.json", grid)
        cmd += ["--enrich", "--carry-events", "--xthreat", "--xt-grid", str(grid)]
    else:
        # no-shots league: xG comes from your model
        xg = merged / "xG.parquet"
        if download_if_exists((lg.name,), "xG.parquet", xg):
            cmd += ["--xg", str(xg)]
        else:
            gh("warning", f"{lg.name}: no xG.parquet from the model yet - wide table built without it")
        cmd += ["--carry-events", "--xthreat"]
    cmd += ["--out", str(merged / "wide_events")]

    try:
        run(cmd, "build_wide_events_loc.py", cwd=script.parent)
    except StepFailed as e:
        gh("error", f"{lg.name}: {e}")
        sys.exit(1)

    n = replace_dir(merged / "wide_events", remote(lg.name, "wide_events"))
    log(f"wide_events: uploaded {n} file(s)")
    if lg.has_fotmob and grid.exists() and not had_grid:
        replace_file(grid, remote(lg.name, "xt_grid.json"))
        log("xt_grid.json: first fit saved - reused from now on")


INFO_FILES = ("final_events.parquet", "player_stats.parquet", "substitutions.parquet")


def cmd_info(args) -> None:
    """info_tables.py for one league -> <league>/info_tables/*.csv in Drive."""
    lg = League(**json.loads(args.league or os.environ["LEAGUE_JSON"]))
    fetch_code()
    script = find_code_file("info_tables.py")
    positions = find_code_file(INFO_POSITIONS_CSV)

    merged = WORK / lg.name / "merged"
    missing = [f for f in INFO_FILES if not download_if_exists((lg.name, "merged"), f, merged / f)]
    if missing:
        gh("notice", f"{lg.name}: no {', '.join(missing)} in merged/ yet - no info tables")
        return

    out = WORK / lg.name / "info_tables"
    try:
        run([sys.executable, script.name, "--merged-dir", str(merged),
             "--positions", str(positions), "--out", str(out)],
            "info_tables.py", cwd=script.parent)
    except StepFailed as e:
        gh("error", f"{lg.name}: {e}")
        sys.exit(1)
    n = replace_dir(out, remote(lg.name, "info_tables"))
    log(f"info_tables: uploaded {n} file(s)")


def cmd_analytics(args) -> None:
    """PMR, DRF, sequences (+ dashboards' role / team-style tables), grades, VAEP and
    process-data for one league -> <league>/analytics/ in Drive. See analytics.py."""
    import analytics

    lg = League(**json.loads(args.league or os.environ["LEAGUE_JSON"]))
    fetch_code()
    base = WORK / lg.name
    wide_dir, info_dir = base / "wide_events", base / "info_tables"
    for part, d in (("wide_events", wide_dir), ("info_tables", info_dir)):
        if not remote_listing(remote(lg.name, part)):
            gh("notice", f"{lg.name}: no {part}/ in Drive yet - analytics skipped")
            return
        rclone("copy", remote(lg.name, part), str(d))
    wide = sorted(p for p in wide_dir.glob("*.parquet") if not p.name.startswith("._"))
    if not wide:
        gh("notice", f"{lg.name}: wide_events/ has no parquet - analytics skipped")
        return

    steps = [s.strip() for s in ANALYTICS_STEPS.split(",") if s.strip()] or None
    out = base / "analytics"
    problems = analytics.run_all(lg.name, wide, info_dir, CODE, base / "_analytics_work", out, steps)

    # one tool's folder at a time: a tool that failed or wasn't run today keeps
    # yesterday's files in Drive instead of losing them
    n = 0
    for sub in sorted(p for p in out.iterdir() if p.is_dir()):
        n += replace_dir(sub, remote(lg.name, "analytics", sub.name))
    log(f"analytics: uploaded {n} file(s) -> {ROOT}/{lg.name}/analytics/")
    if problems:
        sys.exit(1)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("plan")
    p.add_argument("--only", default="", help="comma-separated league folders, e.g. 'USA/MLS,Brazil'")
    for name in ("run", "wide", "info", "analytics"):
        r = sub.add_parser(name)
        r.add_argument("--league", help="league JSON (default: $LEAGUE_JSON)")
    sub.add_parser("xg")
    args = ap.parse_args()
    {"plan": cmd_plan, "run": cmd_run, "xg": cmd_xg, "wide": cmd_wide,
     "info": cmd_info, "analytics": cmd_analytics}[args.cmd](args)


if __name__ == "__main__":
    main()
