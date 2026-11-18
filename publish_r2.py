"""publish_r2.py - every league's analytics outputs, merged into one All_leagues/
folder and uploaded to Cloudflare R2 for the app.

For each table the tools write (processdata_p90.csv, sequence_role.csv,
player_shotmap.csv, ...) the copies from every league in scrape_leagues.csv are
stacked into one file, with a Region column (the league folder, e.g.
"Norway/Eliteserien") on every row:

    All_leagues/<tool>/<table>.csv        small tables stay CSV
    All_leagues/<tool>/<table>.parquet    bigger than PUBLISH_PARQUET_MB (all
                                          leagues together), or parquet already
    All_leagues/leagues.csv               the leagues that went in
    All_leagues/manifest.json             every file: format, rows, size, leagues

Leagues whose file differs in columns still stack: missing columns are left
empty. Files that are only inputs for other tools are left out (PUBLISH_SKIP),
and files that are the same in every league are published once (PUBLISH_ONCE).

Settings (environment / repository variables):
    R2_REMOTE            rclone remote + bucket, e.g. "r2:football-app"   (required)
    R2_FOLDER            folder in the bucket                (default All_leagues)
    PUBLISH_PARQUET_MB   CSV tables bigger than this become parquet   (default 25)
    PUBLISH_SKIP         extra file names to leave out, comma-separated
    PUBLISH_CODEC        parquet compression: zstd or snappy        (default zstd)
"""
from __future__ import annotations

import json
import os
import shutil
import time
from pathlib import Path

DEFAULT_SKIP = {
    # event-level inputs for other tools - big, and not read by the app
    "seq_events.parquet", "sequences.parquet", "vaep_sequences.parquet",
    "vaep_seq_actions.parquet", "drf_defensive_actions_sw.csv", "sc_key_actions.csv",
    "sequences_meta.json",
}
ONCE = {"pitch_responsibility_grid.csv"}      # identical in every league
TABLE_EXT = {".csv", ".parquet"}


def _escape(path: str) -> str:
    """a literal path for an rclone filter rule"""
    out = []
    for ch in path:
        out.append("\\" + ch if ch in "\\*?[]{}" else ch)
    return "".join(out)


def _sql(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def work_dir(default: Path) -> Path:
    """the runner's big /mnt disk when there is one (the staging can be >10 GB)"""
    for cand in (os.environ.get("PUBLISH_WORK"), "/mnt/publish"):
        if not cand:
            continue
        p = Path(cand)
        try:
            p.mkdir(parents=True, exist_ok=True)
            probe = p / ".probe"
            probe.write_text("x")
            probe.unlink()
            return p
        except OSError:
            continue
    return default


def stage(leagues, remote_root: str, dest: Path, skip: set[str], rclone, log) -> None:
    """copy <league>/analytics/** of every league from Drive, minus the skipped files"""
    rules = [f"- {_escape(n)}" for n in sorted(skip)]
    rules += [f"+ /{_escape(lg.name)}/analytics/**" for lg in leagues]
    rules.append("- **")
    filt = dest.parent / "publish_filter.txt"
    filt.write_text("\n".join(rules) + "\n", encoding="utf-8")
    log(f"staging analytics/ of {len(leagues)} league(s) from Drive")
    rclone("copy", remote_root, str(dest), "--filter-from", str(filt))


def group_tables(stage_dir: Path, leagues) -> dict[tuple[str, str], list[tuple[Path, str]]]:
    """(tool folder, file name) -> [(path, league), ...] in scrape_leagues.csv order"""
    groups: dict[tuple[str, str], list[tuple[Path, str]]] = {}
    for lg in leagues:
        a = stage_dir / lg.name / "analytics"
        if not a.is_dir():
            continue
        for p in sorted(a.glob("*/*")):
            if p.is_file() and p.suffix.lower() in TABLE_EXT and not p.name.startswith("._") \
                    and p.stat().st_size > 0:
                groups.setdefault((p.parent.name, p.name), []).append((p, lg.name))
    return groups


def combine(con, files: list[tuple[Path, str]], out_base: Path, threshold_mb: float,
            codec: str, add_region: bool, log) -> dict:
    """stack one table across leagues -> out_base.csv or out_base.parquet"""
    import duckdb

    paths = [str(p.resolve()) for p, _ in files]
    total = sum(p.stat().st_size for p, _ in files)
    is_parquet = files[0][0].suffix.lower() == ".parquet"
    as_parquet = is_parquet or total > threshold_mb * 1024 * 1024
    ext = ".parquet" if as_parquet else ".csv"
    out = out_base.with_suffix(ext)
    out.parent.mkdir(parents=True, exist_ok=True)

    con.execute("CREATE OR REPLACE TEMP TABLE src_map (path VARCHAR, league VARCHAR)")
    con.executemany("INSERT INTO src_map VALUES (?, ?)", [(str(p.resolve()), lg) for p, lg in files])
    plist = "[" + ", ".join(_sql(p) for p in paths) + "]"

    if is_parquet:
        readers = [f"read_parquet({plist}, union_by_name=true, filename=true)"]
    else:
        base = f"{plist}, union_by_name=true, filename=true, header=true"
        readers = [f"read_csv({base}, nullstr=['NA', ''])",                   # typed columns
                   f"read_csv({base}, nullstr=['NA', ''], all_varchar=true)",  # leagues disagree on a type
                   f"read_csv({base}, nullstr='NA')",                          # older DuckDB
                   f"read_csv({base}, nullstr='NA', all_varchar=true)"]

    last_err = None
    for reader in readers:
        try:
            cols = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM {reader}").fetchall()]
            data_cols = [c for c in cols if c != "filename"]
            if not add_region:
                select = "t.* EXCLUDE (filename)"
            elif "Region" in data_cols:
                select = "t.* EXCLUDE (filename) REPLACE (COALESCE(CAST(t.Region AS VARCHAR), m.league) AS Region)"
            else:
                select = "t.* EXCLUDE (filename), m.league AS Region"
            query = f"SELECT {select} FROM {reader} t JOIN src_map m ON t.filename = m.path"
            if as_parquet:
                con.execute(f"COPY ({query}) TO {_sql(str(out))} "
                            f"(FORMAT parquet, COMPRESSION {codec}, ROW_GROUP_SIZE 100000)")
            else:
                con.execute(f"COPY ({query}) TO {_sql(str(out))} (FORMAT csv, HEADER true)")
            rows = con.execute(
                f"SELECT COUNT(*) FROM {'read_parquet' if as_parquet else 'read_csv'}({_sql(str(out))})"
            ).fetchone()[0]
            return {"format": ext[1:], "rows": int(rows), "bytes": out.stat().st_size,
                    "source_bytes": total, "leagues": [lg for _, lg in files],
                    "all_text": "all_varchar" in reader}
        except duckdb.Error as e:
            last_err = e
            out.unlink(missing_ok=True)
            continue
    raise RuntimeError(str(last_err).splitlines()[0] if last_err else "unknown error")


def publish(leagues, remote_root: str, r2_target: str, work: Path, rclone, log, gh) -> list[str]:
    import duckdb

    threshold = float(os.environ.get("PUBLISH_PARQUET_MB") or 25)
    codec = (os.environ.get("PUBLISH_CODEC") or "zstd").lower()
    skip = DEFAULT_SKIP | {s.strip() for s in (os.environ.get("PUBLISH_SKIP") or "").split(",")
                           if s.strip()}
    root = work_dir(work / "_publish")
    stage_dir, out_dir = root / "stage", root / "All_leagues"
    for d in (stage_dir, out_dir):
        shutil.rmtree(d, ignore_errors=True)
        d.mkdir(parents=True)

    t0 = time.time()
    stage(leagues, remote_root, stage_dir, skip, rclone, log)
    groups = group_tables(stage_dir, leagues)
    log(f"{len(groups)} table(s) to merge ({time.time() - t0:.0f}s staging)")

    con = duckdb.connect()
    tmp = root / "duckdb_tmp"
    tmp.mkdir(exist_ok=True)
    con.execute(f"SET temp_directory = {_sql(str(tmp))}")
    con.execute("SET preserve_insertion_order = false")

    manifest, problems = {}, []
    for (tool, name), files in sorted(groups.items()):
        rel = f"{tool}/{Path(name).stem}"
        once = name in ONCE
        try:
            info = combine(con, files[:1] if once else files, out_dir / tool / Path(name).stem,
                           threshold, codec, add_region=not once, log=log)
        except Exception as e:  # noqa: BLE001 - one bad table must not stop the rest
            problems.append(f"{tool}/{name}: {e}")
            gh("warning", f"publish: {tool}/{name} not merged - {e}")
            continue
        key = f"{rel}.{info['format']}"
        manifest[key] = info
        note = " (text columns: leagues disagreed on a type)" if info["all_text"] else ""
        log(f"  {key:<55} {info['rows']:>10,} rows  {info['bytes'] / 1048576:8.1f} MB  "
            f"from {len(info['leagues'])} league(s), {info['source_bytes'] / 1048576:.1f} MB{note}")
        # free the disk as we go
        for p, _ in files:
            p.unlink(missing_ok=True)

    with open(out_dir / "leagues.csv", "w", encoding="utf-8") as f:
        f.write("Region,season,has_shots\n")
        for lg in leagues:
            name = lg.name.replace('"', '""')
            f.write(f'"{name}",{lg.season},{1 if lg.has_fotmob else 0}\n')
    (out_dir / "manifest.json").write_text(json.dumps(
        {"updated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "files": manifest},
        indent=1), encoding="utf-8")

    size = sum(p.stat().st_size for p in out_dir.rglob("*") if p.is_file())
    log(f"All_leagues: {len(manifest)} table(s), {size / 1073741824:.2f} GB -> {r2_target}")
    # sync: files gone from today's set (or switched CSV -> parquet) are removed from R2
    rclone("sync", str(out_dir), r2_target)
    shutil.rmtree(stage_dir, ignore_errors=True)
    return problems
