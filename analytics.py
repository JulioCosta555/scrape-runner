"""
analytics.py - the per-league analysis tools, run after the scrape / wide / info steps.

    from analytics import run_all
    problems = run_all(league_name, inputs, code_dir, work_dir, out_dir, steps)

`inputs` holds this league's files (downloaded from Drive by pipeline.py):
    wide        list of wide_events parquet files   (<league>/wide_events/)
    info        folder with the info_tables CSVs    (<league>/info_tables/)

Every tool runs in its OWN folder, with a data/ subfolder holding only the files
that tool reads, under the exact names it expects. Your scripts are used exactly
as they are in Drive _code - nothing is edited. Why separate folders: several of
the scripts read every data/*.parquet as event files, so in a shared folder they
would also read each other's outputs.

Steps (each needs the ones before it marked *):
    pmr         pmr_run_sc.mjs -> pmr_archetypes_sc.mjs
    drf         drf.mjs
    sequences   build_sequences.py --source scoresway
    dashboards  sequence_role.py  -> sequence_role.csv
                team_style.py     -> team_style_zscores.csv + team_style_per_game.csv
                (Python ports of sequence_analyser_scoresway.qmd and
                teamstyle_radar_scoresway.qmd)                 (*sequences)
    grades      scoresway_to_events.py -> build_grades.py        (*pmr)
    vaep        build_vaep.py --feed scoresway -> build_vaep_dash.py
    processdata processess_data.py --team-split per-team
"""
from __future__ import annotations

import os
import shlex
import shutil
import subprocess
import sys
import time
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
STEPS = ["pmr", "drf", "sequences", "dashboards", "grades", "vaep", "processdata"]
NEEDS = {"dashboards": ["sequences"], "grades": ["pmr"]}

# the two dashboard ports: a copy in Drive _code wins over the one in this repo
DASHBOARD_SCRIPTS = ("sequence_role.py", "team_style.py")
# optional extras: staged when they are in Drive _code, skipped otherwise
OPTIONAL_DATA = ["Id_keys.csv", "mapping_resolved.csv", "match_bridge.csv",
                 "minutes_roles_2026.parquet", "minutes_2025.parquet", "scoresway_minutes.parquet"]


class StepFailed(RuntimeError):
    pass


def log(msg: str) -> None:
    print(msg, file=sys.stderr, flush=True)


def gh(kind: str, msg: str) -> None:
    print(f"::{kind}::{msg}", flush=True)


# ----------------------------------------------------------------------------- helpers
class Code:
    """Finds your files in the Drive _code folder (any subfolder)."""

    def __init__(self, root: Path):
        self.root = root

    def find(self, name: str, required: bool = True) -> Path | None:
        hits = sorted(p for p in self.root.rglob(name)
                      if not p.name.startswith("._") and "__MACOSX" not in p.parts)
        if not hits and required:
            raise StepFailed(f"'{name}' is not in Drive _code")
        return hits[0] if hits else None

    def package(self, name: str) -> Path:
        for p in sorted(self.root.rglob("__init__.py")):
            if p.parent.name == name and "__MACOSX" not in p.parts:
                return p.parent
        raise StepFailed(f"the '{name}' package (folder with __init__.py) is not in Drive _code")


def link(src: Path, dst: Path) -> None:
    """Put src at dst without copying big files (a symlink)."""
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists() or dst.is_symlink():
        dst.unlink()
    os.symlink(src.resolve(), dst)


def copy_tree(src: Path, dst: Path) -> None:
    shutil.copytree(src, dst, dirs_exist_ok=True,
                    ignore=shutil.ignore_patterns("__pycache__", ".DS_Store", "._*"))


def run(cmd: list[str], cwd: Path, title: str, env: dict | None = None) -> None:
    print(f"::group::{title}", flush=True)
    print(f"$ (in {cwd.name}) " + shlex.join(cmd), flush=True)
    t = time.time()
    proc = subprocess.run(cmd, cwd=str(cwd), env={**os.environ, "PYTHONUNBUFFERED": "1", **(env or {})})
    print(f"({(time.time() - t) / 60:.1f} min)", flush=True)
    print("::endgroup::", flush=True)
    if proc.returncode != 0:
        raise StepFailed(f"{title} failed (exit {proc.returncode})")


def collect(src_dir: Path, names: list[str], out: Path, required: list[str] = ()) -> int:
    """Copy the named outputs (wildcards ok) into out/."""
    out.mkdir(parents=True, exist_ok=True)
    n = 0
    for pat in names:
        hits = sorted(src_dir.glob(pat))
        if not hits and pat in required:
            raise StepFailed(f"expected output {pat} was not written")
        for p in hits:
            if p.is_file():
                shutil.copy2(p, out / p.name)
                n += 1
    return n


# ----------------------------------------------------------------------------- shared inputs
def derived_inputs(wide: list[Path], info: Path, code: Code, dest: Path) -> dict[str, Path]:
    """The side files the tools read, built from this league's own data:

    Pos_info.csv / Pos_by_game_info.csv / match_mapping_names.csv - the info tables
    team_names.csv   teamOfficialName -> match_teamName, match_teamShortName, with a
                     row for BOTH official spellings a team can have (teamOfficialName
                     and optaOfficialName), so every tool finds either one
    true_lineups.csv each team's starting formation per match = OG_lineup from
                     match_mapping_names.csv
    pitch_responsibility_grid.csv  from generate_responsibility_grid.py
    """
    dest.mkdir(parents=True, exist_ok=True)
    files: dict[str, Path] = {}
    for name in ("Pos_info.csv", "Pos_by_game_info.csv", "match_mapping_names.csv"):
        p = info / name
        if not p.exists():
            raise StepFailed(f"{name} missing from info_tables - run the info step first")
        files[name] = p

    want = ["teamOfficialName", "optaOfficialName", "match_teamName", "match_teamShortName"]
    parts = []
    for w in wide:
        try:
            import pyarrow.parquet as pq
            have = set(pq.ParquetFile(w).schema_arrow.names)
            df = pd.read_parquet(w, columns=[c for c in want if c in have])
        except ImportError:
            df = pd.read_parquet(w)
        parts.append(df[[c for c in want if c in df.columns]])
    ev = pd.concat(parts, ignore_index=True)
    for c in want:
        if c not in ev.columns:
            ev[c] = pd.NA
    rows = []
    for official in ("teamOfficialName", "optaOfficialName"):
        t = ev[[official, "match_teamName", "match_teamShortName"]].dropna(subset=[official])
        rows.append(t.rename(columns={official: "teamOfficialName"}))
    tn = (pd.concat(rows, ignore_index=True).drop_duplicates("teamOfficialName")
          .sort_values("teamOfficialName"))
    files["team_names.csv"] = dest / "team_names.csv"
    tn.to_csv(files["team_names.csv"], index=False, na_rep="NA")

    mm = pd.read_csv(files["match_mapping_names.csv"], na_values=["NA"], keep_default_na=True)
    tl = mm[[c for c in ("matchId", "matchDate", "teamOfficialName", "homeTeam", "awayTeam",
                         "OG_lineup") if c in mm.columns]].rename(columns={"OG_lineup": "Formation"})
    files["true_lineups.csv"] = dest / "true_lineups.csv"
    tl.to_csv(files["true_lineups.csv"], index=False, na_rep="NA")

    grid_script = code.find("generate_responsibility_grid.py")
    files["pitch_responsibility_grid.csv"] = dest / "pitch_responsibility_grid.csv"
    run([sys.executable, str(grid_script), "--out", str(files["pitch_responsibility_grid.csv"])],
        dest, "generate_responsibility_grid.py")

    for name in OPTIONAL_DATA:
        p = code.find(name, required=False)
        if p:
            files[name] = p
    log(f"inputs: {len(tn)} team spellings, {tl['Formation'].notna().sum() if 'Formation' in tl else 0}"
        f"/{len(tl)} match-team formations")
    return files


def workdir(base: Path, name: str, scripts: list[Path], data: dict[str, Path]) -> Path:
    """A fresh folder: the scripts next to a data/ subfolder of named inputs."""
    w = base / name
    if w.exists():
        shutil.rmtree(w)
    (w / "data").mkdir(parents=True)
    for s in scripts:
        if s.is_dir():
            copy_tree(s, w / s.name)
        else:
            shutil.copy2(s, w / s.name)
    for dst_name, src in data.items():
        link(src, w / "data" / dst_name)
    return w


def wide_names(wide: list[Path], stem: str = "wide_events") -> dict[str, Path]:
    if len(wide) == 1:
        return {f"{stem}.parquet": wide[0]}
    return {f"{stem}_{i:02d}.parquet": p for i, p in enumerate(wide, 1)}


# ----------------------------------------------------------------------------- steps
def step_pmr(ctx) -> None:
    c, f = ctx["code"], ctx["files"]
    data = {**wide_names(ctx["wide"]),
            # pmr_run_sc.mjs reads JK_2026_*; pmr_archetypes_sc.mjs reads Pos_info.csv
            "JK_2026_Pos_info.csv": f["Pos_info.csv"],
            "JK_2026_Pos_by_game_info.csv": f["Pos_by_game_info.csv"],
            "Pos_info.csv": f["Pos_info.csv"],
            "pitch_responsibility_grid.csv": f["pitch_responsibility_grid.csv"]}
    for opt in ("mapping_resolved.csv", "minutes_roles_2026.parquet"):
        if opt in f:
            data[opt] = f[opt]
    w = workdir(ctx["work"], "pmr", [c.find("pmr_run_sc.mjs"), c.find("pmr_archetypes_sc.mjs")], data)
    env = {"PMR_MEM": os.environ.get("PMR_MEM", "10GB"), "PMR_THREADS": os.environ.get("PMR_THREADS", "4")}
    run(["node", "pmr_run_sc.mjs"], w, "pmr_run_sc.mjs", env)
    d = w / "data"
    # pmr_run writes sc_*.csv; the archetypes script (and build_grades) read pmr_sc_*.csv
    for name in ("sc_match_ratings.csv", "sc_season_summary.csv", "sc_key_actions.csv"):
        if not (d / name).exists():
            raise StepFailed(f"pmr_run_sc.mjs did not write {name}")
        shutil.copy2(d / name, d / f"pmr_{name}")
    run(["node", "pmr_archetypes_sc.mjs"], w, "pmr_archetypes_sc.mjs", env)
    n = collect(d, ["sc_match_ratings.csv", "sc_season_summary.csv", "sc_key_actions.csv",
                    "pmr_sc_archetypes.csv"], ctx["out"] / "pmr", required=["pmr_sc_archetypes.csv"])
    for name in ("pmr_sc_match_ratings.csv", "pmr_sc_season_summary.csv", "pmr_sc_archetypes.csv"):
        ctx["files"][name] = d / name
    log(f"pmr: {n} file(s)")


def step_drf(ctx) -> None:
    c, f = ctx["code"], ctx["files"]
    data = {**wide_names(ctx["wide"])}
    for name in ("Pos_by_game_info.csv", "Pos_info.csv", "true_lineups.csv", "team_names.csv",
                 "match_mapping_names.csv", "pitch_responsibility_grid.csv"):
        data[name] = f[name]
    w = workdir(ctx["work"], "drf", [c.find("drf.mjs")], data)
    events = "data/wide_events.parquet" if len(ctx["wide"]) == 1 else "data/wide_events_*.parquet"
    run(["node", "drf.mjs", "--events", events,
         "--pos-by-game", "data/Pos_by_game_info.csv", "--position-info", "data/Pos_info.csv",
         "--true-lineups", "data/true_lineups.csv", "--team-names", "data/team_names.csv",
         "--match-mapping", "data/match_mapping_names.csv",
         "--grid", "data/pitch_responsibility_grid.csv", "--out", "out"], w, "drf.mjs")
    n = collect(w / "out", ["drf_*.csv"], ctx["out"] / "drf")
    if not n:
        raise StepFailed("drf.mjs wrote no CSVs")
    log(f"drf: {n} file(s)")


def step_sequences(ctx) -> None:
    c, f = ctx["code"], ctx["files"]
    data = {**wide_names(ctx["wide"])}
    for name in ("team_names.csv", "true_lineups.csv", "match_mapping_names.csv",
                 "Pos_info.csv", "Pos_by_game_info.csv"):
        data[name] = f[name]
    w = workdir(ctx["work"], "sequences", [c.find("build_sequences.py")], data)
    run([sys.executable, "build_sequences.py", "--source", "scoresway", "--data-dir", "data"],
        w, "build_sequences.py")
    names = ["sequences.parquet", "seq_events.parquet", "player.parquet", "sequences_meta.json"]
    collect(w / "data", names, ctx["out"] / "sequences", required=names[:3])
    for name in names:
        ctx["files"][name] = w / "data" / name


def step_dashboards(ctx) -> None:
    """sequence_role.csv, team_style_zscores.csv and team_style_per_game.csv, written
    next to the sequences outputs (analytics/sequences/)."""
    c, f = ctx["code"], ctx["files"]
    out = (ctx["out"] / "sequences").absolute()
    out.mkdir(parents=True, exist_ok=True)
    w = ctx["work"] / "dashboards"
    w.mkdir(parents=True, exist_ok=True)

    def script(name: str) -> Path:
        return c.find(name, required=False) or HERE / name

    def opt(flag: str, name: str) -> list[str]:
        return [flag, str(Path(f[name]).absolute())] if name in f else []

    problems = []
    minutes = next((n for n in ("minutes_2025.parquet", "scoresway_minutes.parquet") if n in f), None)
    jobs = [
        ("sequence_role.py", [
            "--data-dir", "", "--players", str(Path(f["player.parquet"]).absolute()),
            *opt("--events", "seq_events.parquet"), *opt("--archetypes", "pmr_sc_archetypes.csv"),
            *opt("--pos-info", "Pos_info.csv"), *opt("--team-names", "team_names.csv"),
            *opt("--team-map", "match_mapping_names.csv"),
            *(opt("--minutes", minutes) if minutes else []),
            "--out", str(out), "--region", ctx["league"]]),
        ("team_style.py", [
            "--data-dir", "", "--events", str(Path(f["seq_events.parquet"]).absolute()),
            *opt("--match-info", "Pos_by_game_info.csv"), *opt("--minutes", "Pos_info.csv"),
            *opt("--lineups", "true_lineups.csv"), *opt("--team-names", "team_names.csv"),
            *opt("--fixtures", "match_mapping_names.csv"),
            "--threads", os.environ.get("TEAM_STYLE_THREADS", "4"),
            "--out", str(out), "--region", ctx["league"]]),
    ]
    if "pmr_sc_archetypes.csv" not in f:
        gh("warning", f"{ctx['league']}: no PMR archetypes - sequence_role.csv has no PMR minutes")
    for name, cmd_args in jobs:
        try:
            run([sys.executable, str(script(name)), *cmd_args], w, name)
        except StepFailed as e:
            problems.append(str(e))
    if problems:
        raise StepFailed("; ".join(problems))


def step_grades(ctx) -> None:
    c, f = ctx["code"], ctx["files"]
    data = {**wide_names(ctx["wide"])}
    for name in ("Pos_by_game_info.csv", "Pos_info.csv", "true_lineups.csv", "team_names.csv",
                 "pmr_sc_archetypes.csv", "pmr_sc_season_summary.csv", "pmr_sc_match_ratings.csv"):
        data[name] = f[name]
    if "match_bridge.csv" in f:
        data["match_bridge.csv"] = f["match_bridge.csv"]
    if "Id_keys.csv" in f:
        data["Id_keys.csv"] = f["Id_keys.csv"]
    w = workdir(ctx["work"], "grades", [c.find("scoresway_to_events.py"), c.find("sw_positions.py"),
                                         c.find("build_grades.py")], data)
    run([sys.executable, "scoresway_to_events.py", "--pos-by-game", "Pos_by_game_info.csv",
         "--pos-info", "Pos_info.csv"], w, "scoresway_to_events.py")
    run([sys.executable, "build_grades.py"], w, "build_grades.py")
    n = collect(w / "data", ["grades_*.csv", "grades_*.json"], ctx["out"] / "grades",
                required=["grades_players.csv"])
    log(f"grades: {n} file(s)")


def step_vaep(ctx) -> None:
    c, f = ctx["code"], ctx["files"]
    data = {**wide_names(ctx["wide"])}
    for name in ("Pos_info.csv", "Pos_by_game_info.csv", "true_lineups.csv", "team_names.csv"):
        data[name] = f[name]
    if "Id_keys.csv" in f:
        data["Id_keys.csv"] = f["Id_keys.csv"]
    w = workdir(ctx["work"], "vaep", [c.find("build_vaep.py"), c.find("build_vaep_dash.py"),
                                       c.package("vaep")], data)
    events = "data/wide_events.parquet" if len(ctx["wide"]) == 1 else "data/wide_events_*.parquet"
    run([sys.executable, "build_vaep.py", "--feed", "scoresway", "--events", events,
         "--minutes", "data/Pos_info.csv", "--match-info", "data/Pos_by_game_info.csv",
         "--formations", "data/true_lineups.csv"], w, "build_vaep.py")
    run([sys.executable, "build_vaep_dash.py"], w, "build_vaep_dash.py")
    # the big per-action parquets (vaep_actions / vaep_xy / vaep_valued) stay behind
    n = collect(w / "data", ["vaep_players.csv", "vaep_per_game.csv", "vaep_metrics.json",
                             "vaep_match_positions.csv", "vaep_players_dash.csv",
                             "vaep_sequences.parquet", "vaep_seq_actions.parquet"],
                ctx["out"] / "vaep", required=["vaep_players.csv"])
    log(f"vaep: {n} file(s)")


def step_processdata(ctx) -> None:
    c, f = ctx["code"], ctx["files"]
    data = {**wide_names(ctx["wide"])}
    for name in ("Pos_info.csv", "match_mapping_names.csv", "team_names.csv"):
        data[name] = f[name]
    w = workdir(ctx["work"], "processdata", [c.find("processess_data.py")], data)
    events = "data/wide_events.parquet" if len(ctx["wide"]) == 1 else "data/wide_events_*.parquet"
    run([sys.executable, "processess_data.py", "--feed", "scoresway", "--events", events,
         "--minutes", "data/Pos_info.csv", "--team-split", "per-team",
         "--team-map", "data/match_mapping_names.csv", "data/team_names.csv",
         "--output", "out/processdata.csv"], w, "processess_data.py")
    n = collect(w / "out", ["processdata_*.csv"], ctx["out"] / "processdata",
                required=["processdata_p90.csv"])
    log(f"processdata: {n} file(s)")


STEP_FN = {"pmr": step_pmr, "drf": step_drf, "sequences": step_sequences,
           "dashboards": step_dashboards, "grades": step_grades, "vaep": step_vaep,
           "processdata": step_processdata}


def run_all(league: str, wide: list[Path], info: Path, code_dir: Path, work: Path, out: Path,
            steps: list[str] | None = None) -> list[str]:
    steps = [s for s in STEPS if s in (steps or STEPS)]
    code = Code(code_dir)
    work.mkdir(parents=True, exist_ok=True)
    out.mkdir(parents=True, exist_ok=True)
    try:
        files = derived_inputs(wide, info, code, work / "_inputs")
    except StepFailed as e:
        return [f"inputs: {e}"]
    # the derived helper files are useful on their own (and for your dashboards)
    collect(work / "_inputs", ["team_names.csv", "true_lineups.csv",
                               "pitch_responsibility_grid.csv"], out / "inputs")

    ctx = {"league": league, "wide": wide, "files": files, "code": code, "work": work, "out": out}
    done, problems = set(), []
    for s in steps:
        missing = [n for n in NEEDS.get(s, []) if n not in done]
        if missing:
            msg = f"{s}: skipped, needs {', '.join(missing)}"
            problems.append(msg)
            gh("warning", f"{league}: {msg}")
            continue
        t = time.time()
        try:
            STEP_FN[s](ctx)
            done.add(s)
            log(f"== {s} done in {(time.time() - t) / 60:.1f} min")
        except Exception as e:  # one tool failing never stops the others
            problems.append(f"{s}: {e}")
            gh("error", f"{league}: {s}: {e}")
    return problems
