#!/usr/bin/env node
// ojs_run.mjs - run a Quarto OJS dashboard headlessly and save its tables as CSV.
//
//   node ojs_run.mjs --job sequence_role --qmd sequence_analyser_scoresway.qmd --out OUT [--region R]
//   node ojs_run.mjs --job team_style    --qmd teamstyle_radar_scoresway.qmd    --out OUT [--region R]
//
// Run it in the folder that holds the .qmd and its data/ folder, exactly like
// opening the dashboard. Every ```{ojs} cell is parsed with Observable's own
// parser and run on Observable's runtime, so the numbers come from YOUR cells,
// unchanged. Only these are swapped for headless stand-ins:
//   DuckDBClient   -> native DuckDB (same SQL engine as the browser's DuckDB-Wasm)
//   FileAttachment -> the files in data/ ("data/JK_2026_Pos_info.csv" also finds
//                     data/Pos_info.csv, so the per-league prefix doesn't matter)
//   Inputs.*       -> each control at its own default value
//   html/md/svg    -> inert placeholders (nothing is drawn)
// and two controls are set:
//   sequence_role  pl_type = "All", pl_scope = "All leagues" (every player in the data)
//   team_style     league_filter = "All" (z-scores across every team in the data)
//
// Outputs
//   sequence_role  sequence_role.csv          = the "sequence_role.csv" button, + Region
//   team_style     team_style_zscores.csv     = the "Full z-score table", + Region
//                  team_style_per_game.csv    = the same metrics per team per MATCH,
//                                               raw values (before z-scoring)

import fs from "node:fs";
import path from "node:path";
import { pathToFileURL, fileURLToPath } from "node:url";
import { DuckDBInstance } from "@duckdb/node-api";
import * as d3 from "d3";

// ------------------------------------------------------------------ args ---
const argv = process.argv.slice(2);
const arg = (n, d) => { const i = argv.indexOf(n); return i === -1 ? d : argv[i + 1]; };
const JOB = arg("--job");
const QMD = path.resolve(arg("--qmd"));
const OUT = path.resolve(arg("--out", "out"));
const REGION = arg("--region", "");
if (!["sequence_role", "team_style"].includes(JOB)) throw new Error("--job sequence_role|team_style");
const BASE = path.dirname(QMD);                  // FileAttachment paths are relative to the .qmd
const DUCKFS = path.join(BASE, ".duckfs");       // where registerFile* writes; DuckDB's cwd
fs.mkdirSync(DUCKFS, { recursive: true });
fs.mkdirSync(OUT, { recursive: true });
process.chdir(DUCKFS);

const log = (...a) => console.error(...a);
process.on("unhandledRejection", (e) => log("  (background cell error)", String(e?.message ?? e).split("\n")[0]));

// --------------------------------------------------------- browser stand-ins ---
function stub(props = {}) {
  const target = function () {};
  for (const [k, v] of Object.entries(props)) target[k] = v;
  return new Proxy(target, {
    get(t, k) {
      if (k in t) return t[k];
      if (k === "then") return undefined;              // never look like a promise
      if (k === Symbol.toPrimitive) return () => "";
      if (k === Symbol.iterator) return function* () {};
      if (typeof k === "symbol") return undefined;
      return stub();
    },
    set(t, k, v) { t[k] = v; return true; },
    apply() { return stub(); },
    construct() { return stub(); },
  });
}
const tag = () => stub();
globalThis.window = globalThis;
globalThis.document ??= stub();
globalThis.requestAnimationFrame ??= (cb) => setTimeout(() => cb(Date.now()), 0);
globalThis.cancelAnimationFrame ??= (id) => clearTimeout(id);

const realFetch = globalThis.fetch;
globalThis.fetch = async (input, init) => {
  const url = typeof input === "string" ? input : String(input?.url ?? input);
  if (url.startsWith("file:")) {
    const p = fileURLToPath(url);
    return fs.existsSync(p) ? new Response(fs.readFileSync(p), { status: 200 })
                            : new Response("Not found", { status: 404 });
  }
  return realFetch(input, { ...init, signal: AbortSignal.timeout(15000) });
};

// "data/JK_2026_Pos_info.csv" -> data/Pos_info.csv when only the bare name exists
const CANONICAL = ["Pos_by_game_info.csv", "match_mapping_names.csv", "true_lineups.csv",
                   "team_names.csv", "Pos_info.csv"];
function resolveAttachment(name) {
  const p = path.resolve(BASE, name);
  if (fs.existsSync(p)) return p;
  const base = path.basename(name).toLowerCase();
  for (const c of CANONICAL) {
    if (base.endsWith(c.toLowerCase())) {
      const q = path.join(path.dirname(p), c);
      if (fs.existsSync(q)) return q;
    }
  }
  return null;
}
function FileAttachment(name) {
  const p = resolveAttachment(name);
  const need = () => { if (!p) throw new Error(`File not found: ${name}`); return p; };
  const fa = {
    name: path.basename(name),
    url: async () => pathToFileURL(p ?? path.resolve(BASE, name)).href,
    text: async () => fs.readFileSync(need(), "utf8"),
    json: async () => JSON.parse(fs.readFileSync(need(), "utf8")),
    arrayBuffer: async () => { const b = fs.readFileSync(need()); return b.buffer.slice(b.byteOffset, b.byteOffset + b.byteLength); },
    csv: async ({ typed = false } = {}) => d3.csvParse(fs.readFileSync(need(), "utf8"), typed ? d3.autoType : undefined),
    tsv: async ({ typed = false } = {}) => d3.tsvParse(fs.readFileSync(need(), "utf8"), typed ? d3.autoType : undefined),
    image: async () => stub(),
  };
  return fa;
}

// DuckDB: one native connection behind the DuckDBClient API the cells use
function toJS(v) {
  if (v === null || v === undefined) return null;
  if (typeof v === "bigint") return Number(v);
  if (Array.isArray(v)) return v.map(toJS);
  if (typeof v === "object") {
    const cn = v.constructor?.name ?? "";
    if (cn.startsWith("DuckDB")) {
      if (/Decimal/.test(cn)) return Number(v.toString());
      if (/List|Array/.test(cn)) return (v.items ?? []).map(toJS);
      if (/Struct/.test(cn)) return Object.fromEntries(Object.entries(v.entries ?? {}).map(([k, x]) => [k, toJS(x)]));
      if (/Map/.test(cn)) return (v.entries ?? []).map(({ key, value }) => [toJS(key), toJS(value)]);
      return v.toString();                           // dates, timestamps, uuids, ...
    }
    if (v instanceof Date || v instanceof Uint8Array) return v;
    if (Object.getPrototypeOf(v) === Object.prototype)
      return Object.fromEntries(Object.entries(v).map(([k, x]) => [k, toJS(x)]));
  }
  return v;
}
let sharedCon = null;
class NodeDuckDBClient {
  static async of() {
    if (!sharedCon) {
      const inst = await DuckDBInstance.create(":memory:", {
        threads: process.env.OJS_THREADS ?? "4",
        memory_limit: process.env.OJS_MEM ?? "10GB",
      });
      sharedCon = await inst.connect();
    }
    return new NodeDuckDBClient(sharedCon);
  }
  constructor(con) {
    this.con = con;
    const write = (n, data) => fs.writeFileSync(path.join(DUCKFS, n), data);
    this._db = {
      registerFileBuffer: async (n, buf) => write(n, Buffer.from(buf)),
      registerFileText: async (n, text) => write(n, text),
      registerFileURL: async (n, url) => write(n, fs.readFileSync(fileURLToPath(url))),
      dropFile: async (n) => fs.rmSync(path.join(DUCKFS, n), { force: true }),
    };
  }
  async query(sql) {
    const reader = await this.con.runAndReadAll(sql);
    return reader.getRowObjects().map((r) => toJS(r));
  }
  async queryRow(sql) { return (await this.query(sql))[0] ?? null; }
  async sql(strings, ...args) { return this.query(String.raw(strings, ...args)); }
}

const firstOf = (data) => {
  if (data instanceof Map) return data.values().next().value;
  if (Array.isArray(data)) return data[0];
  if (data && typeof data[Symbol.iterator] === "function") return data[Symbol.iterator]().next().value;
  return undefined;
};
const el = (value) => stub({ value });
const Inputs = new Proxy({
  select: (d, o = {}) => el(o.value !== undefined ? o.value : (o.multiple ? [] : firstOf(d))),
  radio: (d, o = {}) => el(o.value !== undefined ? o.value : null),
  checkbox: (d, o = {}) => el(o.value !== undefined ? o.value : []),
  toggle: (o = {}) => el(o.value ?? false),
  range: (ext = [0, 1], o = {}) => el(o.value !== undefined ? o.value : (ext[0] + ext[1]) / 2),
  number: (a, b) => { const o = Array.isArray(a) ? (b ?? {}) : (a ?? {}); return el(o.value ?? 0); },
  text: (o = {}) => el(o.value ?? ""),
  textarea: (o = {}) => el(o.value ?? ""),
  search: (d) => el(d),
  table: (d, o = {}) => el(o.value ?? []),
  button: (c, o = {}) => el(o.value ?? 0),
  input: (v) => el(v),
  bind: (a) => a,
}, { get: (t, k) => t[k] ?? ((...a) => el(a[1]?.value ?? null)) });

const BUILTINS = {
  md: () => tag, html: () => tag, svg: () => tag, tex: () => tag, dot: () => tag, mermaid: () => tag,
  htl: () => ({ html: tag, svg: tag }),
  Inputs: () => Inputs, Plot: () => stub(), DOM: () => stub(),
  d3: () => d3,
  require: () => { const r = async (n) => (String(n).startsWith("d3") ? d3 : stub()); r.alias = () => r; r.resolve = (n) => n; return r; },
  FileAttachment: () => FileAttachment,
  DuckDBClient: () => NodeDuckDBClient,
  width: () => 1200, now: () => Date.now(),
  visibility: () => () => Promise.resolve(true),
  invalidation: () => new Promise(() => {}),
  Generators: () => ({ input: (e) => e?.value, observe: () => stub(), queue: () => stub() }),
  Promises: () => ({ delay: (ms, v) => new Promise((r) => setTimeout(() => r(v), ms)),
                     when: (d, v) => new Promise((r) => setTimeout(() => r(v), Math.max(0, +d - Date.now()))),
                     tick: (ms, v) => new Promise((r) => setTimeout(() => r(v), ms)) }),
};

// ------------------------------------------------------- compile the cells ---
// loaded only now, after the browser stand-ins above exist
// Both packages ship an older module format in some versions; under import() their
// functions can then sit on `default` instead of being named exports. Take them from
// wherever they are, and fall back to require().
async function load(pkg, name) {
  let mod = null;
  try { mod = await import(pkg); } catch (e) { log(`  (import ${pkg}: ${e.message})`); }
  let fn = mod?.[name] ?? mod?.default?.[name];
  if (typeof fn !== "function") {
    try { const { createRequire } = await import("node:module"); const m = createRequire(import.meta.url)(pkg); fn = m?.[name] ?? m?.default?.[name]; }
    catch (e) { log(`  (require ${pkg}: ${e.message})`); }
  }
  if (typeof fn !== "function")
    throw new Error(`${pkg} has no ${name}() - found: ${Object.keys(mod?.default ?? mod ?? {}).join(", ") || "nothing"}`);
  return fn;
}
const parseModule = await load("@observablehq/parser", "parseModule");
const Runtime = await load("@observablehq/runtime", "Runtime");

const qmdText = fs.readFileSync(QMD, "utf8");
const blocks = [...qmdText.matchAll(/^```\{ojs[^}\n]*\}[^\n]*\n([\s\S]*?)^```[ \t]*$/gm)].map((m) => m[1]);
log(`${path.basename(QMD)}: ${blocks.length} ojs blocks`);

const paramOf = (input) => input.startsWith("viewof ") ? `$viewof$${input.slice(7)}`
  : input.startsWith("mutable ") ? `$mutable$${input.slice(8)}` : input;

function compile(src, cell, patch = null) {
  const inputs = [];
  for (const ref of cell.references ?? []) {
    const input = ref.type === "ViewExpression" ? `viewof ${ref.id.name}`
      : ref.type === "MutableExpression" ? `mutable ${ref.id.name}` : ref.name;
    if (!inputs.includes(input)) inputs.push(input);
  }
  let body = src.slice(cell.body.start, cell.body.end);
  body = body.replace(/\bviewof\s+([A-Za-z_$][\w$]*)/g, (m, n) => inputs.includes(`viewof ${n}`) ? `$viewof$${n}` : m);
  body = body.replace(/\bmutable\s+([A-Za-z_$][\w$]*)/g, (m, n) => inputs.includes(`mutable ${n}`) ? `$mutable$${n}.value` : m);
  if (patch) body = patch(body);
  const fnBody = cell.body.type === "BlockStatement" ? body : `{ return (\n${body}\n); }`;
  const head = `${cell.async ? "async " : ""}function${cell.generator ? "*" : ""}(${inputs.map(paramOf).join(", ")})`;
  const fn = new Function(`return (${head} ${fnBody});`)();
  return { inputs, fn };
}

const cells = new Map();          // name -> {src, cell, kind}
for (const src of blocks) {
  let mod;
  try { mod = parseModule(src); }
  catch (e) { log(`  ! a block failed to parse (${e.message}) - skipped`); continue; }
  for (const cell of mod.cells) {
    if (!cell.id || !cell.body || cell.body.type === "ImportDeclaration") continue;  // display-only / imports
    const kind = cell.id.type === "ViewExpression" ? "viewof"
      : cell.id.type === "MutableExpression" ? "mutable" : "plain";
    const name = kind === "plain" ? cell.id.name : cell.id.id.name;
    if (cells.has(name)) { log(`  ! ${name} is defined twice - keeping the first`); continue; }
    cells.set(name, { src, cell, kind });
  }
}
log(`  ${cells.size} named cells`);
if (!cells.size) { log("FAILED: no cell could be read from the notebook"); process.exit(1); }

const OVERRIDES = JOB === "sequence_role"
  ? { pl_type: "All", pl_scope: "All leagues" }
  : { league_filter: "All" };

const runtime = new Runtime(BUILTINS, (name) => globalThis[name]);
const main = runtime.module();
const deps = new Map();
for (const [name, { src, cell, kind }] of cells) {
  let compiled;
  try { compiled = compile(src, cell); }
  catch (e) { log(`  ! ${name}: does not compile (${e.message}) - skipped`); continue; }
  deps.set(name, compiled.inputs);
  if (kind === "viewof") {
    main.variable().define(`viewof ${name}`, compiled.inputs, compiled.fn);
    if (name in OVERRIDES) main.variable().define(name, [], () => OVERRIDES[name]);
    else main.variable().define(name, [`viewof ${name}`], (e) => e?.value);
  } else if (kind === "mutable") {
    main.variable().define(`mutable ${name}`, compiled.inputs, function (...a) {
      const v = compiled.fn.apply(this, a); return { value: v };
    });
    main.variable().define(name, [`mutable ${name}`], (m) => m.value);
  } else {
    main.variable().define(name, compiled.inputs,
      name in OVERRIDES ? () => OVERRIDES[name] : compiled.fn);
  }
}

// ------------------------------------------------------------ evaluation ---
async function diagnose(name, seen = new Set()) {
  // follow the failing chain down to the first cell that actually throws
  if (seen.has(name)) return null;
  seen.add(name);
  for (const d of deps.get(name) ?? []) {
    const base = d.replace(/^(viewof|mutable) /, "");
    if (!deps.has(base)) continue;
    try { await main.value(d); } catch (e) { return (await diagnose(base, seen)) ?? `${base}: ${e?.message ?? e}`; }
  }
  return null;
}
async function value(name) {
  const t = Date.now();
  try {
    const v = await main.value(name);
    log(`  ${name} (${((Date.now() - t) / 1000).toFixed(1)}s)`);
    return v;
  } catch (e) {
    const root = await diagnose(name);
    throw new Error(`${name} failed: ${e?.message ?? e}${root ? `\n    first failing cell -> ${root}` : ""}`);
  }
}
async function callPatched(name, patch, given = {}) {
  // run one of the notebook's cells with a small text patch, inputs from the runtime
  const c = cells.get(name);
  if (!c) throw new Error(`cell ${name} not found in ${path.basename(QMD)}`);
  const { inputs, fn } = compile(c.src, c.cell, patch);
  const args = [];
  for (const i of inputs) args.push(i in given ? given[i] : await value(i));
  return fn(...args);
}
const mustReplace = (s, from, to, what) => {
  const out = s.replace(from, to);
  if (out === s) throw new Error(`could not adapt ${what}: the notebook's code has changed`);
  return out;
};

// ----------------------------------------------------------------- csv io ---
const esc = (v) => { const t = v == null ? "" : String(v); return /[",\n]/.test(t) ? `"${t.replaceAll('"', '""')}"` : t; };
function writeCsv(file, header, rows) {
  const lines = [header.map(esc).join(","), ...rows.map((r) => header.map((h) => esc(r[h])).join(","))];
  fs.writeFileSync(path.join(OUT, file), "﻿" + lines.join("\n") + "\n");
  log(`wrote ${file} (${rows.length} rows)`);
}

// ------------------------------------------------------------------- jobs ---
async function jobSequenceRole() {
  const csv = await callPatched("sequence_role_export", (body) => mustReplace(body,
    /(const csv = [^;]+;)/, "$1\n  return csv;", "sequence_role_export"));
  if (typeof csv !== "string") throw new Error("no players under the export's filters (playerRows is empty)");
  let lines = csv.replace(/^﻿/, "").split("\n");
  if (REGION) lines = lines.map((l, i) => l + "," + (i === 0 ? "Region" : esc(REGION)));
  fs.writeFileSync(path.join(OUT, "sequence_role.csv"), "﻿" + lines.join("\n") + "\n");
  log(`wrote sequence_role.csv (${lines.length - 1} players)`);
}

async function jobTeamStyle() {
  const SLICES = await value("ALL_SLICES");
  const label = (s) => s.label.replaceAll("\n", " ");

  // 1. the "Full z-score table" cell, as a file
  const z = await value("teams_z");
  const zHeader = ["team", "league", "matches", ...SLICES.map(label), ...(REGION ? ["Region"] : [])];
  writeCsv("team_style_zscores.csv", zHeader, z.map((d) => {
    const row = { team: d.team, league: d.league, matches: d.matches, Region: REGION };
    for (const s of SLICES) row[label(s)] = d[s.key + "_z"] == null ? null : +d[s.key + "_z"].toFixed(2);
    return row;
  }));

  // 2. the same metrics per team per MATCH, raw. The notebook's own SQL, grouped by
  //    (match, team) instead of team; then its own teamMetrics formulas with m = 1.
  const db = await value("db");
  await value("seqTeam");                     // builds seq_s
  await value("concededTeam");                // builds tm_agg
  const byMatch = (sql, what) => mustReplace(
    mustReplace(sql, /SELECT\s+team\s*,/, "SELECT match, team,", what),
    /GROUP BY 1\s*$/, "GROUP BY 1, 2", what);
  const seqPG = await db.query(byMatch((await value("seqFinalSQL"))("seq_s"), "seqFinalSQL"));
  const passPG = await db.query(byMatch((await value("passSQL"))("ev"), "passSQL"));
  const shotPG = await db.query(byMatch((await value("shotDefSQL"))("ev"), "shotDefSQL"));
  const concPG = await db.query(mustReplace(mustReplace((await value("concededFinalSQL"))("tm_agg"),
    /SELECT\s+a\.team\s*,/, "SELECT a.match, a.team,", "concededFinalSQL"),
    /GROUP BY 1\s*$/, "GROUP BY 1, 2", "concededFinalSQL"));

  const SEP = "␟";
  const keyed = (rows) => rows.map((r) => ({ ...r, team: r.team == null ? null : `${r.match}${SEP}${r.team}` }));
  const teamOf = (k) => String(k).split(SEP)[1];
  const keys = [...new Set([...seqPG, ...passPG, ...shotPG, ...concPG]
    .filter((r) => r.team != null).map((r) => `${r.match}${SEP}${r.team}`))];
  const leagues = await value("teamLeagueMap");
  const evF = await value("evFormation");
  const shortMap = await value("teamShortMap");
  const remap = (m) => new Map(keys.map((k) => [k, m?.get?.(teamOf(k))]));

  const rows = await callPatched("teamMetrics",
    (body) => mustReplace(body, /if\s*\(\s*!m\s*\|\|\s*m\s*<\s*3\s*\)\s*continue;/, "if (!m) continue;", "teamMetrics"),
    {
      seqTeam: keyed(seqPG), passTeam: keyed(passPG), shotDefTeam: keyed(shotPG), concededTeam: keyed(concPG),
      matchesPlayed: new Map(keys.map((k) => [k, 1])),
      teamInfo: new Map(), toRaw: (x) => x,
      teamLeagueMap: remap(leagues), teamShortMap: remap(shortMap),
      // per-match formation isn't in the notebook; the team's usual back line is used
      evFormation: { dist: new Map(), shape: new Map(), back: remap(evF?.back) },
    });
  const pgHeader = ["matchId", "team", "league", ...SLICES.map(label)];
  writeCsv("team_style_per_game.csv", pgHeader, rows.map((d) => {
    const [matchId, team] = String(d.team).split(SEP);
    const row = { matchId, team, league: d.league };
    for (const s of SLICES) row[label(s)] = Number.isFinite(d[s.key]) ? +d[s.key].toFixed(4) : null;
    return row;
  }));
}

try {
  if (JOB === "sequence_role") await jobSequenceRole();
  else await jobTeamStyle();
  process.exit(0);
} catch (e) {
  log(`FAILED: ${e?.message ?? e}`);
  process.exit(1);
}
