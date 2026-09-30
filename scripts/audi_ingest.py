#!/usr/bin/env python3
"""Ingest the Audi e-tron field dataset (Joule 2023, doi:10.17632/7vdkzpnjgj.2) into Parquet.

Reads Charge/Folder*/Raw.mat and Drive/Folder*/Raw.mat (MATLAB v7.3, or older
MAT versions via scipy; unreadable files are skipped and listed in the manifest). Current
is the base clock; voltage and SOC are attached as their latest sample (rows
without one newer than --max-signal-age are dropped) and temperature likewise
(blank when older than --max-temp-age). Relative time is converted to UTC via
the Epoch anchors, rows are optionally downsampled, and the simulator schema
read by cmd/bmssim is written. The pack reports no per-cell values, so cell_vmax_v/cell_vmin_v
are null and temp_max_c == temp_min_c.

    python3 pipelines/audi_ingest.py "/path/Real-world electric vehicle data driving and charging" \\
        -o data/sim/audi.parquet
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from collections.abc import Iterator
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import h5py
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import scipy.io

log = logging.getLogger("audi_ingest")

SIGNALS = ("TimeCurr", "Curr", "TimeVolt", "Volt", "TimeSoC", "SoC", "TimeTemp", "Temp",
           "TimeEpoch", "Epoch")

SCHEMA = pa.schema([
    ("pack_id", pa.string()),
    ("mileage_km", pa.float32()),
    ("timestamp", pa.timestamp("ms", tz="UTC")),
    ("current_a", pa.float32()),
    ("voltage_v", pa.float32()),
    ("cell_vmax_v", pa.float32()),
    ("cell_vmin_v", pa.float32()),
    ("temp_max_c", pa.float32()),
    ("temp_min_c", pa.float32()),
    ("soc_pct", pa.float32()),
    ("phase", pa.string()),
    ("folder", pa.int16()),
    ("t_rel_s", pa.float64()),
])


@dataclass(frozen=True)
class Config:
    root: Path
    out: Path
    pack_id: str = "audi-etron"
    every_s: float = 1.0
    max_temp_age_s: float = 60.0
    max_signal_age_s: float = 5.0
    clock_tolerance_s: float = 1e-3


@dataclass
class FolderStats:
    phase: str
    folder: int
    samples_in: int
    rows_out: int
    first_utc: str
    last_utc: str
    epoch_mode: str
    volt_clock: str
    soc_clock: str
    rows_dropped_no_signal: int
    temp_missing_frac: float


@dataclass
class Manifest:
    source: str = "Audi e-tron field data, doi:10.17632/7vdkzpnjgj.2"
    ingested_at: str = ""
    root: str = ""
    every_s: float = 0.0
    folders: list[FolderStats] = field(default_factory=list)
    skipped: list[dict[str, str]] = field(default_factory=list)


def discover(root: Path) -> Iterator[tuple[str, int, Path]]:
    for phase in ("Charge", "Drive"):
        base = root / phase
        if not base.is_dir():
            raise FileNotFoundError(base)
        for entry in sorted(base.iterdir()):
            m = re.fullmatch(r"Folder(\d+)", entry.name)
            if m and (entry / "Raw.mat").is_file():
                yield phase.lower(), int(m.group(1)), entry / "Raw.mat"


def read_mat(path: Path) -> dict[str, np.ndarray]:
    if h5py.is_hdf5(path):
        with h5py.File(path, "r") as f:
            group = f["Raw"] if "Raw" in f else f
            return pick_signals(path, lambda name: group[name] if name in group else None)
    m = scipy.io.loadmat(path, squeeze_me=True, struct_as_record=False)
    raw = m.get("Raw")
    if raw is not None:
        return pick_signals(path, lambda name: getattr(raw, name, None))
    return pick_signals(path, m.get)


def pick_signals(path: Path, get) -> dict[str, np.ndarray]:
    out = {}
    for name in SIGNALS:
        value = get(name)
        if value is None:
            raise KeyError(f"{path}: missing {name}")
        out[name] = np.asarray(value, dtype=np.float64).ravel()
    return out


def sorted_signal(t: np.ndarray, v: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    ok = np.isfinite(t) & np.isfinite(v)
    order = np.argsort(t[ok], kind="stable")
    return t[ok][order], v[ok][order]


def attach(base_t: np.ndarray, t: np.ndarray, v: np.ndarray, max_age: float,
           tolerance: float) -> tuple[np.ndarray, str]:
    if len(t) == len(base_t) and np.all(np.abs(t - base_t) <= tolerance):
        return v, "shared"
    st, sv = sorted_signal(t, v)
    return asof(base_t, st, sv, max_age), "aligned"


def to_epoch(t: np.ndarray, anchors_t: np.ndarray, anchors_epoch: np.ndarray) -> tuple[np.ndarray, str]:
    ok = np.isfinite(anchors_t) & np.isfinite(anchors_epoch)
    anchors_t, anchors_epoch = anchors_t[ok], anchors_epoch[ok]
    if anchors_t.size == 0:
        raise ValueError("no Epoch anchors")
    offsets = anchors_epoch - anchors_t
    if np.ptp(offsets) < 2.0:
        return t + np.median(offsets), "offset"
    order = np.argsort(anchors_t)
    epoch = np.interp(t, anchors_t[order], anchors_epoch[order])
    before, after = t < anchors_t[order][0], t > anchors_t[order][-1]
    epoch[before] = t[before] + offsets[order][0]
    epoch[after] = t[after] + offsets[order][-1]
    return epoch, "interp"


def asof(t: np.ndarray, source_t: np.ndarray, source_v: np.ndarray, max_age: float) -> np.ndarray:
    order = np.argsort(source_t, kind="stable")
    st, sv = source_t[order], source_v[order]
    idx = np.searchsorted(st, t, side="right") - 1
    valid = idx >= 0
    out = np.full(t.shape, np.nan)
    out[valid] = sv[idx[valid]]
    age = np.full(t.shape, np.inf)
    age[valid] = t[valid] - st[idx[valid]]
    out[age > max_age] = np.nan
    return out


def downsample(t: np.ndarray, every: float) -> np.ndarray:
    if every <= 0:
        return np.arange(len(t))
    bucket = np.floor((t - t[0]) / every).astype(np.int64)
    return np.flatnonzero(np.r_[bucket[1:] != bucket[:-1], True])


def build_table(cfg: Config, phase: str, folder: int, raw: dict[str, np.ndarray]) -> tuple[pa.Table, FolderStats]:
    base_t, base_curr = raw["TimeCurr"], raw["Curr"]
    volt, volt_clock = attach(base_t, raw["TimeVolt"], raw["Volt"], cfg.max_signal_age_s, cfg.clock_tolerance_s)
    soc, soc_clock = attach(base_t, raw["TimeSoC"], raw["SoC"], cfg.max_signal_age_s, cfg.clock_tolerance_s)

    order = np.argsort(base_t, kind="stable")
    t, curr, volt, soc = base_t[order], base_curr[order], volt[order], soc[order]
    keep = np.isfinite(t) & np.isfinite(curr) & np.r_[True, np.diff(t) > 0]
    usable = keep & np.isfinite(volt) & np.isfinite(soc)
    dropped = int(np.count_nonzero(keep & ~usable))
    t, curr, volt, soc = t[usable], curr[usable], volt[usable], soc[usable]
    if t.size == 0:
        raise ValueError(f"{phase} folder {folder}: no rows with current, voltage and SOC together")

    temp = asof(t, raw["TimeTemp"], raw["Temp"], cfg.max_temp_age_s)
    epoch, mode = to_epoch(t, raw["TimeEpoch"], raw["Epoch"])

    pick = downsample(t, cfg.every_s)
    n = len(pick)
    ts_ms = np.round(epoch[pick] * 1000).astype(np.int64)
    temp_p = temp[pick].astype(np.float32)
    null_f32 = pa.nulls(n, pa.float32())

    table = pa.Table.from_arrays([
        pa.array([cfg.pack_id] * n, pa.string()),
        pa.array(np.zeros(n, np.float32)),
        pa.array(ts_ms, pa.int64()).cast(pa.timestamp("ms", tz="UTC")),
        pa.array(curr[pick].astype(np.float32)),
        pa.array(volt[pick].astype(np.float32)),
        null_f32,
        null_f32,
        pa.array(temp_p, from_pandas=True),
        pa.array(temp_p, from_pandas=True),
        pa.array(soc[pick].astype(np.float32)),
        pa.array([phase] * n, pa.string()),
        pa.array(np.full(n, folder, np.int16)),
        pa.array(t[pick]),
    ], schema=SCHEMA)

    utc = lambda ms: datetime.fromtimestamp(ms / 1000, timezone.utc).isoformat()  # noqa: E731
    stats = FolderStats(phase, folder, int(len(raw["TimeCurr"])), n, utc(ts_ms[0]), utc(ts_ms[-1]), mode,
                        volt_clock, soc_clock, dropped, float(np.mean(np.isnan(temp_p))))
    return table, stats


def run(cfg: Config) -> Manifest:
    parts: list[tuple[int, pa.Table]] = []
    manifest = Manifest(ingested_at=datetime.now(timezone.utc).isoformat(), root=str(cfg.root),
                        every_s=cfg.every_s)
    for phase, folder, path in discover(cfg.root):
        try:
            table, stats = build_table(cfg, phase, folder, read_mat(path))
        except Exception as e:
            log.error("skipping %s folder %d (%s): %s", phase, folder, path, e)
            manifest.skipped.append({"phase": phase, "folder": str(folder), "path": str(path), "error": str(e)})
            continue
        parts.append((int(table["timestamp"].cast(pa.int64())[0].as_py()), table))
        manifest.folders.append(stats)
        log.info("%s folder %d: %d samples -> %d rows, %s .. %s (epoch %s, volt %s, soc %s, "
                 "%d rows without volt/soc dropped, temp missing %.1f%%)",
                 phase, folder, stats.samples_in, stats.rows_out, stats.first_utc, stats.last_utc,
                 stats.epoch_mode, stats.volt_clock, stats.soc_clock, stats.rows_dropped_no_signal,
                 100 * stats.temp_missing_frac)
    if not parts:
        raise FileNotFoundError(f"no Raw.mat under {cfg.root}/Charge or Drive")

    parts.sort(key=lambda p: p[0])
    manifest.folders.sort(key=lambda s: s.first_utc)
    check_overlaps(manifest.folders)

    cfg.out.parent.mkdir(parents=True, exist_ok=True)
    tmp = cfg.out.with_name(f".{cfg.out.name}.tmp")
    try:
        with pq.ParquetWriter(tmp, SCHEMA, compression="zstd", use_dictionary=["pack_id", "phase"]) as w:
            for _, table in parts:
                w.write_table(table, row_group_size=max(table.num_rows, 1))
        os.replace(tmp, cfg.out)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise
    manifest_path = cfg.out.with_name(cfg.out.stem + ".manifest.json")
    manifest_path.write_text(json.dumps(asdict(manifest), indent=2))
    return manifest


def check_overlaps(folders: list[FolderStats]) -> None:
    for a, b in zip(folders, folders[1:]):
        if b.first_utc < a.last_utc:
            log.warning("%s folder %d starts before %s folder %d ends", b.phase, b.folder, a.phase, a.folder)


def parse_args(argv: list[str] | None = None) -> Config:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("root", type=Path, help="folder containing Charge/ and Drive/")
    p.add_argument("-o", "--out", type=Path, required=True)
    p.add_argument("--pack-id", default=Config.pack_id)
    p.add_argument("--every", type=float, default=Config.every_s, help="seconds per output row; 0 keeps all samples")
    p.add_argument("--max-temp-age", type=float, default=Config.max_temp_age_s)
    p.add_argument("--max-signal-age", type=float, default=Config.max_signal_age_s,
                   help="seconds a voltage/SOC sample stays valid when its clock differs from current's")
    a = p.parse_args(argv)
    return Config(root=a.root, out=a.out, pack_id=a.pack_id, every_s=a.every, max_temp_age_s=a.max_temp_age,
                  max_signal_age_s=a.max_signal_age)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    cfg = parse_args(argv)
    m = run(cfg)
    log.info("wrote %d rows from %d folders to %s", sum(f.rows_out for f in m.folders), len(m.folders), cfg.out)
    if m.skipped:
        log.warning("%d folder(s) skipped: %s", len(m.skipped),
                    ", ".join(f"{s['phase']} {s['folder']}" for s in m.skipped))
    return 1 if m.skipped else 0


if __name__ == "__main__":
    sys.exit(main())
