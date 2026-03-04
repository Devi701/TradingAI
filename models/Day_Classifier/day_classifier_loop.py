import argparse
import datetime
import gc
import json
import logging
import os
import pickle
import warnings

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
try:
    from tqdm import tqdm
except Exception:
    def tqdm(it, **_kwargs):
        return it
from sklearn.linear_model import SGDClassifier, PassiveAggressiveClassifier
from sklearn.metrics import balanced_accuracy_score, classification_report, confusion_matrix, f1_score
from sklearn.naive_bayes import GaussianNB
from sklearn.preprocessing import StandardScaler

try:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    HAS_MATPLOTLIB = True
except Exception:
    HAS_MATPLOTLIB = False
    plt = None

warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")
log = logging.getLogger(__name__)

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------
BASE_DIR = "/Users/pardeepkumarmaheshwari/Desktop/TradingAI"
DEFAULT_FEATURE_FILE = f"{BASE_DIR}/data/master_features_fixed.parquet"
MODEL_DIR = f"{BASE_DIR}/models/Day_Classifier"
ARTIFACT_DIR = f"{MODEL_DIR}/artifacts"
CM_DIR = f"{ARTIFACT_DIR}/confusion_matrices"
PLOT_DIR = f"{ARTIFACT_DIR}/plots"
DATA_LOG_FILE = f"{BASE_DIR}/data/day_accuracy_log_sgd.csv"
ARTIFACT_LOG_FILE = f"{ARTIFACT_DIR}/day_accuracy_log_sgd.csv"

os.makedirs(MODEL_DIR, exist_ok=True)
os.makedirs(CM_DIR, exist_ok=True)
os.makedirs(PLOT_DIR, exist_ok=True)

# ------------------------------------------------------------
# Date boundaries
# ------------------------------------------------------------
PHASE1_END = datetime.date(2022, 12, 31)
PHASE2_END = datetime.date(2023, 12, 31)
PHASE3_START = datetime.date(2024, 1, 1)

# ------------------------------------------------------------
# Config
# ------------------------------------------------------------
FEATURE_COLS = [
    "open",
    "high",
    "low",
    "close",
    "volume",
    "trade_count",
    "vwap",
    "atr_14",
    "vol_ratio_14",
    "bias",
    "vwap_14",
]
TARGET_COL = "target"
DAY_TYPE_CLASSES = np.arange(1, 6, dtype=int)

DEFAULT_ROWS_PER_DAY = 12_000
DEFAULT_BATCH_SIZE = 50_000
DEFAULT_WARM_UP_DAYS = 8

# Reinforcement-style expert weighting (online Hedge update).
RL_ETA = 0.12
WRONG_REWARD = -0.40
MIN_EXPERT_WEIGHT = 0.05


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Hybrid day classifier with supervised + rule + RL-style weighting")
    parser.add_argument("--feature-file", default=DEFAULT_FEATURE_FILE)
    parser.add_argument("--rows-per-day", type=int, default=DEFAULT_ROWS_PER_DAY)
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--warm-up-days", type=int, default=DEFAULT_WARM_UP_DAYS)
    parser.add_argument("--max-days", type=int, default=None, help="Safety limiter for quick smoke runs")
    parser.add_argument("--log-every", type=int, default=50, help="Emit one progress line every N evaluated samples")
    parser.add_argument("--run-tag", type=str, default="", help="Optional tag to isolate outputs (files/artifacts)")
    parser.add_argument("--disable-plots", action="store_true", help="Skip plotting for lower CPU usage")
    return parser.parse_args()


def get_phase(day: datetime.date) -> str:
    if day <= PHASE1_END:
        return "PHASE1_TRAIN"
    if day <= PHASE2_END:
        return "PHASE2_UPGRADE"
    return "PHASE3_TEST"


def entropy(probs: np.ndarray) -> float:
    p = probs[probs > 0]
    return float(-np.sum(p * np.log2(p)))


def stable_softmax(x: np.ndarray) -> np.ndarray:
    z = x - np.max(x)
    exp_z = np.exp(z)
    denom = exp_z.sum()
    if denom <= 0:
        return np.full_like(exp_z, 1.0 / len(exp_z), dtype=np.float64)
    return exp_z / denom


def normalize_probs(v: np.ndarray) -> np.ndarray:
    v = np.asarray(v, dtype=np.float64)
    v = np.where(np.isfinite(v), v, 0.0)
    v = np.clip(v, 1e-9, None)
    s = v.sum()
    if s <= 0:
        return np.full_like(v, 1.0 / len(v), dtype=np.float64)
    return v / s


def safe_div(a: float, b: float, default: float = 0.0) -> float:
    if b == 0 or not np.isfinite(b):
        return default
    out = a / b
    if not np.isfinite(out):
        return default
    return float(out)


def with_tag(path: str, run_tag: str) -> str:
    if not run_tag:
        return path
    root, ext = os.path.splitext(path)
    return f"{root}_{run_tag}{ext}"


def stream_symbol_days(parquet_path: str, rows_per_day: int, batch_size: int):
    needed = FEATURE_COLS + [TARGET_COL, "timestamp", "symbol"]
    parquet_file = pq.ParquetFile(parquet_path)

    active_by_symbol = {}
    emitted_keys = set()
    out_of_order = 0

    for batch in parquet_file.iter_batches(batch_size=batch_size, columns=needed):
        chunk = batch.to_pandas()
        if chunk.empty:
            continue
        chunk["timestamp"] = pd.to_datetime(chunk["timestamp"], errors="coerce")
        chunk = chunk.dropna(subset=["timestamp", "symbol"])
        if chunk.empty:
            continue

        chunk["date"] = chunk["timestamp"].dt.date
        chunk = chunk.sort_values(["symbol", "timestamp"]).reset_index(drop=True)

        for (symbol, day), grp in chunk.groupby(["symbol", "date"], sort=False):
            symbol = str(symbol)
            grp = grp.reset_index(drop=True)

            if symbol not in active_by_symbol:
                active_by_symbol[symbol] = {"date": day, "df": grp}
                continue

            active_date = active_by_symbol[symbol]["date"]
            if day == active_date:
                active_by_symbol[symbol]["df"] = pd.concat([active_by_symbol[symbol]["df"], grp], ignore_index=True)
                continue

            if day < active_date:
                out_of_order += 1
                key = (symbol, day)
                if key in emitted_keys:
                    continue
                late_out = grp.sort_values("timestamp").head(rows_per_day).reset_index(drop=True)
                emitted_keys.add(key)
                yield symbol, day, late_out
                continue

            prev_key = (symbol, active_date)
            if prev_key not in emitted_keys:
                prev_out = (
                    active_by_symbol[symbol]["df"]
                    .sort_values("timestamp")
                    .head(rows_per_day)
                    .reset_index(drop=True)
                )
                emitted_keys.add(prev_key)
                yield symbol, active_date, prev_out

            active_by_symbol[symbol] = {"date": day, "df": grp}

    for symbol, payload in sorted(active_by_symbol.items(), key=lambda kv: (kv[1]["date"], kv[0])):
        key = (symbol, payload["date"])
        if key in emitted_keys:
            continue
        out = payload["df"].sort_values("timestamp").head(rows_per_day).reset_index(drop=True)
        emitted_keys.add(key)
        yield symbol, payload["date"], out

    if out_of_order > 0:
        log.warning(f"Detected {out_of_order} out-of-order symbol/date groups while streaming.")


def resolve_target(y_values: np.ndarray) -> int:
    if y_values is None or len(y_values) == 0:
        return 3
    y = pd.Series(y_values).dropna()
    if y.empty:
        return 3
    y = y.astype(int)
    return int(y.mode().iloc[0])


def build_day_feature_vector(day_df: pd.DataFrame):
    day_df = day_df.copy()
    day_df = day_df.replace([np.inf, -np.inf], np.nan)

    close = day_df["close"].astype(float).to_numpy()
    open_ = day_df["open"].astype(float).to_numpy()
    high = day_df["high"].astype(float).to_numpy()
    low = day_df["low"].astype(float).to_numpy()
    volume = day_df["volume"].astype(float).to_numpy()
    vwap = day_df["vwap"].astype(float).to_numpy()

    if len(close) == 0:
        close = np.array([0.0], dtype=np.float64)
        open_ = np.array([0.0], dtype=np.float64)
        high = np.array([0.0], dtype=np.float64)
        low = np.array([0.0], dtype=np.float64)
        volume = np.array([0.0], dtype=np.float64)
        vwap = np.array([0.0], dtype=np.float64)

    start_open = float(open_[0]) if np.isfinite(open_[0]) else float(np.nanmean(open_))
    end_close = float(close[-1]) if np.isfinite(close[-1]) else float(np.nanmean(close))
    high_day = float(np.nanmax(high))
    low_day = float(np.nanmin(low))

    oc_return = safe_div(end_close - start_open, start_open, default=0.0)
    day_range = high_day - low_day
    day_range_pct = safe_div(day_range, max(abs(end_close), 1e-9), default=0.0)
    close_pos = safe_div(end_close - low_day, max(day_range, 1e-9), default=0.5)

    diff_close = np.diff(np.log(np.clip(close, 1e-9, None)))
    realized_vol = float(np.nanstd(diff_close)) if len(diff_close) else 0.0

    x_axis = np.arange(len(close), dtype=np.float64)
    if len(x_axis) > 1:
        slope = np.polyfit(x_axis, close, deg=1)[0]
    else:
        slope = 0.0
    intraday_trend = safe_div(slope, np.nanmean(np.abs(close)) + 1e-9, default=0.0)

    vol_mean = float(np.nanmean(volume))
    vol_max = float(np.nanmax(volume))
    volume_spike = safe_div(vol_max, vol_mean + 1e-9, default=1.0)

    vwap_spread = safe_div(np.nanmean(close - vwap), np.nanmean(np.abs(vwap)) + 1e-9, default=0.0)

    f = {
        "open_mean": float(np.nanmean(day_df["open"])),
        "high_mean": float(np.nanmean(day_df["high"])),
        "low_mean": float(np.nanmean(day_df["low"])),
        "close_mean": float(np.nanmean(day_df["close"])),
        "volume_mean": vol_mean,
        "volume_std": float(np.nanstd(day_df["volume"])),
        "volume_sum": float(np.nansum(day_df["volume"])),
        "trade_count_mean": float(np.nanmean(day_df["trade_count"])),
        "vwap_mean": float(np.nanmean(day_df["vwap"])),
        "atr_mean": float(np.nanmean(day_df["atr_14"])),
        "vol_ratio_mean": float(np.nanmean(day_df["vol_ratio_14"])),
        "bias_mean": float(np.nanmean(day_df["bias"])),
        "bias_std": float(np.nanstd(day_df["bias"])),
        "vwap14_mean": float(np.nanmean(day_df["vwap_14"])),
        "oc_return": oc_return,
        "day_range_pct": day_range_pct,
        "close_pos": close_pos,
        "intraday_trend": intraday_trend,
        "realized_vol": realized_vol,
        "volume_spike": volume_spike,
        "vwap_spread": vwap_spread,
    }

    for k, v in list(f.items()):
        if not np.isfinite(v):
            f[k] = 0.0

    names = sorted(f.keys())
    x = np.array([f[n] for n in names], dtype=np.float32)
    return x, f, names


def heuristic_probs(day_stats: dict, classes: np.ndarray) -> np.ndarray:
    oc_return = float(day_stats.get("oc_return", 0.0))
    close_pos = float(day_stats.get("close_pos", 0.5))
    day_range_pct = float(day_stats.get("day_range_pct", 0.0))
    realized_vol = float(day_stats.get("realized_vol", 0.0))
    volume_spike = float(day_stats.get("volume_spike", 1.0))
    vol_ratio_mean = float(day_stats.get("vol_ratio_mean", 1.0))
    intraday_trend = float(day_stats.get("intraday_trend", 0.0))

    trend_strength = np.clip(abs(oc_return) * 80.0, 0.0, 3.0)
    direction = np.sign(oc_return)

    bull = 0.30 + max(direction, 0.0) * (1.20 + trend_strength)
    bull += max(close_pos - 0.50, 0.0) * 1.0 + max(intraday_trend * 30.0, 0.0) * 0.8

    bear = 0.30 + max(-direction, 0.0) * (1.20 + trend_strength)
    bear += max(0.50 - close_pos, 0.0) * 1.0 + max(-intraday_trend * 30.0, 0.0) * 0.8

    mr = 0.35 + max(0.02 - abs(oc_return), 0.0) * 80.0
    mr += max(1.0 - abs(close_pos - 0.5) * 2.0, 0.0) * 0.8

    vol_spike = 0.25 + max(day_range_pct - 0.02, 0.0) * 60.0
    vol_spike += max(realized_vol - 0.01, 0.0) * 45.0
    vol_spike += max(volume_spike - 1.6, 0.0) * 0.9
    vol_spike += max(vol_ratio_mean - 1.2, 0.0) * 0.6

    dead = 0.20 + max(0.012 - day_range_pct, 0.0) * 70.0
    dead += max(0.006 - abs(oc_return), 0.0) * 110.0
    dead += max(1.15 - volume_spike, 0.0) * 0.9

    score_by_class = {
        1: bull,
        2: bear,
        3: mr,
        4: vol_spike,
        5: dead,
    }
    scores = np.array([score_by_class.get(int(c), 0.25) for c in classes], dtype=np.float64)
    return normalize_probs(scores)


def pa_probs(pa_model: PassiveAggressiveClassifier, x: np.ndarray, classes: np.ndarray) -> np.ndarray:
    decision = pa_model.decision_function(x.reshape(1, -1))
    decision = np.asarray(decision, dtype=np.float64)
    if decision.ndim == 1:
        decision = decision.reshape(1, -1)
    raw = decision[0]
    if len(raw) != len(classes):
        out = np.zeros(len(classes), dtype=np.float64)
        cls_to_idx = {int(c): i for i, c in enumerate(pa_model.classes_)}
        for i, c in enumerate(classes):
            out[i] = raw[cls_to_idx[int(c)]] if int(c) in cls_to_idx else -8.0
        raw = out
    return stable_softmax(raw)


def get_model_probs(
    models: dict,
    scalers: dict,
    trained: dict,
    x_raw: np.ndarray,
    day_stats: dict,
    classes: np.ndarray,
):
    expert_probs = {}

    if trained["sgd"]:
        x = scalers["sgd"].transform(x_raw.reshape(1, -1))
        p = models["sgd"].predict_proba(x)[0]
        expert_probs["sgd"] = normalize_probs(p)

    if trained["pa"]:
        x = scalers["pa"].transform(x_raw.reshape(1, -1))
        p = pa_probs(models["pa"], x[0], classes)
        expert_probs["pa"] = normalize_probs(p)

    if trained["gnb"]:
        p = models["gnb"].predict_proba(x_raw.reshape(1, -1))[0]
        expert_probs["gnb"] = normalize_probs(p)

    expert_probs["rule"] = heuristic_probs(day_stats, classes)
    return expert_probs


def weighted_ensemble_probs(expert_probs: dict, expert_weights: dict, classes: np.ndarray) -> np.ndarray:
    total_weight = 0.0
    mix = np.zeros(len(classes), dtype=np.float64)
    for name, probs in expert_probs.items():
        w = float(expert_weights.get(name, 0.0))
        if w <= 0:
            continue
        mix += w * normalize_probs(probs)
        total_weight += w
    if total_weight <= 0:
        return np.full(len(classes), 1.0 / len(classes), dtype=np.float64)
    return normalize_probs(mix / total_weight)


def update_expert_weights(expert_weights: dict, expert_preds: dict, y_true: int):
    for name, pred in expert_preds.items():
        reward = 1.0 if int(pred) == int(y_true) else WRONG_REWARD
        expert_weights[name] = float(expert_weights[name] * np.exp(RL_ETA * reward))

    floor = MIN_EXPERT_WEIGHT
    for k in list(expert_weights.keys()):
        expert_weights[k] = max(expert_weights[k], floor)
    s = float(sum(expert_weights.values()))
    for k in list(expert_weights.keys()):
        expert_weights[k] /= s


def save_confusion_artifacts(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    labels: np.ndarray,
    name: str,
    cm_dir: str = CM_DIR,
):
    if len(y_true) == 0:
        return

    cm = confusion_matrix(y_true, y_pred, labels=labels)
    cm_df = pd.DataFrame(
        cm,
        index=[f"true_{int(c)}" for c in labels],
        columns=[f"pred_{int(c)}" for c in labels],
    )
    cm_df.to_csv(os.path.join(cm_dir, f"{name}.csv"))

    if HAS_MATPLOTLIB:
        fig, ax = plt.subplots(figsize=(6, 5))
        im = ax.imshow(cm, cmap="Blues")
        fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
        ax.set_xticks(np.arange(len(labels)))
        ax.set_yticks(np.arange(len(labels)))
        ax.set_xticklabels([int(c) for c in labels])
        ax.set_yticklabels([int(c) for c in labels])
        ax.set_xlabel("Predicted")
        ax.set_ylabel("True")
        ax.set_title(f"Confusion Matrix - {name}")
        for i in range(cm.shape[0]):
            for j in range(cm.shape[1]):
                ax.text(j, i, str(cm[i, j]), ha="center", va="center", color="black")
        fig.tight_layout()
        fig.savefig(os.path.join(cm_dir, f"{name}.png"), dpi=180)
        plt.close(fig)


def save_plot_artifacts(records_df: pd.DataFrame, phase_stats: dict, allow_plots: bool, plot_dir: str = PLOT_DIR):
    if records_df.empty or not allow_plots or not HAS_MATPLOTLIB:
        return

    df = records_df.sort_values("date").copy()
    df["date"] = pd.to_datetime(df["date"])
    df["rolling_acc_30"] = df["correct"].rolling(window=30, min_periods=5).mean()

    fig, ax = plt.subplots(figsize=(11, 6))
    ax.plot(df["date"], df["cumulative_acc"], linewidth=2, label="Cumulative Accuracy")
    ax.plot(df["date"], df["balanced_acc"], linewidth=1.5, label="Balanced Accuracy")
    ax.plot(df["date"], df["macro_f1"], linewidth=1.5, label="Macro F1")
    ax.set_title("Hybrid Model Metrics Over Time")
    ax.set_xlabel("Date")
    ax.set_ylabel("Score")
    ax.set_ylim(0.0, 1.0)
    ax.grid(alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(os.path.join(plot_dir, "metrics_over_time.png"), dpi=180)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(11, 6))
    for phase in ["PHASE1_TRAIN", "PHASE2_UPGRADE", "PHASE3_TEST"]:
        phase_df = df[df["phase"] == phase]
        if not phase_df.empty:
            ax.plot(phase_df["date"], phase_df["rolling_acc_30"], linewidth=2, label=phase)
    ax.set_title("30-Day Rolling Accuracy by Phase")
    ax.set_xlabel("Date")
    ax.set_ylabel("Rolling Accuracy")
    ax.set_ylim(0.0, 1.0)
    ax.grid(alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(os.path.join(plot_dir, "rolling_accuracy_by_phase.png"), dpi=180)
    plt.close(fig)

    phase_rows = []
    for phase in ["PHASE1_TRAIN", "PHASE2_UPGRADE", "PHASE3_TEST"]:
        total = phase_stats[phase]["total"]
        correct = phase_stats[phase]["correct"]
        acc = (correct / total) if total else np.nan
        phase_rows.append({"phase": phase, "accuracy": acc})
    phase_df = pd.DataFrame(phase_rows).dropna()

    if not phase_df.empty:
        fig, ax = plt.subplots(figsize=(8, 5))
        ax.bar(phase_df["phase"], phase_df["accuracy"], color=["#4E79A7", "#F28E2B", "#59A14F"])
        ax.set_title("Accuracy by Phase")
        ax.set_xlabel("Phase")
        ax.set_ylabel("Accuracy")
        ax.set_ylim(0.0, 1.0)
        ax.grid(axis="y", alpha=0.3)
        for idx, val in enumerate(phase_df["accuracy"]):
            ax.text(idx, float(val) + 0.015, f"{val:.2%}", ha="center", va="bottom")
        fig.tight_layout()
        fig.savefig(os.path.join(plot_dir, "phase_accuracy.png"), dpi=180)
        plt.close(fig)


def save_classification_reports(records_df: pd.DataFrame, artifact_dir: str = ARTIFACT_DIR):
    if records_df.empty:
        return

    report = classification_report(
        records_df["y_true"],
        records_df["y_pred"],
        output_dict=True,
        zero_division=0,
    )
    pd.DataFrame(report).transpose().to_csv(
        os.path.join(artifact_dir, "classification_report_overall.csv"),
        index=True,
    )

    for phase in ["PHASE1_TRAIN", "PHASE2_UPGRADE", "PHASE3_TEST"]:
        phase_df = records_df[records_df["phase"] == phase]
        if phase_df.empty:
            continue
        phase_report = classification_report(
            phase_df["y_true"],
            phase_df["y_pred"],
            output_dict=True,
            zero_division=0,
        )
        pd.DataFrame(phase_report).transpose().to_csv(
            os.path.join(artifact_dir, f"classification_report_{phase.lower()}.csv"),
            index=True,
        )


def save_checkpoint(path: str, state: dict):
    with open(path, "wb") as f:
        pickle.dump(state, f)


def main():
    args = parse_args()
    allow_plots = (not args.disable_plots)
    run_tag = args.run_tag.strip()
    if args.max_days is not None and not run_tag:
        run_tag = f"max{args.max_days}"

    run_artifact_dir = ARTIFACT_DIR if not run_tag else os.path.join(ARTIFACT_DIR, "runs", run_tag)
    run_cm_dir = CM_DIR if not run_tag else os.path.join(run_artifact_dir, "confusion_matrices")
    run_plot_dir = PLOT_DIR if not run_tag else os.path.join(run_artifact_dir, "plots")
    os.makedirs(run_artifact_dir, exist_ok=True)
    os.makedirs(run_cm_dir, exist_ok=True)
    os.makedirs(run_plot_dir, exist_ok=True)

    data_log_path = with_tag(DATA_LOG_FILE, run_tag)
    artifact_log_path = os.path.join(run_artifact_dir, "day_accuracy_log_sgd.csv")
    phase1_ckpt_path = with_tag(os.path.join(MODEL_DIR, "day_classifier_phase1_end2022.pkl"), run_tag)
    phase2_ckpt_path = with_tag(os.path.join(MODEL_DIR, "day_classifier_phase2_end2023.pkl"), run_tag)
    phase2_meta_path = os.path.join(run_artifact_dir, "phase2_locked_hybrid_meta.json")
    final_hybrid_path = with_tag(os.path.join(MODEL_DIR, "day_classifier_hybrid_final.pkl"), run_tag)
    final_compat_path = with_tag(os.path.join(MODEL_DIR, "day_classifier_sgd_final.pkl"), run_tag)
    final_expert_weights_path = os.path.join(run_artifact_dir, "final_expert_weights.json")

    classes = DAY_TYPE_CLASSES.copy()

    models = {
        "sgd": SGDClassifier(
            loss="log_loss",
            penalty="l2",
            alpha=8e-5,
            max_iter=1,
            tol=None,
            warm_start=True,
            random_state=42,
        ),
        "pa": PassiveAggressiveClassifier(
            C=0.50,
            loss="hinge",
            max_iter=1,
            tol=None,
            warm_start=True,
            random_state=42,
        ),
        "gnb": GaussianNB(var_smoothing=1e-9),
    }
    scalers = {
        "sgd": StandardScaler(),
        "pa": StandardScaler(),
    }
    trained = {"sgd": False, "pa": False, "gnb": False}

    expert_weights = {"sgd": 0.25, "pa": 0.25, "gnb": 0.25, "rule": 0.25}

    records = []
    y_true_all = []
    y_pred_all = []
    phase_stats = {
        "PHASE1_TRAIN": {"correct": 0, "total": 0},
        "PHASE2_UPGRADE": {"correct": 0, "total": 0},
        "PHASE3_TEST": {"correct": 0, "total": 0},
    }

    total_correct = 0
    days_seen = 0
    phase1_saved = False
    phase2_saved = False
    class_day_counts = {int(c): 0 for c in classes}
    feature_names = None

    log.info("=" * 72)
    log.info("Starting HYBRID three-phase day classifier")
    log.info("Models: SGDClassifier + PassiveAggressive + GaussianNB + Rule Scoring")
    log.info("RL-style weighting: online Hedge updates in Phase1+Phase2 only")
    log.info(f"Feature file: {args.feature_file}")
    log.info(f"rows_per_day={args.rows_per_day} batch_size={args.batch_size} warm_up_days={args.warm_up_days}")
    log.info(f"max_days={args.max_days}")
    log.info(f"log_every={args.log_every}")
    log.info(f"run_tag={run_tag if run_tag else 'default'}")
    log.info(f"Phase 1 TRAIN   : start       -> {PHASE1_END}")
    log.info(f"Phase 2 UPGRADE : 2023-01-01  -> {PHASE2_END}")
    log.info(f"Phase 3 TEST    : {PHASE3_START} -> present [LOCKED]")
    log.info("=" * 72)

    for symbol, day, day_df in tqdm(
        stream_symbol_days(args.feature_file, args.rows_per_day, args.batch_size),
        desc="Days",
    ):
        if args.max_days is not None and days_seen >= args.max_days:
            log.info(f"Reached --max-days={args.max_days}; stopping early for safety.")
            break

        phase = get_phase(day)

        x_day, day_stats, names = build_day_feature_vector(day_df)
        if feature_names is None:
            feature_names = names

        y_day = day_df[TARGET_COL].astype(int).values
        y_true = resolve_target(y_day)

        if not phase1_saved and phase != "PHASE1_TRAIN":
            save_checkpoint(
                phase1_ckpt_path,
                {
                    "models": models,
                    "scalers": scalers,
                    "trained": trained,
                    "expert_weights": expert_weights,
                    "classes": classes,
                    "feature_names": feature_names,
                    "pipeline": "hybrid_supervised_rule_rl",
                },
            )
            log.info(f"Phase 1 checkpoint saved -> {phase1_ckpt_path}")
            phase1_saved = True

        if not phase2_saved and phase == "PHASE3_TEST":
            save_checkpoint(
                phase2_ckpt_path,
                {
                    "models": models,
                    "scalers": scalers,
                    "trained": trained,
                    "expert_weights": expert_weights,
                    "classes": classes,
                    "feature_names": feature_names,
                    "pipeline": "hybrid_supervised_rule_rl",
                    "locked_from": str(PHASE3_START),
                },
            )
            phase2_meta = {
                "locked_from": str(PHASE3_START),
                "expert_weights": {k: round(float(v), 6) for k, v in expert_weights.items()},
            }
            with open(phase2_meta_path, "w") as f:
                json.dump(phase2_meta, f, indent=2)
            log.info(f"Phase 2 checkpoint saved -> {phase2_ckpt_path}")
            log.info("Model and RL weights now LOCKED for Phase 3")
            phase2_saved = True

        # PREDICT
        if days_seen >= args.warm_up_days:
            expert_probs = get_model_probs(models, scalers, trained, x_day, day_stats, classes)
            expert_preds = {
                name: int(classes[int(np.argmax(prob_vec))]) for name, prob_vec in expert_probs.items()
            }
            expert_conf = {name: float(np.max(prob_vec)) for name, prob_vec in expert_probs.items()}

            day_prob_vector = weighted_ensemble_probs(expert_probs, expert_weights, classes)
            y_pred = int(classes[int(np.argmax(day_prob_vector))])
            conf = float(np.max(day_prob_vector))
            day_entropy = entropy(day_prob_vector)
            acc = int(y_pred == y_true)

            total_correct += acc
            phase_stats[phase]["correct"] += acc
            phase_stats[phase]["total"] += 1
            y_true_all.append(y_true)
            y_pred_all.append(y_pred)

            cumulative_acc = total_correct / len(y_true_all)
            bal_acc = balanced_accuracy_score(y_true_all, y_pred_all)
            macro_f1 = f1_score(y_true_all, y_pred_all, average="macro", zero_division=0)

            records.append(
                {
                    "phase": phase,
                    "symbol": symbol,
                    "date": day,
                    "sample_key": f"{symbol}|{day}",
                    "y_true": y_true,
                    "y_pred": y_pred,
                    "correct": acc,
                    "confidence": round(conf, 4),
                    "entropy": round(day_entropy, 4),
                    "cumulative_acc": round(cumulative_acc, 4),
                    "balanced_acc": round(float(bal_acc), 4),
                    "macro_f1": round(float(macro_f1), 4),
                    "prob_vector": day_prob_vector.round(6).tolist(),
                    "expert_weights": {k: round(float(v), 5) for k, v in expert_weights.items()},
                    "expert_preds": expert_preds,
                    "expert_conf": {k: round(float(v), 4) for k, v in expert_conf.items()},
                    "feature_oc_return": round(float(day_stats.get("oc_return", 0.0)), 6),
                    "feature_day_range_pct": round(float(day_stats.get("day_range_pct", 0.0)), 6),
                }
            )

            if len(y_true_all) % max(args.log_every, 1) == 0:
                log.info(
                    f"[{phase}] {day} {symbol} | true={y_true} pred={y_pred} acc={acc} "
                    f"cum={cumulative_acc:.2%} bal={bal_acc:.3f} f1={macro_f1:.3f} "
                    f"conf={conf:.3f} H={day_entropy:.3f} "
                    f"w={{{', '.join([f'{k}:{expert_weights[k]:.2f}' for k in ['sgd','pa','gnb','rule']])}}}"
                )

            if phase in ("PHASE1_TRAIN", "PHASE2_UPGRADE"):
                update_expert_weights(expert_weights, expert_preds, y_true)
        else:
            log.info(f"[{phase}] {day} {symbol} | warm-up {days_seen + 1}/{args.warm_up_days}")

        # TRAIN (Phase 1 + Phase 2 only)
        if phase in ("PHASE1_TRAIN", "PHASE2_UPGRADE"):
            label = int(y_true)
            if label not in class_day_counts:
                log.warning(f"Skipping training for {day}: unseen class={label}")
                days_seen += 1
                gc.collect()
                continue

            class_day_counts[label] += 1
            seen_train_days = sum(class_day_counts.values())
            label_count = class_day_counts[label]
            day_weight = seen_train_days / (len(class_day_counts) * label_count)
            sample_weight = np.array([day_weight], dtype=np.float64)

            for scaler_name in ["sgd", "pa"]:
                scalers[scaler_name].partial_fit(x_day.reshape(1, -1))

            x_sgd = scalers["sgd"].transform(x_day.reshape(1, -1))
            x_pa = scalers["pa"].transform(x_day.reshape(1, -1))
            y_sample = np.array([y_true], dtype=int)

            if not trained["sgd"]:
                models["sgd"].partial_fit(x_sgd, y_sample, classes=classes, sample_weight=sample_weight)
                trained["sgd"] = True
            else:
                models["sgd"].partial_fit(x_sgd, y_sample, sample_weight=sample_weight)

            if not trained["pa"]:
                models["pa"].partial_fit(x_pa, y_sample, classes=classes)
                trained["pa"] = True
            else:
                models["pa"].partial_fit(x_pa, y_sample)

            if not trained["gnb"]:
                models["gnb"].partial_fit(x_day.reshape(1, -1), y_sample, classes=classes, sample_weight=sample_weight)
                trained["gnb"] = True
            else:
                models["gnb"].partial_fit(x_day.reshape(1, -1), y_sample, sample_weight=sample_weight)

        days_seen += 1
        gc.collect()

    records_df = pd.DataFrame(records)

    # Save logs to both paths for compatibility.
    records_df.to_csv(data_log_path, index=False)
    records_df.to_csv(artifact_log_path, index=False)
    log.info(f"Log saved -> {data_log_path}")
    log.info(f"Log saved -> {artifact_log_path}")

    final_state = {
        "models": models,
        "scalers": scalers,
        "trained": trained,
        "expert_weights": expert_weights,
        "classes": classes,
        "feature_names": feature_names,
        "pipeline": "hybrid_supervised_rule_rl",
        "rl_eta": RL_ETA,
        "wrong_reward": WRONG_REWARD,
    }

    save_checkpoint(final_hybrid_path, final_state)
    # Backward-compatible name used by existing paths (tagged for isolated runs).
    save_checkpoint(final_compat_path, final_state)
    log.info(f"Final model saved -> {final_hybrid_path}")
    log.info(f"Compatibility model saved -> {final_compat_path}")

    evaluated = len(y_true_all)
    if evaluated:
        final_bal = balanced_accuracy_score(y_true_all, y_pred_all)
        final_f1 = f1_score(y_true_all, y_pred_all, average="macro", zero_division=0)

        labels = np.array(sorted(np.unique(np.concatenate([np.array(y_true_all), np.array(y_pred_all)]))))
        save_confusion_artifacts(np.array(y_true_all), np.array(y_pred_all), labels, "overall", cm_dir=run_cm_dir)
        for phase in ["PHASE1_TRAIN", "PHASE2_UPGRADE", "PHASE3_TEST"]:
            phase_df = records_df[records_df["phase"] == phase]
            if phase_df.empty:
                continue
            save_confusion_artifacts(
                phase_df["y_true"].to_numpy(),
                phase_df["y_pred"].to_numpy(),
                labels,
                phase.lower(),
                cm_dir=run_cm_dir,
            )

        save_classification_reports(records_df, artifact_dir=run_artifact_dir)
        save_plot_artifacts(records_df, phase_stats, allow_plots, plot_dir=run_plot_dir)

        with open(final_expert_weights_path, "w") as f:
            json.dump({k: round(float(v), 6) for k, v in expert_weights.items()}, f, indent=2)

        log.info("=" * 72)
        log.info(f"Total evaluated: {evaluated}")
        log.info(f"Overall raw acc:      {total_correct}/{evaluated} = {total_correct / evaluated:.2%}")
        log.info(f"Overall balanced acc: {final_bal:.4f}")
        log.info(f"Overall macro F1:     {final_f1:.4f}")
        log.info(f"Final expert weights: {json.dumps({k: round(v, 4) for k, v in expert_weights.items()})}")
        log.info("-" * 72)
        for phase, stats in phase_stats.items():
            if stats["total"] > 0:
                pct = stats["correct"] / stats["total"]
                log.info(f"{phase:22s}  {stats['correct']}/{stats['total']} = {pct:.2%}")
        log.info("=" * 72)
        log.info(f"Diagnostics saved -> {run_artifact_dir}")
    else:
        log.warning("No days were evaluated.")


if __name__ == "__main__":
    main()
