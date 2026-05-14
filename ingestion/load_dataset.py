# ingestion/load_dataset.py
# ─────────────────────────────────────────────────────
# Step 1: Load dataset from Kaggle and save locally
# ─────────────────────────────────────────────────────

import os
import sys
import glob
import logging

import kagglehub
import pandas as pd

# ── path fix so sibling packages resolve ─────────────
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import DATASET_KAGGLE_ID, DATASET_LOCAL_PATH

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


def _find_datafile(dataset_dir: str) -> str:
    """
    Return the path of the first readable tabular file in dataset_dir.
    Handles:
      - .csv / .tsv files (with extension)
      - Kaggle files stored without any extension (sniff first bytes)
    """
    # 1. Prefer explicit CSV/TSV files
    for pattern in ("**/*.csv", "**/*.tsv"):
        matches = glob.glob(os.path.join(dataset_dir, pattern), recursive=True)
        if matches:
            return matches[0]

    # 2. Fall back to extension-less files — try to read as CSV
    all_files = [
        os.path.join(root, f)
        for root, _, files in os.walk(dataset_dir)
        for f in files
        if "." not in f  # no extension
    ]
    for path in all_files:
        try:
            # Sniff: if the first line parses as CSV with >1 column it's tabular
            pd.read_csv(path, nrows=1)
            log.info("Detected extension-less CSV: %s", path)
            return path
        except Exception:
            continue

    raise FileNotFoundError(
        f"No readable tabular file found in downloaded dataset at: {dataset_dir}\n"
        f"Files present: {list(os.walk(dataset_dir))}"
    )


def load_and_save() -> None:
    """Download dataset via kagglehub and persist as local CSV."""
    os.makedirs(os.path.dirname(DATASET_LOCAL_PATH), exist_ok=True)

    log.info("Downloading dataset: %s", DATASET_KAGGLE_ID)

    # dataset_download() returns the local directory the archive was
    # extracted to — works with all kagglehub versions and avoids the
    # deprecated load_dataset() / file-extension detection issue.
    dataset_dir = kagglehub.dataset_download(DATASET_KAGGLE_ID)
    log.info("Dataset downloaded to: %s", dataset_dir)

    data_path = _find_datafile(dataset_dir)
    log.info("Reading file: %s", data_path)
    df = pd.read_csv(data_path)

    log.info("Dataset loaded — shape: %s", df.shape)
    log.info("Columns: %s", list(df.columns))
    log.info("First 5 rows:\n%s", df.head())

    # ── Basic sanity checks ───────────────────────────
    log.info("Null counts:\n%s", df.isnull().sum())
    log.info("Dtypes:\n%s", df.dtypes)

    # ── Persist locally ───────────────────────────────
    df.to_csv(DATASET_LOCAL_PATH, index=False)
    log.info("Saved to %s (%d rows)", DATASET_LOCAL_PATH, len(df))


if __name__ == "__main__":
    load_and_save()