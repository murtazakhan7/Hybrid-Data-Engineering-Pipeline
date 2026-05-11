# ingestion/load_dataset.py
# ─────────────────────────────────────────────────────
# Step 1: Load dataset from Kaggle and save locally
# ─────────────────────────────────────────────────────

import os
import sys
import logging

import kagglehub
from kagglehub import KaggleDatasetAdapter

# ── path fix so sibling packages resolve ─────────────
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import DATASET_KAGGLE_ID, DATASET_LOCAL_PATH

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


def load_and_save() -> None:
    """Download dataset via kagglehub and persist as local CSV."""
    os.makedirs(os.path.dirname(DATASET_LOCAL_PATH), exist_ok=True)

    log.info("Downloading dataset: %s", DATASET_KAGGLE_ID)
    df = kagglehub.load_dataset(
        KaggleDatasetAdapter.PANDAS,
        DATASET_KAGGLE_ID,
        "",          # file_path — empty = first CSV found
    )

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
