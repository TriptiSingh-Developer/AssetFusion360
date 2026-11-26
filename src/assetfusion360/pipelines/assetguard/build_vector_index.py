
'''
AssetFusion360 | Semantic Embedding Pipeline

Full-corpus embedding generation with
batch checkpointing and recovery.
'''

from pathlib import Path
import json
import time
import hashlib

import numpy as np
import pandas as pd
import torch

from sentence_transformers import SentenceTransformer


ROOT = Path(__file__).resolve().parents[4]

INDEX_DIR = ROOT / "data" / "09_semantic_search_index"

METADATA_FILE = INDEX_DIR / "maintenance_metadata.parquet"

CHECKPOINT_DIR = INDEX_DIR / "embedding_checkpoints"

MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"

CHECKPOINT_SIZE = 5000
INFERENCE_BATCH_SIZE = 128
EMBEDDING_DIMENSION = 384


def main():

    CHECKPOINT_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    torch.set_num_threads(4)

    print("=" * 75)
    print("ASSETFUSION360 | SEMANTIC EMBEDDING GENERATION")
    print("=" * 75)

    corpus = pd.read_parquet(
        METADATA_FILE,
        columns=[
            "vector_id",
            "normalized_description"
        ]
    ).sort_values("vector_id")

    total = len(corpus)

    assert np.array_equal(
        corpus["vector_id"].to_numpy(),
        np.arange(total)
    )

    descriptions = corpus[
        "normalized_description"
    ].tolist()

    metadata_hash = hashlib.sha256(
        METADATA_FILE.read_bytes()
    ).hexdigest()

    manifest = {
        "metadata_hash": metadata_hash,
        "model": MODEL_NAME,
        "checkpoint_size": CHECKPOINT_SIZE,
        "embedding_dimension": EMBEDDING_DIMENSION,
        "total_descriptions": total
    }

    manifest_file = CHECKPOINT_DIR / "manifest.json"

    if manifest_file.exists():

        previous = json.loads(
            manifest_file.read_text(encoding="utf-8")
        )

        if previous != manifest:

            raise RuntimeError(
                "Existing checkpoints belong to a different "
                "corpus or embedding configuration."
            )

    else:

        if list(CHECKPOINT_DIR.glob("batch_*.npy")):

            raise RuntimeError(
                "Checkpoints exist without a manifest."
            )

        manifest_file.write_text(
            json.dumps(manifest, indent=4),
            encoding="utf-8"
        )

    total_batches = (
        total + CHECKPOINT_SIZE - 1
    ) // CHECKPOINT_SIZE

    print("TOTAL DESCRIPTIONS:", total)
    print("TOTAL BATCHES:", total_batches)
    print("DEVICE: CPU")

    print("\nLoading embedding model...")

    model = SentenceTransformer(
        MODEL_NAME,
        device="cpu"
    )

    start_time = time.time()

    for batch_number, start in enumerate(
        range(0, total, CHECKPOINT_SIZE),
        start=1
    ):

        end = min(start + CHECKPOINT_SIZE, total)

        output_file = (
            CHECKPOINT_DIR
            / f"batch_{batch_number:04d}.npy"
        )

        if output_file.exists():

            try:

                existing = np.load(
                    output_file,
                    allow_pickle=False
                )

                if (
                    existing.shape
                    == (end - start, EMBEDDING_DIMENSION)
                    and existing.dtype == np.float32
                    and np.isfinite(existing).all()
                ):

                    print(
                        f"Batch {batch_number}/{total_batches}"
                        " | CHECKPOINT EXISTS",
                        flush=True
                    )

                    continue

            except Exception:

                pass

        batch_start = time.time()

        vectors = model.encode(

            descriptions[start:end],

            batch_size=INFERENCE_BATCH_SIZE,

            normalize_embeddings=True,

            convert_to_numpy=True,

            show_progress_bar=False

        ).astype(np.float32)

        assert vectors.shape == (
            end - start,
            EMBEDDING_DIMENSION
        )

        assert np.isfinite(vectors).all()

        temporary_file = output_file.with_suffix(
            ".tmp.npy"
        )

        np.save(temporary_file, vectors)

        temporary_file.replace(output_file)

        duration = time.time() - batch_start

        progress = end / total * 100

        print(
            f"Batch {batch_number}/{total_batches}"
            f" | Records: {end:,}/{total:,}"
            f" | Progress: {progress:.2f}%"
            f" | Time: {duration:.1f}s",
            flush=True
        )

    summary = {

        "status": "SUCCESS",

        "embedding_model": MODEL_NAME,

        "total_embeddings": total,

        "embedding_dimension": EMBEDDING_DIMENSION,

        "total_batches": total_batches,

        "normalized_embeddings": True,

        "elapsed_minutes": round(
            (time.time() - start_time) / 60,
            2
        )

    }

    summary_file = (
        INDEX_DIR / "embedding_summary.json"
    )

    summary_file.write_text(
        json.dumps(summary, indent=4),
        encoding="utf-8"
    )

    print("\nEMBEDDING SUMMARY")

    print(json.dumps(summary, indent=4))

    print("\nEMBEDDING GENERATION: SUCCESS")


if __name__ == "__main__":

    main()
