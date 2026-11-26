
'''
AssetFusion360 | Compressed Semantic Vector Index

FAISS IVF Product Quantization using
checkpointed MiniLM embeddings.
'''

from pathlib import Path
import json
import time
import hashlib

import faiss
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[4]

INDEX_DIR = ROOT / "data" / "09_semantic_search_index"

CHECKPOINT_DIR = INDEX_DIR / "embedding_checkpoints"

METADATA_FILE = INDEX_DIR / "maintenance_metadata.parquet"

INDEX_FILE = INDEX_DIR / "maintenance_vectors.faiss"

DIMENSION = 384
NLIST = 1024
PQ_M = 48
PQ_BITS = 8
NPROBE = 32
TRAIN_SIZE = 100000
RANDOM_STATE = 42


def main():

    start_time = time.time()

    faiss.omp_set_num_threads(4)

    print("=" * 75)
    print("ASSETFUSION360 | FAISS VECTOR INDEX")
    print("=" * 75)

    manifest = json.loads(
        (CHECKPOINT_DIR / "manifest.json").read_text(
            encoding="utf-8"
        )
    )

    metadata_hash = hashlib.sha256(
        METADATA_FILE.read_bytes()
    ).hexdigest()

    assert manifest["metadata_hash"] == metadata_hash

    total = manifest["total_descriptions"]

    checkpoint_size = manifest["checkpoint_size"]

    expected_batches = (
        total + checkpoint_size - 1
    ) // checkpoint_size

    files = [
        CHECKPOINT_DIR / f"batch_{i:04d}.npy"
        for i in range(1, expected_batches + 1)
    ]

    print("EXPECTED VECTORS:", total)
    print("EXPECTED BATCHES:", expected_batches)

    # -------------------------------------------------------
    # 1. VALIDATE EMBEDDING CHECKPOINTS
    # -------------------------------------------------------

    counts = []

    for i, file in enumerate(files):

        assert file.exists(), f"Missing checkpoint: {file}"

        vectors = np.load(
            file,
            mmap_mode="r",
            allow_pickle=False
        )

        expected_rows = min(
            checkpoint_size,
            total - i * checkpoint_size
        )

        assert vectors.shape == (
            expected_rows,
            DIMENSION
        )

        assert vectors.dtype == np.float32

        assert np.isfinite(vectors).all()

        counts.append(len(vectors))

    assert sum(counts) == total

    print("\nCHECKPOINT VALIDATION: PASSED")

    # -------------------------------------------------------
    # 2. CREATE REPRESENTATIVE TRAINING SAMPLE
    # -------------------------------------------------------

    rng = np.random.default_rng(RANDOM_STATE)

    sample_ids = np.sort(
        rng.choice(
            total,
            size=min(TRAIN_SIZE, total),
            replace=False
        )
    )

    training_batches = []

    offset = 0

    for file, count in zip(files, counts):

        vectors = np.load(
            file,
            mmap_mode="r",
            allow_pickle=False
        )

        selected = sample_ids[
            (sample_ids >= offset)
            & (sample_ids < offset + count)
        ] - offset

        if len(selected):

            training_batches.append(
                vectors[selected].copy()
            )

        offset += count

    training_vectors = np.ascontiguousarray(
        np.concatenate(training_batches),
        dtype=np.float32
    )

    del training_batches

    print(
        "\nTRAINING SAMPLE:",
        training_vectors.shape
    )

    # -------------------------------------------------------
    # 3. TRAIN COMPRESSED IVF-PQ INDEX
    # -------------------------------------------------------

    quantizer = faiss.IndexFlatIP(DIMENSION)

    index = faiss.IndexIVFPQ(
        quantizer,
        DIMENSION,
        NLIST,
        PQ_M,
        PQ_BITS,
        faiss.METRIC_INNER_PRODUCT
    )

    print("\nTraining FAISS IVF-PQ index...")
    print("This may take several minutes.", flush=True)

    index.train(training_vectors)

    assert index.is_trained

    del training_vectors

    print("\nINDEX TRAINING: SUCCESS")

    # -------------------------------------------------------
    # 4. ADD ALL EMBEDDING VECTORS
    # -------------------------------------------------------

    offset = 0

    for batch_number, file in enumerate(
        files,
        start=1
    ):

        vectors = np.ascontiguousarray(
            np.load(file, allow_pickle=False),
            dtype=np.float32
        )

        vector_ids = np.arange(
            offset,
            offset + len(vectors),
            dtype=np.int64
        )

        index.add_with_ids(
            vectors,
            vector_ids
        )

        offset += len(vectors)

        print(
            f"Batch {batch_number}/{expected_batches}"
            f" | Indexed: {offset:,}/{total:,}"
            f" | Progress: {offset / total * 100:.2f}%",
            flush=True
        )

    assert index.ntotal == total

    index.nprobe = NPROBE

    # -------------------------------------------------------
    # 5. SAVE THE VECTOR INDEX
    # -------------------------------------------------------

    temporary_file = INDEX_FILE.with_suffix(".tmp.faiss")

    faiss.write_index(
        index,
        str(temporary_file)
    )

    temporary_file.replace(INDEX_FILE)

    # -------------------------------------------------------
    # 6. RELOAD AND VALIDATE
    # -------------------------------------------------------

    loaded_index = faiss.read_index(
        str(INDEX_FILE)
    )

    loaded_index.nprobe = NPROBE

    assert loaded_index.ntotal == total

    assert loaded_index.d == DIMENSION

    assert loaded_index.is_trained

    # Basic retrieval smoke test

    query = np.ascontiguousarray(
        np.load(files[0])[:1],
        dtype=np.float32
    )

    scores, ids = loaded_index.search(
        query,
        10
    )

    assert ids.shape == (1, 10)

    assert (ids >= 0).all()

    assert (ids < total).all()

    # -------------------------------------------------------
    # 7. SAVE INDEX GOVERNANCE METADATA
    # -------------------------------------------------------

    summary = {

        "status": "SUCCESS",

        "index_type": "IndexIVFPQ",

        "embedding_model": manifest["model"],

        "metadata_hash": metadata_hash,

        "total_vectors": int(loaded_index.ntotal),

        "embedding_dimension": DIMENSION,

        "nlist": NLIST,

        "pq_subquantizers": PQ_M,

        "pq_bits": PQ_BITS,

        "nprobe": NPROBE,

        "training_vectors": len(sample_ids),

        "metric": "inner_product",

        "normalized_embeddings": True,

        "index_size_mb": round(
            INDEX_FILE.stat().st_size / 1000000,
            2
        ),

        "metadata_size_mb": round(
            METADATA_FILE.stat().st_size / 1000000,
            2
        ),

        "elapsed_minutes": round(
            (time.time() - start_time) / 60,
            2
        ),

        "smoke_test": "PASSED"
    }

    (INDEX_DIR / "index_metadata.json").write_text(

        json.dumps(summary, indent=4),

        encoding="utf-8"

    )

    print("\nFAISS INDEX SUMMARY")

    print(json.dumps(summary, indent=4))

    print("\nFAISS VECTOR INDEX: SUCCESS")


if __name__ == "__main__":

    main()
