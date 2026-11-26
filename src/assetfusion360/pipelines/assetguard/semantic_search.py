
'''
AssetFusion360 | Semantic Maintenance Search

MiniLM embeddings + FAISS IVF-PQ retrieval
with source work-order traceability.
'''

from pathlib import Path
import json
import time
import hashlib

import faiss
import numpy as np
import pandas as pd
import torch

from sentence_transformers import SentenceTransformer


ROOT = Path(__file__).resolve().parents[4]

INDEX_DIR = ROOT / "data" / "09_semantic_search_index"

INDEX_FILE = INDEX_DIR / "maintenance_vectors.faiss"

METADATA_FILE = INDEX_DIR / "maintenance_metadata.parquet"

CONFIG_FILE = INDEX_DIR / "index_metadata.json"


class AssetGuardSemanticSearch:

    def __init__(self):

        torch.set_num_threads(2)

        faiss.omp_set_num_threads(2)

        self.config = json.loads(
            CONFIG_FILE.read_text(encoding="utf-8")
        )

        metadata_hash = hashlib.sha256(
            METADATA_FILE.read_bytes()
        ).hexdigest()

        if metadata_hash != self.config["metadata_hash"]:

            raise RuntimeError(
                "Vector index and metadata do not match."
            )

        self.index = faiss.read_index(
            str(INDEX_FILE)
        )

        self.index.nprobe = self.config["nprobe"]

        self.metadata = pd.read_parquet(
            METADATA_FILE,
            columns=[
                "vector_id",
                "workorder_code",
                "resolved_asset_identity",
                "workorder_description",
                "occurrence_count",
                "created_at"
            ]
        ).sort_values(
            "vector_id"
        ).reset_index(drop=True)

        assert self.index.ntotal == len(
            self.metadata
        )

        assert np.array_equal(
            self.metadata["vector_id"].to_numpy(),
            np.arange(len(self.metadata))
        )

        self.model = SentenceTransformer(
            self.config["embedding_model"],
            device="cpu"
        )

    def search(self, question, limit=10):

        question = question.strip()

        if not question:

            return pd.DataFrame()

        limit = min(max(int(limit), 1), 50)

        question = question[:500]

        query_vector = self.model.encode(
            [question],
            normalize_embeddings=True,
            convert_to_numpy=True,
            show_progress_bar=False
        )

        query_vector = np.ascontiguousarray(
            query_vector,
            dtype=np.float32
        )

        scores, ids = self.index.search(
            query_vector,
            limit
        )

        valid = (
            (ids[0] >= 0)
            & (ids[0] < len(self.metadata))
        )

        vector_ids = ids[0][valid]

        result = self.metadata.iloc[
            vector_ids
        ].copy()

        result["semantic_similarity"] = (
            scores[0][valid].round(4)
        )

        result.insert(
            0,
            "semantic_rank",
            range(1, len(result) + 1)
        )

        return result.reset_index(drop=True)


def main():

    print("=" * 75)
    print("ASSETFUSION360 | SEMANTIC MAINTENANCE SEARCH")
    print("=" * 75)

    start = time.perf_counter()

    engine = AssetGuardSemanticSearch()

    print(
        "ENGINE LOAD TIME:",
        round(time.perf_counter() - start, 2),
        "seconds"
    )

    questions = [

        "Leaking water pipe",

        "Air conditioning system not cooling",

        "Electrical power supply failure",

        "Damaged children's playground equipment",

        "Toilet unable to flush",

        "Broken door locking mechanism"

    ]

    for question in questions:

        start = time.perf_counter()

        result = engine.search(
            question,
            limit=5
        )

        elapsed = time.perf_counter() - start

        print("\n" + "=" * 75)

        print("QUERY:", question)

        print(
            "SEARCH TIME:",
            round(elapsed, 3),
            "seconds"
        )

        print(
            result[
                [
                    "semantic_rank",
                    "workorder_code",
                    "workorder_description",
                    "semantic_similarity",
                    "occurrence_count"
                ]
            ].to_string(index=False)
        )

    print("\nSEMANTIC SEARCH ENGINE: SUCCESS")


if __name__ == "__main__":

    main()
