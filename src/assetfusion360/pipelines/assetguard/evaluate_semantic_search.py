
'''
AssetFusion360 | Retrieval Evaluation

Comparison of keyword-based and semantic
maintenance retrieval.
'''

from pathlib import Path
import time
import json

import pandas as pd

from .maintenance_agent import AssetGuardAgent
from .semantic_search import AssetGuardSemanticSearch


ROOT = Path(__file__).resolve().parents[4]

OUTPUT_DIR = (
    ROOT
    / "data"
    / "09_semantic_search_index"
    / "evaluation"
)

QUERIES = [

    "Leaking water pipe",

    "Air conditioning system not cooling",

    "Electrical power supply failure",

    "Damaged children's playground equipment",

    "Toilet unable to flush",

    "Broken door locking mechanism",

    "Water accumulating on walking surfaces",

    "Damaged outdoor seating",

    "Blocked drainage system",

    "Malfunctioning lighting fixtures"

]


def main():

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    print("=" * 75)
    print("ASSETFUSION360 | RETRIEVAL EVALUATION")
    print("=" * 75)

    agent = AssetGuardAgent()

    semantic = AssetGuardSemanticSearch()

    # Warm up both retrieval methods.

    agent.find_similar_workorders(
        "water pipe",
        limit=5
    )

    semantic.search(
        "water pipe",
        limit=5
    )

    comparisons = []

    detailed_results = []

    try:

        for query in QUERIES:

            print("\n" + "=" * 75)

            print("QUERY:", query)

            start = time.perf_counter()

            keyword_results = (
                agent.find_similar_workorders(
                    query,
                    limit=5
                )
            )

            keyword_time = (
                time.perf_counter() - start
            )

            start = time.perf_counter()

            semantic_results = semantic.search(
                query,
                limit=5
            )

            semantic_time = (
                time.perf_counter() - start
            )

            keyword_ids = set(
                keyword_results[
                    "workorder_code"
                ].astype(str)
            ) if not keyword_results.empty else set()

            semantic_ids = set(
                semantic_results[
                    "workorder_code"
                ].astype(str)
            ) if not semantic_results.empty else set()

            overlap = len(
                keyword_ids & semantic_ids
            )

            comparisons.append({

                "query": query,

                "keyword_results":
                    len(keyword_results),

                "semantic_results":
                    len(semantic_results),

                "keyword_latency_ms":
                    round(keyword_time * 1000, 2),

                "semantic_latency_ms":
                    round(semantic_time * 1000, 2),

                "overlapping_workorders":
                    overlap,

                "semantic_average_score":
                    float(
                        semantic_results[
                            "semantic_similarity"
                        ].mean()
                    ) if not semantic_results.empty else None

            })

            for method, frame in [

                ("keyword", keyword_results),

                ("semantic", semantic_results)

            ]:

                for rank, (_, row) in enumerate(
                    frame.iterrows(),
                    start=1
                ):

                    detailed_results.append({

                        "query": query,

                        "retrieval_method": method,

                        "rank": rank,

                        "workorder_code":
                            row["workorder_code"],

                        "workorder_description":
                            row["workorder_description"]

                    })

            print("\nKEYWORD SEARCH:")

            print(
                keyword_results[
                    ["workorder_description"]
                ].to_string(index=False)
            )

            print("\nSEMANTIC SEARCH:")

            print(
                semantic_results[
                    [
                        "workorder_description",
                        "semantic_similarity"
                    ]
                ].to_string(index=False)
            )

            print(
                "\nKeyword latency:",
                round(keyword_time * 1000, 2),
                "ms"
            )

            print(
                "Semantic latency:",
                round(semantic_time * 1000, 2),
                "ms"
            )

        comparison = pd.DataFrame(comparisons)

        details = pd.DataFrame(detailed_results)

        comparison.to_csv(

            OUTPUT_DIR / "retrieval_comparison.csv",

            index=False

        )

        details.to_csv(

            OUTPUT_DIR / "retrieval_details.csv",

            index=False

        )

        # Create a human relevance evaluation template.

        evaluation = details.sample(
            frac=1,
            random_state=42
        ).reset_index(drop=True)

        evaluation.insert(
            0,
            "evaluation_id",
            range(1, len(evaluation) + 1)
        )

        evaluation[
            [
                "evaluation_id",
                "query",
                "workorder_description"
            ]
        ].assign(
            relevance_score="",
            reviewer_notes=""
        ).to_csv(

            OUTPUT_DIR / "human_relevance_review.csv",

            index=False

        )

        evaluation[
            [
                "evaluation_id",
                "retrieval_method",
                "rank",
                "workorder_code"
            ]
        ].to_csv(

            OUTPUT_DIR / "evaluation_answer_key.csv",

            index=False

        )

        summary = {

            "status": "SUCCESS",

            "evaluated_queries": len(QUERIES),

            "top_k": 5,

            "mean_keyword_latency_ms": round(
                comparison[
                    "keyword_latency_ms"
                ].mean(),
                2
            ),

            "mean_semantic_latency_ms": round(
                comparison[
                    "semantic_latency_ms"
                ].mean(),
                2
            ),

            "total_keyword_results": int(
                comparison["keyword_results"].sum()
            ),

            "total_semantic_results": int(
                comparison["semantic_results"].sum()
            ),

            "mean_retrieval_overlap": round(
                comparison[
                    "overlapping_workorders"
                ].mean(),
                2
            ),

            "relevance_evaluation":
                "Pending human relevance assessment"

        }

        (OUTPUT_DIR / "retrieval_summary.json").write_text(

            json.dumps(summary, indent=4),

            encoding="utf-8"

        )

        print("\n" + "=" * 75)

        print("RETRIEVAL EVALUATION SUMMARY")

        print(json.dumps(summary, indent=4))

        print("\nRETRIEVAL EVALUATION: SUCCESS")

    finally:

        agent.close()


if __name__ == "__main__":

    main()
