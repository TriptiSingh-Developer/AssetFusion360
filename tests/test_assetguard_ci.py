
import json
import hashlib
import unittest
from pathlib import Path
from unittest.mock import patch

import faiss

from assetfusion360.pipelines.assetguard.maintenance_agent import (
    AssetGuardAgent
)

from assetfusion360.pipelines.assetguard.intent_router import (
    AssetGuardIntentRouter
)

from assetfusion360.pipelines.assetguard.llama_agent import (
    AssetGuardLlamaAgent
)


ROOT = Path(__file__).resolve().parents[1]

INDEX_DIR = ROOT / "data/09_semantic_search_index"


class TestAssetGuard(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.agent = AssetGuardAgent()

    @classmethod
    def tearDownClass(cls):
        cls.agent.close()

    def test_asset_risk(self):

        result = self.agent.get_asset_risk(14167)

        self.assertFalse(result.empty)

        self.assertEqual(
            result.iloc[0]["risk_category"],
            "High"
        )

    def test_asset_code_resolution(self):

        result = self.agent.run(
            "Why is M081-ZN02-BLG1602 high risk?"
        )

        self.assertEqual(
            result["intent"],
            "asset_assessment"
        )

        self.assertEqual(
            int(result["risk"].iloc[0]["asset_identity"]),
            14167
        )

    def test_invalid_asset(self):

        result = self.agent.run(
            "Why is asset INVALID-TEST-CODE high risk?"
        )

        self.assertEqual(
            result["intent"],
            "asset_not_found"
        )

    def test_shap_explanations(self):

        result = self.agent.explain_asset(14167)

        self.assertFalse(result.empty)

        self.assertIn(
            "shap_value",
            result.columns
        )

    def test_portfolio_summary(self):

        result = self.agent.get_portfolio_summary()

        self.assertFalse(result.empty)

    def test_top_risk_assets(self):

        result = self.agent.get_top_risk_assets(5)

        self.assertEqual(len(result), 5)

    def test_maintenance_history(self):

        result = self.agent.get_maintenance_history(
            14167,
            limit=5
        )

        self.assertFalse(result.empty)

        self.assertLessEqual(len(result), 5)

    def test_keyword_retrieval(self):

        result = self.agent.find_similar_workorders(
            "leaking pipe",
            limit=5
        )

        self.assertFalse(result.empty)

    def test_natural_language_maintenance(self):

        result = self.agent.run(
            "Broken playground equipment"
        )

        self.assertEqual(
            result["intent"],
            "similar_workorders"
        )

    def test_intelligent_router(self):

        router = AssetGuardIntentRouter(
            account_id="test",
            api_token="test",
            model="test"
        )

        plan = {
            "intent": "top_risk_assets",
            "limit": 5
        }

        result = router.normalize(
            "Which five assets should technicians prioritize?",
            plan
        )

        self.assertEqual(
            result,
            "Show top 5 high risk assets"
        )


class TestVectorArtifacts(unittest.TestCase):

    def test_index_integrity(self):

        index = faiss.read_index(
            str(INDEX_DIR / "maintenance_vectors.faiss")
        )

        self.assertTrue(index.is_trained)

        self.assertEqual(index.ntotal, 849667)

        self.assertEqual(index.d, 384)

    def test_metadata_integrity(self):

        config = json.loads(
            (INDEX_DIR / "index_metadata.json").read_text()
        )

        actual_hash = hashlib.sha256(
            (INDEX_DIR / "maintenance_metadata.parquet")
            .read_bytes()
        ).hexdigest()

        self.assertEqual(
            actual_hash,
            config["metadata_hash"]
        )


class TestLLMFallback(unittest.TestCase):

    def test_llm_failure_fallback(self):

        agent = AssetGuardLlamaAgent(
            account_id="test",
            api_token="test"
        )

        try:

            with patch.object(
                agent,
                "generate",
                side_effect=RuntimeError("Simulated API failure")
            ):

                result = agent.run(
                    "Why is asset 14167 high risk?"
                )

            self.assertEqual(
                result["llm_status"],
                "fallback"
            )

            self.assertIn("risk", result)

        finally:

            agent.close()


if __name__ == "__main__":
    unittest.main()
