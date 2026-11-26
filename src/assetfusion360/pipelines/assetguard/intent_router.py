
"""
AssetFusion360 | Intelligent Intent Router

Validated LLM-based tool selection with
deterministic fallback.
"""

import json
import re
import requests


class AssetGuardIntentRouter:

    def __init__(self, account_id, api_token, model):

        self.account_id = account_id
        self.api_token = api_token
        self.model = model

    def route(self, question):

        prompt = """
You are an intent classification agent for an
enterprise asset maintenance intelligence system.

Select exactly one supported tool:

1. asset_assessment
2. maintenance_history
3. top_risk_assets
4. portfolio_summary
5. similar_workorders
6. unsupported_question

Return ONLY a JSON object with these fields:

{
    "intent": "selected_tool",
    "asset_id": null,
    "asset_code": null,
    "limit": 10,
    "search_text": null
}

Rules:
- Extract asset identifiers only when supplied.
- Never invent an asset ID or asset code.
- Select top_risk_assets for prioritization questions.
- Select portfolio_summary for overall risk analysis.
- Select asset_assessment for asset risk, explanations
  and maintenance recommendations.
- Select maintenance_history for historical work orders.
- Select similar_workorders for similar historical cases.
- If the question cannot be supported, select
  unsupported_question.

IMPORTANT MAINTENANCE SEARCH RULES:

- A standalone maintenance issue is a valid search query.
- Users do not need to say "find similar workorders".
- Route short descriptions of maintenance problems
  to similar_workorders.
- Preserve the original maintenance issue in search_text.

Examples:

"Leaking water pipe"
Intent: similar_workorders
Search text: Leaking water pipe

"Broken playground equipment"
Intent: similar_workorders
Search text: Broken playground equipment

"Toilet unable to flush"
Intent: similar_workorders
Search text: Toilet unable to flush

"Electrical power failure"
Intent: similar_workorders
Search text: Electrical power failure

"Which five assets should technicians prioritize?"
Intent: top_risk_assets
Limit: 5

"What is the capital of France?"
Intent: unsupported_question


- Return JSON only.
"""

        url = (
            "https://api.cloudflare.com/client/v4/"
            f"accounts/{self.account_id}/ai/run/{self.model}"
        )

        response = requests.post(
            url,
            headers={
                "Authorization": f"Bearer {self.api_token}"
            },
            json={
                "messages": [
                    {
                        "role": "system",
                        "content": prompt
                    },
                    {
                        "role": "user",
                        "content": question
                    }
                ],
                "temperature": 0,
                "max_tokens": 200
            },
            timeout=30
        )

        response.raise_for_status()

        payload = response.json()

        if not payload.get("success"):
            raise ValueError("Intent inference failed.")


        output = payload["result"]["response"]

        if isinstance(output, dict):

            plan = output

        elif isinstance(output, str):

            output = output.strip()

            # Extract JSON from the generated response.

            start_json = output.find("{")
            end_json = output.rfind("}")

            if start_json == -1 or end_json < start_json:

                raise ValueError(
                    "No JSON object found in Llama response: "
                    + repr(output[:500])
                )

            json_text = output[
                start_json:end_json + 1
            ]

            try:

                plan = json.loads(json_text)

            except json.JSONDecodeError as error:

                raise ValueError(
                    "Invalid JSON returned by Llama: "
                    + repr(json_text[:500])
                ) from error

        else:

            raise ValueError(
                "Unexpected Llama response type."
            )

        allowed = {
            "asset_assessment",
            "maintenance_history",
            "top_risk_assets",
            "portfolio_summary",
            "similar_workorders",
            "unsupported_question"
        }

        if not isinstance(plan, dict):
            raise ValueError("Invalid tool plan.")

        if plan.get("intent") not in allowed:
            raise ValueError("Unsupported tool.")

        limit = plan.get("limit") or 10

        plan["limit"] = min(max(int(limit), 1), 20)

        return plan


    def normalize(self, question, plan):

        intent = plan["intent"]

        asset_id = plan.get("asset_id")
        asset_code = plan.get("asset_code")

        if intent in {
            "asset_assessment",
            "maintenance_history"
        }:

            if asset_id is not None:

                asset_id = str(asset_id)

                if not asset_id.isdigit():
                    return None

                if not re.search(
                    rf"\\b{re.escape(asset_id)}\\b",
                    question
                ):
                    return None

                reference = f"asset {asset_id}"

            elif isinstance(asset_code, str):

                if not re.fullmatch(
                    r"[A-Za-z0-9]+(?:-[A-Za-z0-9]+){2,}",
                    asset_code
                ):
                    return None

                if asset_code.lower() not in question.lower():
                    return None

                reference = f"asset {asset_code}"

            else:

                return None

            if intent == "maintenance_history":

                return f"Show maintenance history for {reference}"

            return f"Explain risk drivers for {reference}"

        if intent == "top_risk_assets":

            return f"Show top {plan['limit']} high risk assets"

        if intent == "portfolio_summary":

            return "Show portfolio summary"

        if intent == "similar_workorders":

            search_text = plan.get("search_text")

            if (
                isinstance(search_text, str)
                and search_text.strip()
                and search_text.lower().strip()
                    in question.lower()
            ):

                return (
                    "Find similar workorders for "
                    + search_text[:200]
                )

        return None
