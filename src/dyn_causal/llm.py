from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Any, List
import os, json

@dataclass
class LLMResponse:
    text: str
    tokens_in: int = 0
    tokens_out: int = 0

class LLMClient:
    def __init__(self, model: str = "gemini-2.5-flash-lite"):
        self.model_name = model
        self._genai = None
        self._model = None

    def _ensure_client(self):
        import google.generativeai as genai
        api_key = os.environ.get("GEMINI_API_KEY")
        if not api_key:
            raise RuntimeError("GEMINI_API_KEY missing in environment")
        genai.configure(api_key=api_key)
        self._genai = genai
        self._model = genai.GenerativeModel(self.model_name)

    def complete(self, prompt: str) -> LLMResponse:
        if self._model is None:
            self._ensure_client()
        r = self._model.generate_content(prompt)
        return LLMResponse(text=r.text or "")

EXPERT_ROLES = {
    "temporal": "Expert in temporal precedence and lag reasonableness in financial events.",
    "discourse": "Expert in entity/discourse linking for financial text.",
    "precondition": "Expert in financial preconditions and enabling constraints.",
    "commonsense": "Expert in pragmatic market-specific causal logic.",
}

JUDGE_ROLE = "You are the judge that determines if a CAUSAL edge exists and its POLARITY (+1 bullish or -1 bearish)."

def build_expert_prompt(role: str, summary_cause: str, summary_effect: str, metadata: Dict[str, Any]) -> str:
    return f"""
You are the {role}
Decide if CAUSE (A) could causally influence EFFECT (B).
Respond as JSON:{{
  "vote": 0 or 1,
  "polarity": -1 or 1 or 0,
  "confidence": 0..1,
  "rationale": "one sentence"
}}
A: {summary_cause}
B: {summary_effect}
Metadata: {json.dumps(metadata)}
Output ONLY the JSON.
""".strip()

def build_judge_prompt(expert_jsons: List[Dict[str, Any]], summary_cause: str, summary_effect: str) -> str:
    return f"""
{JUDGE_ROLE}
Return JSON:{{
  "edge": 0 or 1,
  "polarity": -1 or 1 or 0,
  "confidence": 0..1,
  "rationale": "short reason"
}}
Experts: {json.dumps(expert_jsons)}
A: {summary_cause}
B: {summary_effect}
Output ONLY the JSON.
""".strip()

class DebateOrchestrator:
    def __init__(self, client: LLMClient, max_rounds: int = 1):
        self.client = client
        self.max_rounds = max_rounds

    def run_debate(self, cause: Dict[str, Any], effect: Dict[str, Any], metadata: Dict[str, Any], rounds: int = None) -> Dict[str, Any]:
        rounds = rounds or self.max_rounds
        expert_outputs: List[Dict[str, Any]] = []
        for _ in range(rounds):
            expert_outputs = []
            for name, role in EXPERT_ROLES.items():
                prompt = build_expert_prompt(role, cause["summary"], effect["summary"], metadata)
                resp = self.client.complete(prompt)
                try:
                    j = json.loads(resp.text)
                except Exception:
                    j = {"vote": 0, "polarity": 0, "confidence": 0.0, "rationale": "parse_error"}
                j["role"] = name
                expert_outputs.append(j)
        jprompt = build_judge_prompt(expert_outputs, cause["summary"], effect["summary"])
        jresp = self.client.complete(jprompt)
        try:
            judge = json.loads(jresp.text)
        except Exception:
            judge = {"edge": 0, "polarity": 0, "confidence": 0.0, "rationale": "parse_error"}
        return {"experts": expert_outputs, "judge": judge}
