# Dynamic Causal Graph w/ Experts (Stocks)

A modular Python project that builds a **dynamic causal graph** of market events and uses a **multi-expert LLM “debate + judge”** to add/update edges. It ingests **5-minute price bars**, **news RSS**, **Reddit activity**, and **macroeconomic data**, then emits **alerts** when the graph predicts a meaningful move. Inspired by and aligned with the causal-graph-with-experts approach from **Koupaee et al., 2025**. ([arXiv][1])

---

## How it works

1. **Ingest events (streaming)**

   * **Price bars (Alpaca IEX)** → emit a `price_event` when a 5-min bar breaches return σ or volume percentile thresholds.
   * **News (RSS)** → emit a `news` node when a headline matches a ticker’s `rss_keywords`.
   * **Reddit (social)** → maintain a 120-min rolling buffer; only *matured* posts (aged `score_maturity_minutes` and ≥ `min_score`) count. If raw 5-min mentions explode (≥ `viral_5m_threshold`), emit immediately. Curated post links are attached to each social node.

   *Each event is a node with: `id`, `type`, `ticker`, `ts`, `summary` (short), and `attrs` (e.g., sigma, volume spike, links, sentiment).*

2. **Gate plausible pairs (A → B)**

   * **Time:** A must precede B within a type-specific lag (e.g., `max_bar_lag_minutes` for price effects).
   * **Entity:** same ticker, or cross-ticker only if allowed by sector/peers/macro rules.
   * **De-noise:** cap candidates per new node; require minimal evidence for social (e.g., `mentions_5m ≥ 3`).
   * This keeps the LLM call count small and sane.

3. **Multi-expert “debate” (per paper)**
   For each candidate pair *(cause A → effect B)* we run **four specialists** — short, token-capped prompts — each returns JSON:

   ```json
   { "vote": 0|1, "polarity": -1|0|1, "confidence": 0..1, "rationale": "one sentence" }
   ```

   * **Temporal expert**: checks precedence and whether the A→B lag is *reasonable* (e.g., news → same-morning price).

   * **Discourse/entity expert**: validates that A really refers to B’s entity (ticker/company); resolves aliases, avoids ticker collisions.

   * **Precondition/enabler expert**: looks for enabling constraints (e.g., “supplier beat” → peer demand; “regulatory approval” → product launch).

   * **Market-commonsense expert**: applies finance pragmatics (earnings beats, guidance, macro prints) to estimate the **sign** (bullish/bearish) and flag confounders.

   > **Rounds:** default **1** round. If the judge (below) lands in a configurable gray band, we *optionally* escalate +1 round. This is the main knob for cost/quality.

4. **Judge (aggregation)**
   The **judge** sees only the experts’ JSON + the pair summaries and returns:

   ```json
   { "edge": 0|1, "polarity": -1|0|1, "confidence": 0..1, "rationale": "short reason" }
   ```

   * Accepts an edge when there’s **temporal validity** and a **coherent majority** on existence/polarity; otherwise rejects or lowers confidence.
   * We apply repo knobs before touching the graph:

     * `weights.min_confidence_to_add` — don’t add edges below this.
     * `weights.alpha_blend` — edge weight = `alpha * judge.confidence + (1-alpha) * initial_edge_weight`.

5. **Graph update (NetworkX)**

   * Add/update directed edge **A → B** with **polarity** and **weight**.
   * **Decay** edges by type (half-life per `decay.*`), prune when weak.
   * Keep a full audit (`events.sqlite`) of `add_node`, `add_edge`, and decisions.

6. **Inference & alerts**

   * When a **new price\_event** lands, aggregate its **incoming active edges** (signed, decayed weights) into a per-ticker belief for the configured **horizon**.
   * Convert to probability & expected σ; if both exceed thresholds (`horizon.min_probability`, `horizon.spread_sigma_k`), emit an **alert** (JSONL) with direction and a one-line LLM rationale.

7. **Market hours control**

   * By default, **debates/edges/alerts** only run during **RTH** (NYSE calendar: holidays & early closes respected).
   * News/Reddit still ingest overnight; they’re **considered at the open** when the first price bars arrive.
   * Toggle via:

     ```yaml
     rth:
       enforce: true
       require_price_event: true  # debatе only when the new event is a price_event
     ```

*Notes:* prompts are intentionally short to control token use; gating + RTH keep QPM/QPD low. A budget cap (optional) is configurable and will hard-stop daily spend by limiting evaluated edges.


---

## Quick start

```bash
# Python 3.10+
pip install google-generativeai alpaca-py praw vaderSentiment feedparser \
            pandas python-dotenv pyvis networkx pyyaml pandas-market-calendars

cp .env.example .env   # fill in keys (Gemini, Reddit, Alpaca)x

# Realtime loop (5-min cadence; holiday-aware RTH inference)
PYTHONPATH=src python3 realtime.py
# or just one cycle:
PYTHONPATH=src python3 realtime.py --once
```

---

## Config (edit `config.yaml`)

* **Universe & metadata**: tickers, sector/peers, per-ticker RSS keywords
* **News feeds**: list of RSS URLs (Reuters/CNBC/FT/AP/WSJ/Yahoo, etc.)
* **Reddit**: subs, include links, score maturity, viral threshold
* **RTH**: as explained above


---

## Outputs

* `data/latest_graph.json` — graph snapshot
* `graph.html` — quick visualization of graph (PyVis)
* `data/alerts.jsonl` — alerts with probability/σ/rationale
* `data/events.sqlite` — append-only audit trail

---

## Paper citation

* **Koupaee, Mahnaz; Bai, Xueying; Chen, Mudan; Durrett, Greg; Chambers, Nathanael; Balasubramanian, Niranjan.** *Causal Graph based Event Reasoning using Semantic Relation Experts.* arXiv:2506.06910 (2025). DOI: 10.48550/arXiv.2506.06910. ([arXiv][1])


[1]: https://arxiv.org/abs/2506.06910 "[2506.06910] Causal Graph based Event Reasoning using Semantic Relation Experts"
