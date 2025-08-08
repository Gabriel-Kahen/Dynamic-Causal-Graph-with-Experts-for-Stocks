# Dynamic Causal Graph (Stocks)

A small, modular Python project that builds a **dynamic causal graph** of market events and uses a **multi-expert LLM “debate + judge”** to add/update edges. It ingests **5-minute price bars**, **news RSS**, and **Reddit activity**, then emits **alerts** when the graph predicts a meaningful move. Inspired by and aligned with the causal-graph-with-experts approach from **Koupaee et al., 2025**. ([arXiv][1])

---

## How it works (short)

1. **Ingest events**:

   * Price bars (Alpaca IEX) → derive price events on return/volume spikes
   * News (RSS) → ticker-matched headlines
   * Reddit → rolling 120-min buffer; only matured (scored) posts count; viral overrides; curated links

2. **Gate pairs**: plausible (time-lag, same ticker/sector/peers, macro→ticker, discourse mentions).

3. **Debate + judge**: four experts vote, a judge decides **edge? polarity? confidence?** (token-efficient prompts).

4. **Graph update**: confidence-weighted, polarity-aware edges with decay + pruning.

5. **Inference & alerts**: aggregate incoming edges → per-ticker probability & expected σ → alert if over thresholds.

6. **Market hours**: holiday-aware **RTH-only** debates/alerts by default (NYSE calendar); news/Reddit still buffer 24/7.

---

## Quick start

```bash
# Python 3.10+
pip install google-generativeai alpaca-py praw vaderSentiment feedparser \
            pandas python-dotenv pyvis networkx pyyaml pandas-market-calendars

cp .env.example .env   # fill in keys (Gemini, Reddit, Alpaca)

# Sanity demo (no market keys needed)
PYTHONPATH=src python3 runner.py
PYTHONPATH=src python3 view_graph_pyvis.py --path data/latest_graph.json --out graph.html
open graph.html

# Realtime loop (5-min cadence; holiday-aware RTH inference)
PYTHONPATH=src python3 realtime.py
# or once:
PYTHONPATH=src python3 realtime.py --once
```

---

## Minimal config (edit `config.yaml`)

* **Universe & metadata**: tickers, sector/peers, per-ticker RSS keywords
* **News feeds**: list of RSS URLs (Reuters/CNBC/FT/AP/WSJ/Yahoo, etc.)
* **Reddit**: subs, include links, score maturity, viral threshold
* **RTH**:

  ```yaml
  rth:
    enforce: true
    require_price_event: true
  ```

Secrets live in `.env` (keep it out of git). Use the provided `.env.example`.

---

## Outputs

* `data/latest_graph.json` — graph snapshot for your frontend
* `graph.html` — quick visualization (PyVis)
* `data/alerts.jsonl` — alerts with probability/σ/rationale
* `data/events.sqlite` — append-only audit trail

---

## Paper citation

* **Koupaee, Mahnaz; Bai, Xueying; Chen, Mudan; Durrett, Greg; Chambers, Nathanael; Balasubramanian, Niranjan.** *Causal Graph based Event Reasoning using Semantic Relation Experts.* arXiv:2506.06910 (2025). DOI: 10.48550/arXiv.2506.06910. ([arXiv][1])

**BibTeX**

```bibtex
@article{koupaee2025causal,
  title   = {Causal Graph based Event Reasoning using Semantic Relation Experts},
  author  = {Koupaee, Mahnaz and Bai, Xueying and Chen, Mudan and Durrett, Greg and Chambers, Nathanael and Balasubramanian, Niranjan},
  journal = {arXiv preprint arXiv:2506.06910},
  year    = {2025},
  doi     = {10.48550/arXiv.2506.06910}
}
```

If you want this dropped straight into `README.md`, say the word and I’ll paste a ready-to-commit file.

[1]: https://arxiv.org/abs/2506.06910 "[2506.06910] Causal Graph based Event Reasoning using Semantic Relation Experts"