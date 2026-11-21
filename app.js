// ==============================================================================
// emilianodesu.github.io — Flagship-Aware DAG Inspector & Project Filters
// ==============================================================================

document.addEventListener("DOMContentLoaded", () => {
  initFlagshipDAG();
  initProjectFilters();
});

const FLAGSHIP_DAGS = {
  hvfhv: {
    title: "NYC Mobility Data Reliability Platform (HVFHV)",
    subtitle: "41,474,517 OFFICIAL NYC TLC TRIPS · 229 PASSING TESTS · ZERO CLOUD OVERHEAD",
    status: "CI Passing · 229 Tests",
    stages: [
      {
        step: "Stage 01",
        name: "Immutable Ingest",
        tech: "HTTPS + SHA-256",
        title: "01. Content-Addressed Acquisition (SHA-256 + PyArrow)",
        badges: ["httpx", "SHA-256", "PyArrow 26.0", "SSRF Allowlist"],
        desc: "Downloads official monthly NYC TLC HVFHV Parquet releases with bounded retries and strict host allowlisting, verifying SHA-256 digests and PyArrow schema decoding before landing immutable raw bytes.",
        outcome: "Audited 2024-12 (21.07M rows) and 2025-01 (20.41M rows) = 41,474,517 official trip records with cryptographic lineage.",
        code: `# Immutable content-addressed landing & digest check
digest = sha256_file(temp_parquet_path)
dest = raw_root / "sha256" / f"{digest}.parquet"
verify_pyarrow_schema(temp_parquet_path, contract_version)
os.replace(temp_parquet_path, dest)  # atomic POSIX rename`
      },
      {
        step: "Stage 02",
        name: "Schema Adapter",
        tech: "24 → 25 Columns",
        title: "02. Cross-Year Schema Evolution (NYC Congestion Pricing)",
        badges: ["Data Contracts", "Zero Coercion", "S01–S08 Rules"],
        desc: "Bridges the 2025 NYC Congestion Pricing transition where `cbd_congestion_fee` appeared in 2025-01+ files. Tracks `cbd_fee_field_present` explicitly so pre-2025 NULL values are never silently coerced to $0.00.",
        outcome: "All 21,068,851 rows in 2024-12 retain explicit NULL (`cbd_fee_field_present = false`), while 6.30M trips in 2025-01 record positive congestion fees up to $3.00.",
        code: `-- Explicit schema evolution without silent coercion
SELECT
  request_datetime,
  pickup_datetime,
  CASE WHEN cbd_fee_field_present THEN cbd_congestion_fee ELSE NULL END AS cbd_fee,
  cbd_fee_field_present
FROM staged_hvfhv_trips;`
      },
      {
        step: "Stage 03",
        name: "DuckDB 2.0 Engine",
        tech: "Type 7 Quantiles",
        title: "03. Exact Continuous Quantiles & Semantic Honesty",
        badges: ["DuckDB 2.0", "64-bit Microseconds", "Q01–Q09 Gates"],
        desc: "Evaluates exact continuous Type 7 quantiles (`p50` and `p95`) via `quantile_cont` directly over all eligible non-negative intervals rather than averaging subgroup percentiles, enforcing `total = eligible + negative + missing`.",
        outcome: "Processed 41.02M eligible intervals in 32.63s incremental build time (p50 = 249.0s, p95 = 666.0s) with 0 spill files and Peak RSS 7.63 GB (under 8 GiB cap).",
        code: `-- Exact Type 7 quantiles on eligible population (never subgroup-averaged)
SELECT
  COUNT(*) AS eligible_trips,
  quantile_cont(interval_us / 1e6, 0.50) AS p50_seconds,
  quantile_cont(interval_us / 1e6, 0.95) AS p95_seconds
FROM curated_trips
WHERE interval_status = 'eligible_nonnegative';`
      },
      {
        step: "Stage 04",
        name: "SQLite CAS Control",
        tech: "Zero-WAL & Locks",
        title: "04. Atomic Publication & Option A Retention",
        badges: ["SQLite CAS", "fcntl.flock", "ADR 0006", "0 WAL"],
        desc: "Seals each DuckDB release cleanly (`CHECKPOINT`, zero WAL residual), hashes `analytics.duckdb` + `manifest.json`, and promotes atomically via SQLite Compare-and-Swap (`expected_parent`, `expected_generation`).",
        outcome: "Option A retention keeps footprint at 20.75 GiB (59.25 GiB headroom under 80 GiB cap) with audited rollback and POSIX writer/reader lock serialization.",
        code: `-- Compare-and-Swap (CAS) atomic release promotion
UPDATE active_pointer
SET release_id = :candidate_id, generation = generation + 1
WHERE pointer_name = 'current'
  AND release_id = :expected_parent
  AND generation = :expected_generation;`
      },
      {
        step: "Stage 05",
        name: "Dagster & Streamlit",
        tech: "Lineage & Console",
        title: "05. Orchestration & Out-of-Cache Read-Only Guard",
        badges: ["Dagster 1.13", "17/17 Checks", "Streamlit 1.67", "Read-Only"],
        desc: "Dagster software-defined assets manage schedules, lineage, and 17 gap-aware asset checks. The read-only Streamlit console verifies release eligibility against SQLite before and after every cached query.",
        outcome: "17/17 Dagster checks passing (`SYNCHRONIZED`), 229 automated unit/adversarial tests passing, and 1.923s cold headless consumer query latency.",
        code: `# Out-of-cache release eligibility guard before & after read
def query_active_release(sql: str):
    rel = control.assert_release_active(expected_release_id)
    rows = reader.execute_readonly(rel.duckdb_path, sql)
    control.assert_release_active(rel.release_id)  # reject mid-read retirement
    return rows`
      }
    ]
  },

  weather: {
    title: "MX Weather MOS — Forecast Verification & ML Platform",
    subtitle: "31/32 MEXICAN STATE CAPITALS · 71/71 CI/DOCKER TESTS · WCAG 2.1 AAA UI",
    status: "CI Passing · 71 Tests",
    stages: [
      {
        step: "Stage 01",
        name: "Resilient Ingest",
        tech: "Open-Meteo + CONAGUA",
        title: "01. Multi-Source Ingestion with Circuit-Breaker Degradation",
        badges: ["Python", "Polite Throttling", "Circuit Breaker", "Docker"],
        desc: "Ingests hourly NWP forecasts and station observations across Mexican state capitals with rate-limit backoff and graceful circuit-breaker degradation when upstream feeds throttle.",
        outcome: "Paired 31/32 state capitals (`n = 3,410` holdout observations) with 100% Docker Compose reproducibility.",
        code: `# Resilient upstream ingestion with backoff & circuit breaker
if circuit_breaker.is_open("conagua"):
    return fallback_to_verified_cache(station_id)
payload = http_client.get_with_retry(url, backoff_sec=2.0)
validate_observation_contract(payload)`
      },
      {
        step: "Stage 02",
        name: "PostGIS Store",
        tech: "PostgreSQL 16",
        title: "02. Spatial Indexing & Point-in-Time Revisioning",
        badges: ["PostgreSQL 16", "PostGIS", "Temporal Keys", "Spatial GIST"],
        desc: "Persists municipal geometries and forecast issue-time vs. valid-time revisions in PostgreSQL/PostGIS so model training strictly respects point-in-time availability with zero temporal leakage.",
        outcome: "Sub-10ms spatial joins and indexed forecast map queries (`p95 = 8.47 ms` on `/v1/forecasts/map`).",
        code: `-- Point-in-time forecast alignment (zero future leakage)
SELECT o.municipality_id, o.obs_date, o.tmax_c, f.nwp_tmax_c
FROM weather_observations o
JOIN LATERAL (
  SELECT nwp_tmax_c FROM nwp_forecasts
  WHERE municipality_id = o.municipality_id
    AND valid_date = o.obs_date AND issued_at < o.obs_date
  ORDER BY issued_at DESC LIMIT 1
) f ON true;`
      },
      {
        step: "Stage 03",
        name: "LightGBM MOS",
        tech: "Quantile Regression",
        title: "03. Leakage-Free Model Output Statistics (LightGBM)",
        badges: ["LightGBM", "Quantile MOS", "Temporal Split", "scikit-learn"],
        desc: "Trains quantile gradient-boosted trees on point-in-time meteorological predictors to correct systematic NWP temperature bias across diverse Mexican elevations and microclimates.",
        outcome: "Reduced maximum temperature RMSE by 30.7% and MAE by 16.8% versus raw numerical weather predictions on temporal holdout.",
        code: `# LightGBM quantile MOS bias correction
model = lgb.LGBMRegressor(
    objective="quantile", alpha=0.5,
    n_estimators=400, learning_rate=0.04, random_state=42
)
model.fit(X_train_pit, y_train_tmax)`
      },
      {
        step: "Stage 04",
        name: "Rain Calibration",
        tech: "Isotonic Regression",
        title: "04. Probabilistic Precipitation Calibration",
        badges: ["Isotonic Calibration", "Brier Score", "Probability Reliability"],
        desc: "Calibrates raw NWP precipitation probabilities using out-of-fold Isotonic Regression so predicted rain probabilities match empirical observed frequencies.",
        outcome: "Cut rain Brier score by 77.2% compared to raw NWP probabilities across the 31-capital holdout.",
        code: `# Probabilistic rain calibration via Isotonic Regression
calibrator = IsotonicRegression(y_min=0.0, y_max=1.0, out_of_bounds="clip")
calibrator.fit(raw_rain_prob_val, observed_rain_binary_val)
calibrated_prob = calibrator.predict(raw_rain_prob_test)`
      },
      {
        step: "Stage 05",
        name: "FastAPI & React",
        tech: "API + Leaflet Map",
        title: "05. Low-Latency API Serving & Accessible Geospatial UI",
        badges: ["FastAPI", "React + TypeScript", "Leaflet", "WCAG 2.1 AAA"],
        desc: "Exposes calibrated forecasts, skill metrics, and municipal alerts via FastAPI (`p95 = 7.78–8.47 ms`) consumed by a WCAG 2.1 AAA React + Leaflet interactive web application.",
        outcome: "71/71 automated tests passing (67 backend/Docker E2E pytest + 4 Vitest frontend tests) and live GitHub Pages deployment.",
        code: `@router.get("/v1/ml/skill", response_model=SkillMetricsResponse)
def get_model_skill(db: Session = Depends(get_db)):
    return query_verified_holdout_metrics(db)  # p95 = 7.78 ms`
      }
    ]
  },

  spark: {
    title: "Spark Graph Ranking — Catalyst/Tungsten Engine",
    subtitle: "68,475,391 DIRECTED EDGES · 4.34x–8.37x SPEEDUP · 0 B DISK SPILL",
    status: "CI Passing · 7 Tests",
    stages: [
      {
        step: "Stage 01",
        name: "SNAP Ingest",
        tech: "Streaming SHA-256",
        title: "01. Deterministic Public Graph Acquisition",
        badges: ["Stanford SNAP", "SHA-256", "CLI Reproducible"],
        desc: "Fetches official Stanford SNAP graph archives (`cit-HepPh`, `web-Google`, `soc-LiveJournal1`) atomically with streaming SHA-256 verification and zero raw datasets committed to git.",
        outcome: "Verified digests across 3 real-world graph topologies scaling from 421K to 68.99M raw edges.",
        code: `# One-command deterministic acquisition & Parquet canonicalization
spark-graph-ranking ingest --dataset livejournal --partitions 64
# SHA-256 verified: d7bcd5a87b88c896c35fdb9611e804c3...`
      },
      {
        step: "Stage 02",
        name: "Canonicalizer",
        tech: "INT Parquet Graph",
        title: "02. Compact INT Representation & Graph Audit",
        badges: ["PyArrow", "Parquet", "Self-Loop Removal", "Dangling Audit"],
        desc: "Casts node IDs to compact 32-bit integers, strips null rows and self-loops, deduplicates edges, and explicitly counts dangling nodes (out-degree 0) into `graph_audit.json`.",
        outcome: "On `soc-LiveJournal1`: canonicalized 4,846,609 nodes and 68,475,391 edges, removing 518,382 self-loops and tracking 552,277 dangling nodes.",
        code: `edges = raw_edges.filter(F.col("src") != F.col("dst")).dropDuplicates(["src", "dst"])
dangling = nodes.join(out_degree, nodes.id == out_degree.src, "left_anti")
edges.write.mode("overwrite").parquet("edges.parquet")`
      },
      {
        step: "Stage 03",
        name: "Catalyst / Tungsten",
        tech: "Compact Adj-Lists",
        title: "03. Pure-JVM DataFrame Hot Loop (Zero Python UDFs)",
        badges: ["Apache Spark 3.5", "Tungsten", "KryoSerializer", "collect_list"],
        desc: "Replaces naive Python RDD edge joins (which pay JVM-to-Python `pickle` serialization every iteration) with native JVM Tungsten adjacency lists (`collect_list` + `explode`) and exact dangling-mass redistribution.",
        outcome: "4.34x speedup on `cit-HepPh` PageRank, 8.37x on `cit-HepPh` HITS, and 5.76x on `web-Google` (5.1M edges) with ~1 GB lower Peak RSS.",
        code: `incoming = (
    adj.join(ranks, adj.src == ranks.id)
       .select(F.explode(adj.dsts).alias("id"), (ranks.rank * adj.inv_deg).alias("contrib"))
       .groupBy("id").agg(F.sum("contrib").alias("in_rank"))
)`
      },
      {
        step: "Stage 04",
        name: "O(1) Lineage",
        tech: "localCheckpoint",
        title: "04. Eliminating Exponential Catalyst Plan Blowup",
        badges: ["localCheckpoint", "AQE=false", "0 B Disk Spill"],
        desc: "Truncates iterative Catalyst `LogicalPlan` DAGs every 1–2 iterations via `localCheckpoint(eager=True)` with `spark.sql.adaptive.enabled=false`, preventing driver heap OOM in chained HITS joins.",
        outcome: "Converged `soc-LiveJournal1` (68.47M edges) in 36 iterations (8.5 min on Apple M4) with 0 B disk spill, 0 B memory spill, and 11.12 GB Peak RSS.",
        code: `if iteration % checkpoint_every == 0:
    nxt = nxt.localCheckpoint(eager=True)  # O(1) Catalyst LogicalPlan depth
else:
    nxt = nxt.persist(StorageLevel.MEMORY_AND_DISK)`
      },
      {
        step: "Stage 05",
        name: "Spark Telemetry",
        tech: "REST UI + NetworkX",
        title: "05. Honest Stage Telemetry & Mathematical Verification",
        badges: ["Spark REST API", "NetworkX", "L1 <= 1.43e-16", "Top-20 Exact"],
        desc: "Queries Spark's local REST UI API for exact per-run `shuffleReadBytes`, `shuffleWriteBytes`, and `diskBytesSpilled`, transparently reporting both wall-time speedups and Spark 3.5 `localCheckpoint()` shuffle trade-offs.",
        outcome: "Machine-precision agreement (`L1 = 1.29e-16` on HepPh, `1.43e-16` on web-Google), exact 20/20 Top-20 match, and 7/7 passing CI tests vs. NetworkX.",
        code: `stages = requests.get(f"{ui_url}/api/v1/applications/{app_id}/stages").json()
shuffle_write = sum(int(s["shuffleWriteBytes"]) for s in stages)
assert l1_diff(df_scores, rdd_scores) < 1e-7`
      }
    ]
  }
};

function initFlagshipDAG() {
  const switchBtns = Array.from(document.querySelectorAll(".dag-switch-btn"));
  const nodesContainer = document.getElementById("dag-nodes-container");
  const projTitle = document.getElementById("dag-project-title");
  const projSubtitle = document.getElementById("dag-project-subtitle");
  const statusPill = document.getElementById("dag-status-pill");

  const inspTitle = document.getElementById("inspector-title");
  const inspBadges = document.getElementById("inspector-badges");
  const inspDesc = document.getElementById("inspector-desc");
  const inspExp = document.getElementById("inspector-exp");
  const inspCode = document.getElementById("inspector-code");

  if (!nodesContainer || !switchBtns.length) return;

  function renderStage(flagshipKey, stageIdx) {
    const stage = FLAGSHIP_DAGS[flagshipKey].stages[stageIdx];
    const nodeBtns = Array.from(nodesContainer.querySelectorAll(".dag-node"));
    nodeBtns.forEach((b, i) => b.setAttribute("aria-pressed", i === stageIdx ? "true" : "false"));

    inspTitle.textContent = stage.title;
    inspDesc.textContent = stage.desc;
    inspExp.textContent = stage.outcome;
    inspCode.textContent = stage.code;

    inspBadges.innerHTML = "";
    stage.badges.forEach((text) => {
      const span = document.createElement("span");
      span.className = "badge";
      span.textContent = text;
      inspBadges.appendChild(span);
    });
  }

  function renderFlagship(flagshipKey) {
    const fg = FLAGSHIP_DAGS[flagshipKey];
    if (!fg) return;

    projTitle.textContent = fg.title;
    projSubtitle.textContent = fg.subtitle;
    statusPill.textContent = fg.status;

    nodesContainer.innerHTML = "";
    fg.stages.forEach((st, idx) => {
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = "dag-node";
      btn.setAttribute("aria-pressed", idx === 0 ? "true" : "false");
      btn.innerHTML = `
        <div class="dag-node-step">${st.step}</div>
        <div class="dag-node-name">${st.name}</div>
        <div class="dag-node-tech">${st.tech}</div>
      `;
      btn.addEventListener("click", () => renderStage(flagshipKey, idx));
      nodesContainer.appendChild(btn);
    });

    renderStage(flagshipKey, 0);
  }

  switchBtns.forEach((btn) => {
    btn.addEventListener("click", () => {
      switchBtns.forEach((b) => b.setAttribute("aria-selected", "false"));
      btn.setAttribute("aria-selected", "true");
      renderFlagship(btn.dataset.flagship);
    });
  });

  renderFlagship("hvfhv");
}

function initProjectFilters() {
  const filterBtns = Array.from(document.querySelectorAll(".filter-btn"));
  const cards = Array.from(document.querySelectorAll(".project-card"));

  filterBtns.forEach((btn) => {
    btn.addEventListener("click", () => {
      filterBtns.forEach((b) => b.setAttribute("aria-pressed", "false"));
      btn.setAttribute("aria-pressed", "true");

      const filter = btn.dataset.filter;
      cards.forEach((card) => {
        const categories = (card.dataset.category || "").split(/\s+/);
        card.style.display = filter === "all" || categories.includes(filter) ? "flex" : "none";
      });
    });
  });
}
