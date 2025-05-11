in-sight-ai-101
==============================

Learn machine learning fundamentals by detecting anomalies in financial data using Python and PySpark.


├── jobs/
│   ├── __init__.py
│   ├── job1.py              # Entry point for Job 1
│   ├── job2.py              # Entry point for Job 2
│   └── utils.py             # Common job-level helpers
│
├── src/
│   └── my_project/          # Core business logic
│       ├── __init__.py
│       ├── ingest/
│       │   └── read_source.py
│       ├── transform/
│       │   ├── clean_data.py
│       │   └── enrich_data.py
│       ├── write/
│       │   └── write_to_target.py
│       └── common/
│           └── spark_session.py
│
├── tests/
│   ├── __init__.py
│   ├── test_transform.py
│   ├── test_read_source.py
│   └── conftest.py          # Optional: Pytest fixtures, Spark session mocking
│
└── notebooks/
    └── exploration.ipynb    # For data exploration, debugging
```