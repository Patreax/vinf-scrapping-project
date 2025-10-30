# Google Finance Scraper Project

## Setup

Create python virtual environment and install `tiktoken`. The following commands apply on Linux.

```bash
python -m venv venv
source venv/bin/activate
pip install tiktoken
```

## Commands

To run the scraper:

```bash
python scraper.py
```

To create index and initiate search:
```bash
python search.py
```

To generate statistics:
```bash
python statistics.py
```

To run unit tests:
```bash
python extractor-test.py
```