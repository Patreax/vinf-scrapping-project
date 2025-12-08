# Google Finance Scraper Project

## Part 1 - Google Finance Scrapper

### Setup

Create python virtual environment and install `tiktoken`. The following commands apply on Linux.

```bash
python -m venv venv
source venv/bin/activate
pip install tiktoken
```

### Commands

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

## Part 2 - Apache Spark

### Extract google finance data using Apache Spark

Script `extractor_spark.py` contains code for extracting relevant data from the downloaded html files using Apache Spark

### Step 1 - Extract keywords

Script `step1_extract_keys.py` contains code for extracting a set of keywords, which will later be used to filter relevant pages from the wikipedia dump.

### Step 2 - Filter Wikipedia pages

Script `step2_filter_wiki_pages.py` contains code for going through all the wikipedia pages, and filtering the ones, which might be important or relevant for the pages we have scrapped.
The filtered pages can be saved in either JSON or Parquet format. Parquet is prefered.

### Step 3 - Extract company data from Wikipedia

Script `step3_extract_comapny_information.py` contains the code to extract the relevant information for each scrapped company. The information is extracted from the filtered wikipedia pages and stored in an output `.tsv` file.

### Step 4 - Join company data

Script `step4_join_company_data_spark.py` contains code for joining the extracted data from google finance pages together with extracted data from wikipedia. This is achieved using Apache Spark and the data is joined based on the `keyword` column.

### Indexing the data

Script `index_joined_data_lucene.py` contains code for creating an index using PyLucene out of the final joined data. This operation is executed inside a docker image which has Lucene set up. The final index is saved in the `lucene` directory.

In order to regenerate an index and start the search cli applicantion can be using the `rebuild-index.sh` script.