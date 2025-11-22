"""
Step 4 (Spark): Join company_info.tsv with extracted_data_spark_keys.tsv

This script:
1. Reads company_info.tsv using Spark
2. For each keyword, selects the best row based on number of filled columns
3. Joins with extracted_data_spark_keys.tsv on keyword using Spark
4. Outputs combined TSV with all columns

Uses Spark for better performance on large datasets.
"""

import argparse
import os
import glob
import shutil
from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.window import Window

def main():
    parser = argparse.ArgumentParser(description="Join company_info with extracted_data_spark_keys using Spark")
    parser.add_argument("--company-info", default="data/company_info_3.tsv",
                       help="Path to company_info.tsv (default: data/company_info_3.tsv)")
    parser.add_argument("--extracted-data", default="data/extracted_data_spark_keys.tsv",
                       help="Path to extracted_data_spark_keys.tsv (default: data/extracted_data_spark_keys.tsv)")
    parser.add_argument("--out", default="data/joined_company_data_spark.tsv",
                       help="Output TSV file (default: data/joined_company_data_spark.tsv)")
    args = parser.parse_args()
    
    etf_text_path = "text/etf.txt"
    ltd_text_path = "text/ltd.txt"
    
    print("=" * 60)
    print("STAGE 4 (Spark): Joining company_info with extracted_data")
    print("=" * 60)
    
    if not os.path.exists(args.company_info):
        print(f"ERROR: Company info file not found: {args.company_info}")
        return
    
    if not os.path.exists(args.extracted_data):
        print(f"ERROR: Extracted data file not found: {args.extracted_data}")
        return
    
    spark = (
        SparkSession.builder
        .appName("step4-join-company-data-spark")
        .config("spark.sql.shuffle.partitions", 4)
        .getOrCreate()
    )
    
    print(f"\nReading {args.company_info}...")
    company_info_df = spark.read.option("header", True).option("sep", "\t").csv(args.company_info)
    
    company_info_columns = company_info_df.columns
    data_columns = [col for col in company_info_columns if col != 'keyword']
    
    print(f"Company info columns: {', '.join(company_info_columns)}")
    print(f"Data columns to score: {', '.join(data_columns)}")
    
    def count_filled(cols):
        count_expr = None
        for col in cols:
            col_expr = (
                F.when(F.col(col).isNotNull() & 
                       (F.trim(F.col(col)) != "") & 
                       (F.trim(F.col(col)) != "N/A") & 
                       (F.trim(F.col(col)) != "None") & 
                       (F.trim(F.col(col)) != "null"), 1)
                 .otherwise(0)
            )
            if count_expr is None:
                count_expr = col_expr
            else:
                count_expr = count_expr + col_expr
        return count_expr
    
    company_info_with_score = company_info_df.withColumn(
        "filled_count",
        count_filled(data_columns)
    )
    
    char_count_expr = None
    for col in company_info_columns:
        col_expr = F.length(F.coalesce(F.col(col), F.lit("")))
        if char_count_expr is None:
            char_count_expr = col_expr
        else:
            char_count_expr = char_count_expr + col_expr
    
    company_info_with_score = company_info_with_score.withColumn("char_count", char_count_expr)
    
    company_info_with_score = company_info_with_score.withColumn(
        "keyword_lower",
        F.lower(F.trim(F.col("keyword")))
    ).filter(F.col("keyword_lower") != "")
    
    window = Window.partitionBy("keyword_lower").orderBy(
        F.col("filled_count").desc(),
        F.col("char_count").desc()
    )
    
    best_rows_df = (
        company_info_with_score
        .withColumn("rank", F.row_number().over(window))
        .filter(F.col("rank") == 1)
        .drop("rank", "filled_count", "char_count", "keyword")
        .withColumnRenamed("keyword_lower", "keyword")
    )
    
    company_info_count = company_info_df.count()
    best_rows_count = best_rows_df.count()
    print(f"\nProcessed {company_info_count} rows from company_info")
    print(f"Selected {best_rows_count} best rows (one per keyword)")
    
    print(f"\nReading {args.extracted_data}...")
    extracted_df = spark.read.option("header", True).option("sep", "\t").csv(args.extracted_data)
    
    extracted_count = extracted_df.count()
    print(f"Loaded {extracted_count:,} rows from extracted_data")
    
    extracted_df = extracted_df.withColumn(
        "keyword_lower",
        F.lower(F.trim(F.coalesce(F.col("keyword"), F.lit(""))))
    )
    
    company_info_cols_to_add = [col for col in best_rows_df.columns if col != 'keyword']
    
    print(f"\nOutput columns:")
    print(f"  From extracted_data: {len(extracted_df.columns) - 1} columns (excluding keyword_lower)")
    print(f"  From company_info: {len(company_info_cols_to_add)} columns")
    
    print(f"\nJoining data on 'keyword'...")
    
    extracted_alias = extracted_df.alias("extracted")
    best_rows_alias = best_rows_df.alias("company_info")
    
    joined_df = extracted_alias.join(
        best_rows_alias,
        F.col("extracted.keyword_lower") == F.col("company_info.keyword"),
        "left"
    )
    
    extracted_cols = [F.col(f"extracted.{col}").alias(col) for col in extracted_df.columns if col != 'keyword_lower']
    company_info_cols_to_add_filtered = [col for col in company_info_cols_to_add if col != 'timestamp']
    company_info_cols = [F.col(f"company_info.{col}").alias(col) for col in company_info_cols_to_add_filtered]
    
    output_columns = extracted_cols + company_info_cols
    joined_df = joined_df.select(*output_columns)
    
    etf_text = ""
    if os.path.exists(etf_text_path):
        print(f"\nReading ETF description from {etf_text_path}...")
        with open(etf_text_path, 'r', encoding='utf-8') as f:
            etf_text = f.read().strip()
        print(f"Loaded ETF description ({len(etf_text)} characters)")
    else:
        print(f"\nWarning: ETF text file not found: {etf_text_path}")
    
    ltd_text = ""
    if os.path.exists(ltd_text_path):
        print(f"Reading LTD description from {ltd_text_path}...")
        with open(ltd_text_path, 'r', encoding='utf-8') as f:
            ltd_text = f.read().strip()
        print(f"Loaded LTD description ({len(ltd_text)} characters)")
    else:
        print(f"Warning: LTD text file not found: {ltd_text_path}")
    
    no_match_condition = None
    for col in company_info_cols_to_add:
        col_condition = (
            F.col(col).isNull() | 
            (F.trim(F.coalesce(F.col(col), F.lit(""))) == "")
        )
        if no_match_condition is None:
            no_match_condition = col_condition
        else:
            no_match_condition = no_match_condition & col_condition
    
    contains_etf_condition = (
        F.lower(F.coalesce(F.col("company"), F.lit(""))).contains("etf") |
        F.lower(F.coalesce(F.col("symbol"), F.lit(""))).contains("etf")
    )
    
    # Match company names ending with "ltd" or "limited" (case-insensitive)
    ends_with_ltd_condition = (
        F.lower(F.coalesce(F.col("company"), F.lit(""))).rlike(r".*\bltd\.?$") |
        F.lower(F.coalesce(F.col("company"), F.lit(""))).rlike(r".*\blimited\.?$")
    )
    
    if (etf_text or ltd_text) and no_match_condition is not None:
        description_expr = F.lit("")
        
        if etf_text and ltd_text:
            description_expr = F.when(
                no_match_condition & contains_etf_condition,
                F.lit(etf_text)
            ).when(
                no_match_condition & ends_with_ltd_condition & ~contains_etf_condition,
                F.lit(ltd_text)
            ).otherwise(F.lit(""))
        elif etf_text:
            description_expr = F.when(
                no_match_condition & contains_etf_condition,
                F.lit(etf_text)
            ).otherwise(F.lit(""))
        elif ltd_text:
            description_expr = F.when(
                no_match_condition & ends_with_ltd_condition,
                F.lit(ltd_text)
            ).otherwise(F.lit(""))
        
        joined_df = joined_df.withColumn("description", description_expr)
        
        etf_description_count = joined_df.filter(
            no_match_condition & contains_etf_condition & (F.col("description") != "")
        ).count()
        ltd_description_count = joined_df.filter(
            no_match_condition & ends_with_ltd_condition & (F.col("description") != "") & ~contains_etf_condition
        ).count()
        
        if etf_text:
            print(f"\nAdded ETF description to {etf_description_count:,} unmatched rows containing 'etf'")
        if ltd_text:
            print(f"Added LTD description to {ltd_description_count:,} unmatched rows ending with 'ltd'")
    else:
        joined_df = joined_df.withColumn("description", F.lit(""))
    
    matched_df = joined_df.filter(
        F.col(company_info_cols_to_add[0]).isNotNull() & 
        (F.trim(F.col(company_info_cols_to_add[0])) != "")
    )
    matched_count = matched_df.count()
    unmatched_count = extracted_count - matched_count
    
    unique_matched_keywords = (
        matched_df
        .select("keyword")
        .distinct()
        .count()
    )
    
    total_company_info_keywords = best_rows_df.select("keyword").distinct().count()
    unmatched_keywords = total_company_info_keywords - unique_matched_keywords
    
    print(f"\nJoin Statistics:")
    print(f"  Total rows in extracted_data: {extracted_count:,}")
    print(f"  Matched rows: {matched_count:,}")
    print(f"  Unmatched rows: {unmatched_count:,}")
    print(f"  Unique keywords from company_info that were matched: {unique_matched_keywords:,}")
    print(f"  Total unique keywords in company_info: {total_company_info_keywords:,}")
    print(f"  Unique keywords in company_info not matched: {unmatched_keywords:,}")
    
    print(f"\nWriting joined data to {args.out}...")
    os.makedirs(os.path.dirname(args.out) if os.path.dirname(args.out) else '.', exist_ok=True)
    
    if extracted_count < 100000:
        temp_dir = args.out + ".tmp"
        joined_df.coalesce(1).write.mode("overwrite").option("sep", "\t").option("header", "true").csv(temp_dir)
        
        part_files = glob.glob(f"{temp_dir}/part-*.csv")
        if part_files:
            shutil.move(part_files[0], args.out)
            shutil.rmtree(temp_dir, ignore_errors=True)
    else:
        joined_df.write.mode("overwrite").option("sep", "\t").option("header", "true").csv(args.out)
        print(f"  Note: Output written as partitioned files in directory: {args.out}/")
        print(f"  To merge into single file, use: cat {args.out}/part-*.csv > {args.out}.tsv")
    
    print(f"\n✅ Saved {extracted_count:,} rows to: {args.out}")
    print(f"   Total columns: {len(output_columns) + 1} (including description)")
    
    spark.stop()
    print("\nDone!")

if __name__ == "__main__":
    main()

