"""
Step 1: Extract match keys from extracted_data_spark.tsv

This script:
1. Reads extracted_data_spark.tsv
2. Extracts the first word from the company column (lowercase) as match_key
3. Saves all unique match_keys to a single file
4. Creates extracted_data_spark_keys.tsv with a keyword column
"""

import csv
import argparse
import os

def extract_first_word_lowercase(text):
    """Extract the first word from text and convert to lowercase."""
    if not text or not str(text).strip():
        return None
    words = str(text).strip().split()
    if words:
        return words[0].lower()
    return None

def main():
    # Program parameters
    parser = argparse.ArgumentParser(description="Extract match keys from extracted_data_spark.tsv")
    parser.add_argument("--tsv", default="data/extracted_data_spark.tsv",
                       help="Path to extracted_data_spark.tsv (default: data/extracted_data_spark.tsv)")
    parser.add_argument("--out-keys", default="data/match_keys.txt",
                       help="Path to output file for all match keys (default: data/match_keys.txt)")
    parser.add_argument("--out-tsv", default="data/extracted_data_spark_keys.tsv",
                       help="Path to output TSV with keyword column (default: data/extracted_data_spark_keys.tsv)")
    args = parser.parse_args()

    print(f"Reading {args.tsv}...")
    
    rows = []
    match_keys_set = set()
    
    with open(args.tsv, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter='\t')
        fieldnames = reader.fieldnames
        
        for row in reader:
            # Extract first word (lowercase) from company column
            keyword = extract_first_word_lowercase(row.get('company', ''))
            # Only keep keywords with 3 or more characters
            if keyword and len(keyword) >= 3:
                row['keyword'] = keyword
                match_keys_set.add(keyword)
            else:
                row['keyword'] = ''
            
            rows.append(row)
    
    print(f"Processed {len(rows)} rows")
    print(f"Found {len(match_keys_set)} unique match keys (3+ characters)")
    
    # Sort match keys (already filtered to 3+ characters)
    match_keys = sorted(match_keys_set)

    # Save all match keys to a single file (one per line)
    print(f"Writing match keys to {args.out_keys}...")
    os.makedirs(os.path.dirname(args.out_keys) if os.path.dirname(args.out_keys) else '.', exist_ok=True)
    with open(args.out_keys, 'w', encoding='utf-8') as f:
        for key in match_keys:
            f.write(f"{key}\n")

    # Save the TSV with keyword column
    print(f"Writing TSV with keyword column to {args.out_tsv}...")
    os.makedirs(os.path.dirname(args.out_tsv) if os.path.dirname(args.out_tsv) else '.', exist_ok=True)
    
    # Add 'keyword' to fieldnames if not already present
    output_fieldnames = list(fieldnames) + ['keyword']
    
    with open(args.out_tsv, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=output_fieldnames, delimiter='\t', extrasaction='ignore')
        writer.writeheader()
        writer.writerows(rows)

    print("Done")
    print(f"  - Match keys saved to: {args.out_keys}")
    print(f"  - TSV with keywords saved to: {args.out_tsv}")

if __name__ == "__main__":
    main()

