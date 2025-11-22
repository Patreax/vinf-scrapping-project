"""
Step 3: Extract company information from filtered Wikipedia pages
"""

import argparse
import re
import os
import csv
from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.functions import udf

# Match {{Infobox ... }} blocks
INFOBOX_BLOCK = re.compile(r"\{\{\s*Infobox[\s\S]*?\n\}\}", re.IGNORECASE)

def load_match_keys(keys_file):
    keys = set()
    if not os.path.exists(keys_file):
        raise FileNotFoundError(f"Match keys file not found: {keys_file}")
    
    with open(keys_file, 'r', encoding='utf-8') as f:
        for line in f:
            key = line.strip().lower()
            if key and len(key) >= 3:
                keys.add(key)
    
    return keys

def _clean_wiki_markup(v: str) -> str:
    if not v:
        return ""
    v = re.sub(r"<!--.*?-->", "", v, flags=re.DOTALL)
    v = re.sub(r"<ref[^>]*>.*?</ref>", "", v, flags=re.DOTALL | re.IGNORECASE)
    v = re.sub(r"<ref[^>]*/>", "", v, flags=re.IGNORECASE)
    v = re.sub(r"\{\{.*?\}\}", "", v, flags=re.DOTALL)
    v = re.sub(r"\[\[(?:[^\]|]*\|)?([^\]]+)\]\]", r"\1", v)
    v = re.sub(r"\[([^\]]+)\]", r"\1", v)
    v = re.sub(r"<[^>]+>", "", v)
    v = v.replace("&lt;", "<").replace("&gt;", ">").replace("&amp;", "&")
    v = v.replace("&nbsp;", " ").replace("&quot;", '"').replace("&#39;", "'")
    v = re.sub(r"\|\s*\w+\s*=", "", v)
    v = re.sub(r"(?i)(image|file):[^\s]+", "", v)
    v = re.sub(r"\|[^|]*=.*?}}", "", v)
    v = re.sub(r"'{3,}", "", v)
    v = " ".join(v.split())
    return v.strip()

def parse_infobox_py(wikitext: str):
    if not wikitext:
        return {}
    m = INFOBOX_BLOCK.search(wikitext)
    if not m:
        return {}
    
    out = {}
    for line in m.group(0).splitlines():
        line = line.strip()
        if not line.startswith("|") or "=" not in line:
            continue
        try:
            k, v = line.split("=", 1)
            k = k.strip("| ").lower()
            if not k or k.startswith("<!--") or "<!--" in k:
                continue
            v = v.strip()
            if v.startswith("<!--") or (v.startswith("|") and "=" in v):
                continue
            v = _clean_wiki_markup(v)
            if v:
                out[k] = v
        except ValueError:
            pass
    return out

def _extract_field_list(wikitext: str, field_names_regex: str):
    if not wikitext:
        return []
    
    infobox = parse_infobox_py(wikitext)
    field_names_to_try = []
    
    for part in field_names_regex.split("|"):
        part = part.strip()
        base = re.sub(r"\([^)]*\)", "", part).strip()
        if base:
            field_names_to_try.append(base)
        if "(?:y|ies)" in part or "(y|ies)" in part:
            field_names_to_try.append(base + "y")
            field_names_to_try.append(base + "ies")
    
    for field_name in field_names_to_try:
        if field_name in infobox:
            value = infobox[field_name]
            if value:
                text = _clean_wiki_markup(value)
                parts = re.split(r"[,\n•·;]", text)
                result = [p.strip() for p in parts if p.strip() and len(p.strip()) > 1]
                if result:
                    return result
    
    # Match |field_name = value patterns in wikitext
    pattern = re.compile(rf"\|\s*(?:{field_names_regex})\s*=\s*(.*?)(?:\n\||\n\}})", re.IGNORECASE | re.DOTALL)
    m = pattern.search(wikitext)
    if not m:
        return []
    
    text = _clean_wiki_markup(m.group(1).strip())
    if text.startswith("|") or "<!--" in text or len(text) < 2:
        return []
    
    parts = re.split(r"[,\n•·;]", text)
    return [p.strip() for p in parts if p.strip() and len(p.strip()) > 1]

def extract_industries_py(wikitext: str):
    return _extract_field_list(wikitext, r"industr(?:y|ies)")

def extract_founders_py(wikitext: str):
    return _extract_field_list(wikitext, r"founders?")

def extract_headquarters_py(wikitext: str):
    return _extract_field_list(wikitext, r"headquarters|hq_location|location")

def _extract_url_from_text(text: str) -> str:
    url_pattern = re.compile(r'https?://[^\s<>"{}|\\^`\[\]]+', re.IGNORECASE)
    url_match = url_pattern.search(text)
    if url_match:
        return url_match.group(0)
    
    domain_pattern = re.compile(r'[a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(\.[a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*\.[a-zA-Z]{2,}', re.IGNORECASE)
    domain_match = domain_pattern.search(text)
    if domain_match:
        domain = domain_match.group(0)
        return domain if domain.startswith(('http://', 'https://')) else f"https://{domain}"
    
    return text if 3 < len(text) < 200 and not text.startswith("|") else ""

def extract_website_py(wikitext: str):
    if not wikitext:
        return ""
    
    infobox = parse_infobox_py(wikitext)
    website_fields = ["website", "website_url", "url", "homepage", "official_website", "web"]
    
    for field in website_fields:
        if field in infobox:
            value = infobox[field]
            if value:
                cleaned = _clean_wiki_markup(value)
                result = _extract_url_from_text(cleaned)
                if result:
                    return result
    
    # Match website field patterns in wikitext
    pattern = re.compile(r"\|\s*(?:website|website_url|url|homepage|official_website)\s*=\s*(.*?)(?:\n\||\n\}})", re.IGNORECASE | re.DOTALL)
    m = pattern.search(wikitext)
    if m:
        text = _clean_wiki_markup(m.group(1).strip())
        if text and 3 < len(text) < 200:
            return _extract_url_from_text(text)
    
    return ""

def extract_traded_as_py(wikitext: str):
    infobox = parse_infobox_py(wikitext)
    traded_as = infobox.get("traded_as") or infobox.get("symbol") or infobox.get("ticker")
    
    if not traded_as:
        return ""
    
    symbols = []
    exchange_codes = ['BSE', 'NSE', 'NYSE', 'NASDAQ', 'LSE', 'ASX', 'TSX', 'TSE', 'HKEX', 'XETR', 'FWB']
    remaining = traded_as
    
    for _ in range(10):
        found = False
        for exchange_code in exchange_codes:
            # Match {{EXCHANGE_CODE|SYMBOL}} templates
            pattern = r'\{\{' + re.escape(exchange_code) + r'\|([^{}]+)\}\}'
            match = re.search(pattern, remaining, re.IGNORECASE)
            if match:
                symbol = re.sub(r'[|{}]', '', match.group(1).strip()).strip()
                if symbol and len(symbol) <= 20:
                    symbols.append(f"{exchange_code}:{symbol}")
                remaining = remaining[:match.start()] + remaining[match.end():]
                found = True
                break
        if not found:
            break
    
    if symbols:
        traded_as = "; ".join(symbols)
    else:
        traded_as = _clean_wiki_markup(traded_as)
    
    return traded_as.strip() if traded_as else ""

def extract_first_paragraph_py(wikitext: str):
    if not wikitext:
        return ""
    
    if "<text" in wikitext and "</text>" in wikitext:
        # Extract content between <text> and </text> tags
        text_match = re.search(r'<text[^>]*>(.*?)</text>', wikitext, re.DOTALL | re.IGNORECASE)
        if text_match:
            wikitext = text_match.group(1)
    
    text = INFOBOX_BLOCK.sub("", wikitext)
    
    # Remove page metadata patterns like "Title 0 123456 ..."
    text = re.sub(r'^[A-Z][^a-z]*?\d+\s+\d+\s+\d+\s+\d+.*?wikitext', '', text, flags=re.MULTILINE | re.DOTALL)
    
    # Remove revision metadata patterns (e.g., "Title 0 123456 1314501620 1309608472 2025-10-01T19:20:08Z User 12345")
    text = re.sub(r'^[A-Z][^a-z]*?\s+\d+\s+\d+\s+\d+\s+\d+\s+\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z[^\n]*\n', '', text, flags=re.MULTILINE)
    
    # Split by section headers (==) to get lead section
    sections = re.split(r'^==+[^=].*?==+', text, flags=re.MULTILINE)
    lead_section = sections[0] if sections else text
    
    # Remove timestamps
    lead_section = re.sub(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z', '', lead_section)
    # Remove /* comments */
    lead_section = re.sub(r'/\*.*?\*/', '', lead_section)
    
    lines = lead_section.split('\n')
    filtered_lines = []
    for line in lines:
        line_stripped = line.strip()
        # Skip lines that start with numbers or look like metadata
        if (not line_stripped or
            re.match(r'^\d+\s+\d+\s+\d+', line_stripped) or
            re.match(r'^[A-Z][^a-z]*?\s+\d+', line_stripped) or
            'wikitext' in line_stripped.lower() or
            'text/x-wiki' in line_stripped.lower()):
            continue
        filtered_lines.append(line)
    lead_section = '\n'.join(filtered_lines)
    
    # Split by paragraph breaks
    paragraphs = re.split(r'\n\n+', lead_section)
    
    for para in paragraphs:
        para_stripped = para.strip()
        if not para_stripped:
            continue
        
        if (para_stripped.startswith("|") or 
            para_stripped.startswith("{{") or 
            para_stripped.startswith("<!--") or
            re.match(r'^[A-Z][^a-z]*?\d+', para_stripped) or
            len(para_stripped) < 30):
            continue
        
        cleaned = _clean_wiki_markup(para)
        
        if len(cleaned) < 50:
            continue
        
        # Check for common descriptive words
        has_common_words = (
            re.search(r'\b(is|was|are|were|has|have|had)\s+(a|an|the)', cleaned, re.IGNORECASE) or
            re.search(r'\b(founded|established|headquartered|based|located)', cleaned, re.IGNORECASE) or
            re.search(r'\b(company|corporation|organization|firm|business)', cleaned, re.IGNORECASE)
        )
        
        # Check for sentence structure
        has_sentence = re.search(r'[.!?]', cleaned)
        # Check for natural text (at least one 3+ letter word)
        has_natural_text = re.search(r'[a-z]{3,}', cleaned, re.IGNORECASE)
        
        if not (has_common_words or (has_sentence and has_natural_text)):
            continue
        
        if len(cleaned) > 1000:
            # Find last complete sentence within 1000 chars
            match = re.search(r'^(.{1,1000}[.!?])\s+', cleaned, re.MULTILINE)
            if match:
                cleaned = match.group(1).strip()
            else:
                cleaned = cleaned[:1000].strip()
        
        return cleaned
    
    cleaned = _clean_wiki_markup(lead_section)
    # Remove remaining metadata patterns
    cleaned = re.sub(r'^\s*\d+\s+\d+\s+\d+.*?$', '', cleaned, flags=re.MULTILINE)
    cleaned = re.sub(r'wikitext\s+text/x-wiki', '', cleaned, flags=re.IGNORECASE)
    
    if len(cleaned) > 50:
        # Find first sentence or first 1000 chars
        match = re.search(r'^(.{50,1000}[.!?])\s+', cleaned, re.MULTILINE)
        if match:
            cleaned = match.group(1).strip()
        else:
            if len(cleaned) > 1000:
                cleaned = cleaned[:1000].rsplit(' ', 1)[0]
            cleaned = cleaned.strip()
        
        if len(cleaned) >= 50:
            return cleaned
    
    return ""

def main():
    parser = argparse.ArgumentParser(description="Extract company info from filtered Wikipedia pages")
    parser.add_argument("--pages", default="out/wiki_filtered_pages",
                       help="Path to filtered pages directory (default: out/wiki_filtered_pages)")
    parser.add_argument("--keys", default="data/match_keys_final.txt",
                       help="Path to match_keys.txt (default: data/match_keys_final.txt)")
    parser.add_argument("--out", default="data/company_info.tsv",
                       help="Output TSV file (default: data/company_info.tsv)")
    parser.add_argument("--format", choices=["json", "parquet"], default="json",
                       help="Input format of filtered pages (default: json)")
    args = parser.parse_args()

    print("=" * 60)
    print("STAGE 3: Extracting company information from Wikipedia pages")
    print("=" * 60)
    
    print(f"\nLoading match keys from {args.keys}...")
    match_keys = load_match_keys(args.keys)
    print(f"Loaded {len(match_keys)} match keys")
    
    spark = (
        SparkSession.builder
        .appName("step3-extract-company-info")
        .config("spark.sql.shuffle.partitions", 4)
        .getOrCreate()
    )
    
    sc = spark.sparkContext
    keys_bc = sc.broadcast(match_keys)
    
    print(f"\nReading filtered pages from {args.pages}...")
    if args.format == "json":
        df = spark.read.json(args.pages)
    else:
        df = spark.read.parquet(args.pages)
    
    print(f"Loaded {df.count():,} pages")
    
    parse_infobox = udf(parse_infobox_py, T.MapType(T.StringType(), T.StringType()))
    extract_industries = udf(extract_industries_py, T.ArrayType(T.StringType()))
    extract_founders = udf(extract_founders_py, T.ArrayType(T.StringType()))
    extract_headquarters = udf(extract_headquarters_py, T.ArrayType(T.StringType()))
    extract_traded_as = udf(extract_traded_as_py, T.StringType())
    extract_website = udf(extract_website_py, T.StringType())
    extract_first_paragraph = udf(extract_first_paragraph_py, T.StringType())
    
    def get_keyword_func(title):
        if not title:
            return None
        keys = keys_bc.value
        title_lower = title.lower()
        title_words = title_lower.split()
        if title_words:
            first_word = title_words[0]
            if first_word in keys:
                return first_word
            for key in keys:
                if key in title_lower:
                    return key
        return None
    
    get_keyword = udf(get_keyword_func, T.StringType())
    
    print("\nExtracting company information...")
    extracted = (
        df
        .withColumn("keyword", get_keyword(F.col("title")))
        .withColumn("infobox", parse_infobox(F.col("page")))
        .withColumn("traded_as", extract_traded_as(F.col("page")))
        .withColumn("industries", extract_industries(F.col("page")))
        .withColumn("founders", extract_founders(F.col("page")))
        .withColumn("headquarters", extract_headquarters(F.col("page")))
        .withColumn("website", extract_website(F.col("page")))
        .withColumn("first_paragraph", extract_first_paragraph(F.col("page")))
        .filter(F.col("traded_as").isNotNull() & (F.col("traded_as") != ""))
        .filter(F.col("keyword").isNotNull())
        .select(
            "keyword",
            "title",
            "traded_as",
            F.concat_ws("; ", F.col("industries")).alias("industries"),
            F.concat_ws("; ", F.col("founders")).alias("founders"),
            F.concat_ws("; ", F.col("headquarters")).alias("headquarters"),
            "website",
            "first_paragraph"
        )
    )
    
    count = extracted.count()
    print(f"\nFound {count:,} public companies with matching keywords")
    
    if count == 0:
        print("No matching companies found. Exiting.")
        spark.stop()
        return
    
    print(f"\nSaving to {args.out}...")
    os.makedirs(os.path.dirname(args.out) if os.path.dirname(args.out) else '.', exist_ok=True)
    
    print("Collecting results...")
    rows = extracted.collect()
    
    print(f"Writing {len(rows)} records to TSV...")
    with open(args.out, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=["keyword", "title", "traded_as", "industries", "founders", "headquarters", "website", "first_paragraph"], 
                               delimiter='\t', extrasaction='ignore')
        writer.writeheader()
        for row in rows:
            writer.writerow({
                "keyword": row["keyword"] or "",
                "title": row["title"] or "",
                "traded_as": row["traded_as"] or "",
                "industries": row["industries"] or "",
                "founders": row["founders"] or "",
                "headquarters": row["headquarters"] or "",
                "website": row["website"] or "",
                "first_paragraph": row["first_paragraph"] or ""
            })
    
    print(f"\nSuccess! Saved {count:,} company records to: {args.out}")
    print("   Columns: keyword, title, traded_as, industries, founders, headquarters, website, first_paragraph")
    
    spark.stop()

if __name__ == "__main__":
    main()

