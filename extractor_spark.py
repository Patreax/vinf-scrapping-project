import os
import re
import glob
import shutil
from datetime import datetime
from typing import Optional
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StringType, StructType, StructField

class RegexPatterns:
    """Class containing all regex patterns for data extraction"""
    
    # Company name patterns
    COMPANY_NAME = [
        r'<title>([^<]+)\s*\([A-Z]+\)',  # Title tag pattern
        r'<h1[^>]*>([^<]+)</h1>',  # H1 tag pattern
        r'"companyName":"([^"]+)"',  # JSON company name
        r'<meta[^>]*name="description"[^>]*content="([^"]+)"',  # Meta description
    ]
    
    # Exchange patterns
    EXCHANGE = [
        r'Primary exchange.*?class="P6K39c">([^<]+)<',
    ]
    
    # Current price patterns
    CURRENT_PRICE = [
        r'data-last-price="([\d.]+)"',
    ]
    
    # Market cap patterns
    MARKET_CAP = [
        r'Market cap.*?class="P6K39c">([^<]+)<',
    ]
    
    # Founded year patterns
    FOUNDED_YEAR = [
        r'Founded.*?class="P6K39c">([^<]+)<',
    ]
    
    # Employee count patterns
    EMPLOYEES = [
        r'<div class="P6K39c">(\d{1,3}(?:,\d{3})*)</div>',
    ]
    
    # Revenue patterns
    REVENUE = [
        r'Revenue.*?class="QXDnM">([^<]+)<',
    ]
    
    # EBITDA patterns
    EBITDA = [
        r'EBITDA.*?class="QXDnM">([^<]+)<',
    ]

    # Previous close patterns
    PREVIOUS_CLOSE = [
        r'Previous close.*?class="P6K39c">([\$]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
    ]


def extract_with_patterns(html_content: str, patterns: list, use_search: bool = False) -> Optional[str]:
    """Extract value using a list of regex patterns"""
    if not html_content:
        return None
    
    for pattern in patterns:
        if use_search:
            match = re.search(pattern, html_content, re.IGNORECASE | re.DOTALL)
            if match:
                return match.group(1).strip()
        else:
            matches = re.findall(pattern, html_content, re.IGNORECASE)
            if matches:
                return matches[0].strip()
    return None


def extract_company_name(html_content: str, symbol: str) -> str:
    """Extract company name from the HTML content"""
    if not html_content:
        return f"{symbol} Corporation"
    
    for pattern in RegexPatterns.COMPANY_NAME:
        matches = re.findall(pattern, html_content, re.IGNORECASE)
        if matches:
            company_name = matches[0].strip()
            if company_name:
                return company_name
    
    return f"{symbol} Corporation"


def extract_exchange(html_content: str) -> Optional[str]:
    """Extract exchange from the HTML content"""
    exchange = extract_with_patterns(html_content, RegexPatterns.EXCHANGE)
    if exchange:
        return exchange.upper()
    return None


def extract_current_price(html_content: str) -> Optional[str]:
    """Extract current stock price from HTML content"""
    price = extract_with_patterns(html_content, RegexPatterns.CURRENT_PRICE, use_search=True)
    if price:
        price = price.replace(',', '').replace('$', '')
        return f"${price}"
    return None


def extract_previous_close(html_content: str) -> Optional[str]:
    """Extract previous close price from HTML content"""
    previous_close = extract_with_patterns(html_content, RegexPatterns.PREVIOUS_CLOSE, use_search=True)
    if previous_close:
        previous_close = previous_close.replace(',', '').replace('$', '')
        return f"${previous_close}"
    return None


def extract_market_cap(html_content: str) -> Optional[str]:
    """Extract market cap from HTML content"""
    return extract_with_patterns(html_content, RegexPatterns.MARKET_CAP)


def extract_founded_year(html_content: str) -> Optional[str]:
    """Extract founded year from HTML content"""
    return extract_with_patterns(html_content, RegexPatterns.FOUNDED_YEAR)


def extract_employees(html_content: str) -> Optional[str]:
    """Extract number of employees from HTML content"""
    employees = extract_with_patterns(html_content, RegexPatterns.EMPLOYEES)
    if employees:
        return employees.replace(',', '')
    return None


def extract_revenue(html_content: str) -> Optional[str]:
    """Extract revenue from HTML content"""
    return extract_with_patterns(html_content, RegexPatterns.REVENUE)


def extract_ebitda(html_content: str) -> Optional[str]:
    """Extract EBITDA from HTML content"""
    return extract_with_patterns(html_content, RegexPatterns.EBITDA, use_search=True)


def calculate_percentage_change(current_price: Optional[str], previous_close: Optional[str]) -> Optional[str]:
    """Calculate percentage change between current price and previous close"""
    if current_price and previous_close:
        try:
            current_price_str = current_price.replace('$', '').replace(',', '')
            previous_close_str = previous_close.replace('$', '').replace(',', '')
            
            current_price_val = float(current_price_str)
            previous_close_val = float(previous_close_str)
            
            if previous_close_val != 0:
                percentage_change = (current_price_val - previous_close_val) / previous_close_val * 100
                return f"{percentage_change:+.2f}%"
        except (ValueError, ZeroDivisionError):
            pass
    return None


def calculate_difference(current_price: Optional[str], previous_close: Optional[str]) -> Optional[str]:
    """Calculate difference between current price and previous close"""
    if current_price and previous_close:
        try:
            current_price_str = current_price.replace('$', '').replace(',', '')
            previous_close_str = previous_close.replace('$', '').replace(',', '')
            
            current_price_val = float(current_price_str)
            previous_close_val = float(previous_close_str)
            
            raw_difference = current_price_val - previous_close_val
            return f"${raw_difference:+.2f}"
        except ValueError:
            pass
    return None


def extract_symbol_from_filename(filename: str) -> str:
    """Extract symbol from filename (e.g., 'AAPL_20251016_141945.html' -> 'AAPL')"""
    basename = os.path.basename(filename)
    return basename.split('_')[0]


def extract_timestamp_from_filename(filename: str) -> str:
    """
    Extract timestamp from filename.
    
    Filename format: SYMBOL_YYYYMMDD_HHMMSS.html
    Example: SCHJ_20251017_133113.html -> 2025-10-17 13:31:13
    
    Args:
        filename: Path to HTML file
    
    Returns:
        Timestamp string in format '%Y-%m-%d %H:%M:%S' or current time if extraction fails
    """
    try:
        # Extract filename from path
        basename = os.path.basename(filename)
        # Remove .html extension
        name_without_ext = basename.replace('.html', '')
        
        # Split by underscore: SYMBOL_YYYYMMDD_HHMMSS
        parts = name_without_ext.split('_')
        if len(parts) >= 3:
            date_str = parts[1]  # YYYYMMDD
            time_str = parts[2]   # HHMMSS
            
            # Parse date and time
            if len(date_str) == 8 and len(time_str) == 6:
                year = date_str[0:4]
                month = date_str[4:6]
                day = date_str[6:8]
                hour = time_str[0:2]
                minute = time_str[2:4]
                second = time_str[4:6]
                
                timestamp_str = f"{year}-{month}-{day} {hour}:{minute}:{second}"
                # Validate by trying to parse it
                datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
                return timestamp_str
    except Exception:
        pass
    
    # Fallback to current time if extraction fails
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def process_html_file(file_path: str, html_content: str) -> Row:
    """
    Process a single HTML file and extract stock data
    
    Args:
        file_path: Path to HTML file
        html_content: Content of the HTML file
    
    Returns:
        Row containing extracted stock data
    """
    symbol = extract_symbol_from_filename(file_path)
    company_name = extract_company_name(html_content, symbol)
    exchange = extract_exchange(html_content)
    current_price = extract_current_price(html_content)
    previous_close = extract_previous_close(html_content)
    market_cap = extract_market_cap(html_content)
    founded = extract_founded_year(html_content)
    employees = extract_employees(html_content)
    revenue = extract_revenue(html_content)
    ebitda = extract_ebitda(html_content)
    
    calculated_percentage_change = calculate_percentage_change(current_price, previous_close)
    calculated_difference = calculate_difference(current_price, previous_close)
    
    # Extract timestamp from filename instead of using current time
    timestamp = extract_timestamp_from_filename(file_path)
    
    return Row(
        company=company_name,
        symbol=symbol,
        exchange=exchange if exchange else "",
        source_file=file_path,
        timestamp=timestamp,
        current_price=current_price if current_price else "",
        previous_close=previous_close if previous_close else "",
        calculated_percentage_change=calculated_percentage_change if calculated_percentage_change else "",
        calculated_difference=calculated_difference if calculated_difference else "",
        market_cap=market_cap if market_cap else "",
        founded=founded if founded else "",
        employees=employees if employees else "",
        revenue=revenue if revenue else "",
        ebitda=ebitda if ebitda else ""
    )


def read_html_file(file_path: str) -> tuple:
    """Read HTML file and return (file_path, content) tuple"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        return (file_path, content)
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return (file_path, "")


def main():
    """Main function to process HTML files using Spark"""
    
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("StockDataExtractor") \
        .getOrCreate()
    
    # Get the HTML directory path
    html_dir = os.path.join(os.path.dirname(__file__), "html")
    
    # Get all HTML files
    html_files = [os.path.join(html_dir, f) for f in os.listdir(html_dir) if f.endswith('.html')]
    
    print(f"Found {len(html_files)} HTML files to process")
    
    # Create RDD from file paths
    file_paths_rdd = spark.sparkContext.parallelize(html_files)
    
    # Read HTML files in parallel
    html_rdd = file_paths_rdd.map(lambda file_path: read_html_file(file_path))
    
    # Process HTML files and extract data
    extracted_data_rdd = html_rdd.map(lambda x: process_html_file(x[0], x[1]))
    
    # Define schema for the DataFrame
    schema = StructType([
        StructField("company", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("exchange", StringType(), True),
        StructField("source_file", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("current_price", StringType(), True),
        StructField("previous_close", StringType(), True),
        StructField("calculated_percentage_change", StringType(), True),
        StructField("calculated_difference", StringType(), True),
        StructField("market_cap", StringType(), True),
        StructField("founded", StringType(), True),
        StructField("employees", StringType(), True),
        StructField("revenue", StringType(), True),
        StructField("ebitda", StringType(), True)
    ])
    
    # Convert RDD to DataFrame
    df = spark.createDataFrame(extracted_data_rdd, schema)
    
    # Show some statistics
    print(f"Processed {df.count()} records")
    
    # Define output path
    output_path = os.path.join(os.path.dirname(__file__), "data", "extracted_data_spark.tsv")
    
    # Ensure data directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Save to TSV file
    # Spark saves as CSV with part files, so we'll use a temp directory
    temp_dir = output_path.replace('.tsv', '_temp')
    
    # Write DataFrame to TSV format (using CSV writer with tab delimiter)
    df.coalesce(1).write \
        .mode("overwrite") \
        .option("sep", "\t") \
        .option("header", "true") \
        .csv(temp_dir)
    
    # Find and rename the part file to the final TSV file
    part_files = glob.glob(os.path.join(temp_dir, "part-*.csv"))
    
    if part_files:
        # Move the part file to the final location with .tsv extension
        shutil.move(part_files[0], output_path)
        # Clean up temp directory
        shutil.rmtree(temp_dir, ignore_errors=True)
        print(f"Data saved to {output_path}")
    else:
        print(f"Warning: No output file generated in {temp_dir}")
    
    # Stop Spark session
    spark.stop()


if __name__ == "__main__":
    main()

