#!/usr/bin/env python3
"""
Google Finance HTML Extractor
Processes HTML files from the html directory and extracts stock data
"""

import os
import re
import csv
import glob
from datetime import datetime
from typing import Dict, List, Optional

# TODO: mozno by som mohol dat prec net income 
# TODO: in calculated percentage change I also need to have + or - sign
# TODO: perhaps use lesser number of patterns to reduce false positives
# TODO: maybe use "data-last-price" for getting the current price
# TODO: data from the right table might be extracted as:
#   name_of_field ... class="P6K39c"> -> and then there is the actual value (might need to format it)
class RegexPatterns:
    """Centralized class containing all regex patterns for data extraction"""
    
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
        # r'\(([A-Z]+)\)',  # Pattern: "(NASDAQ)" in title or text
        # r'NASDAQ',  # Direct NASDAQ mention
        # r'NYSE',  # Direct NYSE mention
        # r'AMEX',  # Direct AMEX mention
        # r'Primary exchange[^>]*>([^<]+)',  # Primary exchange label
        # r'Exchange[^>]*>([^<]+)',  # Exchange label
        # r'"exchange":"([^"]+)"',  # JSON exchange format
        # r'Listed on ([A-Z]+)',  # "Listed on NASDAQ" pattern
        # r'([A-Z]+) •',  # Pattern: "NASDAQ •" 
    ]
    
    # Current price patterns
    CURRENT_PRICE = [
        r'data-last-price="([\d.]+)"',
        # r'\$(\d+\.\d+)\s*After Hours:',  # Price before "After Hours"
        # r'<div[^>]*class="[^"]*YMlKec[^"]*"[^>]*>\$(\d+\.\d+)</div>',  # Google Finance price class with $ sign
        # r'"price":"([^"]+)"',  # JSON price format
        # r'<div[^>]*class="[^"]*YMlKec[^"]*"[^>]*>([^<]+)</div>',  # Google Finance price class
        # r'<span[^>]*class="[^"]*YMlKec[^"]*"[^>]*>([^<]+)</span>',  # Alternative price class
        # r'\$(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)',  # Dollar format
    ]
    
    # Market cap patterns
    MARKET_CAP = [
        r'Market cap.*?class="P6K39c">([^<]+)<',
        # r'(\d+\.\d+T)\s*USD',  # Direct pattern: "4.33T USD"
        # r'Market cap[^>]*>([^<]+)',  # Market cap label
        # r'"marketCap":"([^"]+)"',  # JSON market cap format
        # r'\b(\d+\.?\d*)\s*(?:billion|B|trillion|T)\b',  # Market cap format
    ]
    
    # Founded year patterns
    FOUNDED_YEAR = [
        r'Founded.*?class="P6K39c">([^<]+)<',
        # r'Founded[^>]*>(\d{4})',  # Direct pattern: "Founded 1993"
        # r'Founded.*?(\d{4})',  # Pattern with text before year
        # r'Founded[^>]*>([^<]+)',  # Founded label
        # r'Est\.?\s*(\d{4})',  # Established abbreviation
        # r'Established.*?(\d{4})',  # Established pattern
        # r'Since.*?(\d{4})',  # Since pattern
        # r'Company.*?Founded.*?(\d{4})',  # Company section with Founded
        # r'About.*?Founded.*?(\d{4})',  # About section with Founded
        # r'Established.*?(\d{4})',  # Established in company info
        # r'Since.*?(\d{4})',  # Since in company info
    ]
    
    # Employee count patterns
    EMPLOYEES = [
        r'<div class="P6K39c">(\d{1,3}(?:,\d{3})*)</div>',  # Google Finance specific pattern: <div class="P6K39c">36,000</div>
        # r'Employees[^>]*>(\d{1,3}(?:,\d{3})*)',  # Direct pattern: "Employees 125,665"
        # r'Employees[^>]*>(\d+)',  # Pattern: "Employees 125665"
        # r'Workforce[^>]*>(\d{1,3}(?:,\d{3})*)',  # Workforce label
        # r'Staff[^>]*>(\d{1,3}(?:,\d{3})*)',  # Staff label
        # r'Company\s*size[^>]*>(\d{1,3}(?:,\d{3})*)',  # Company size
        # r'Company\s*size[^>]*>(\d+)',  # Company size without commas
        # r'About.*?(\d{1,3}(?:,\d{3})*)\s*employees',  # About section
        # r'Company.*?(\d{1,3}(?:,\d{3})*)\s*employees',  # Company section
        # r'(\d{1,3}(?:,\d{3})*)\s*employees',  # Pattern: "125,665 employees"
        # r'(\d+)\s*employees',  # Pattern: "125665 employees"
        # r'(\d{1,3}(?:,\d{3})*)\s*staff',  # Pattern: "125,665 staff"
        # r'(\d+)\s*staff',  # Pattern: "125665 staff"
        # r'(\d{1,3}(?:,\d{3})*)\s*people',  # Pattern: "125,665 people"
        # r'(\d+)\s*people',  # Pattern: "125665 people"
    ]
    
    # Revenue patterns
    REVENUE = [
        r'Revenue.*?class="QXDnM">([^<]+)<',
        # r'Revenue[^>]*>([^<]*\d+\.\d+B)',  # Revenue patterns - looking for "46.74B" format
        # r'Revenue.*?(\d+\.\d+B)',
    ]
    
    # EBITDA patterns
    EBITDA = [
        r'EBITDA.*?class="QXDnM">([^<]+)<',
        # r'EBITDA[^>]*>([^<]*\d+\.\d+B)',  # EBITDA patterns - looking for "29.11B" format
        # r'EBITDA.*?(\d+\.\d+B)',
    ]
    
    # Net income patterns
    NET_INCOME = [
        r'Net income.*?class="QXDnM">([^<]+)<',
        # r'26\.42B',  # Direct 26.42B pattern
        # r'(\d+\.42B)',  # Pattern ending in .42B
        # r'Net income.*?26\.42B',  # Direct net income with 26.42B
        # r'Net income.*?(\d+\.42B)',  # Net income ending in .42B
        # r'Net income[^>]*>([^<]*26\.42B)',  # Net income with 26.42B
        # r'Net income[^>]*>([^<]*\d+\.42B)',  # Net income ending in .42B
        # r'(\d+\.42B)',  # Any value ending in .42B
        # r'Net income.*?(\d+\.\d+B)',
        # r'Net income[^>]*>([^<]*\d+\.\d+B)',
    ]
    
    # Previous close patterns
    PREVIOUS_CLOSE = [
        r'Previous close.*?class="P6K39c">([\$]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
        # r'Previous close[^>]*>\$?([\d.]+)',  # Direct pattern: "Previous close $123.45"
        # r'Previous close[^>]*>([^<]+)',  # Previous close label
        # r'Prev close[^>]*>\$?([\d.]+)',  # Abbreviated pattern
        # r'Previous close.*?\$([\d.]+)',  # Pattern with $ sign
        # r'Previous close.*?([\d.]+)',  # General pattern
    ]

class StockDataExtractor:
    """Main class for extracting stock data from HTML content"""
    
    def __init__(self):
        self.patterns = RegexPatterns()
    
    def extract_company_name(self, html_content: str, symbol: str) -> str:
        """Extract company name from the HTML content"""
        for pattern in self.patterns.COMPANY_NAME:
            matches = re.findall(pattern, html_content, re.IGNORECASE)
            if matches:
                company_name = matches[0].strip()
                # Clean up the company name
                company_name = re.sub(r'\s*\([A-Z]+\)', '', company_name)  # Remove (NASDAQ) etc
                company_name = re.sub(r'\s*Stock.*', '', company_name)  # Remove "Stock" suffix
                if company_name and len(company_name) > 3:
                    return company_name
        
        # Fallback to symbol-based name
        return f"{symbol} Corporation"
    
    def extract_exchange(self, html_content: str, symbol: str) -> str:
        """Extract exchange from the HTML content"""
        for pattern in self.patterns.EXCHANGE:
            matches = re.findall(pattern, html_content, re.IGNORECASE)
            if matches:
                exchange = matches[0].strip().upper()
                # Validate known exchanges
                if exchange in ['NASDAQ', 'NYSE', 'AMEX', 'OTC']:
                    return exchange
        
        # Fallback to empty value
        return ""
    
    def extract_current_price(self, html_content: str) -> Optional[str]:
        """Extract current stock price from HTML content"""
        for pattern in self.patterns.CURRENT_PRICE:
            # matches = re.findall(pattern, html_content)
            matches = re.search(pattern, html_content, re.DOTALL)
            if matches:
                # Clean and return price
                # price = matches[0].replace(',', '').replace('$', '')
                price = matches.group(1).replace(',', '').replace('$', '')
                return f"${price}"
        return None
    
    def extract_previous_close(self, html_content: str) -> Optional[str]:
        """Extract previous close price from HTML content"""
        for pattern in self.patterns.PREVIOUS_CLOSE:
            # matches = re.findall(pattern, html_content, re.IGNORECASE)
            matches = re.search(pattern, html_content, re.DOTALL)
            if matches:
                # previous_close = matches[0].replace(',', '').replace('$', '')
                previous_close = matches.group(1).replace(',', '').replace('$', '')
                return f"${previous_close}"
        return None
    
    def extract_market_cap(self, html_content: str) -> Optional[str]:
        """Extract market cap from HTML content"""
        for pattern in self.patterns.MARKET_CAP:
            matches = re.findall(pattern, html_content, re.IGNORECASE)
            if matches:
                return matches[0]
        return None
    
    def extract_founded_year(self, html_content: str) -> Optional[str]:
        """Extract founded year from HTML content"""
        for pattern in self.patterns.FOUNDED_YEAR:
            matches = re.findall(pattern, html_content, re.IGNORECASE)
            if matches:
                founded_year = matches[0]
                return founded_year
        return None
    
    def extract_employees(self, html_content: str) -> Optional[str]:
        """Extract number of employees from HTML content"""
        for pattern in self.patterns.EMPLOYEES:
            matches = re.findall(pattern, html_content, re.IGNORECASE)
            if matches:
                employees = matches[0].replace(',', '')
                formatted_count = f"{employees}"
                print(f"🔍 Found employees: {formatted_count}")
                return formatted_count
        return None
    
    def extract_revenue(self, html_content: str) -> Optional[str]:
        """Extract revenue from HTML content"""
        for pattern in self.patterns.REVENUE:
            matches = re.findall(pattern, html_content, re.IGNORECASE)
            if matches:
                value = matches[0].strip()
                return value
        return None
    
    def extract_ebitda(self, html_content: str) -> Optional[str]:
        """Extract EBITDA from HTML content"""
        for pattern in self.patterns.EBITDA:
            matches = re.search(pattern, html_content, re.DOTALL)
            # matches = re.findall(pattern, html_content, re.IGNORECASE)
            if matches:
                # value = matches[0].strip()
                value = matches.group(1).strip()
                return value
        return None
    
    def extract_net_income(self, html_content: str) -> Optional[str]:
        """Extract net income from HTML content"""
        for pattern in self.patterns.NET_INCOME:
            matches = re.findall(pattern, html_content, re.IGNORECASE)
            if matches:
                # Look for the correct Net Income value (26.42B) instead of Revenue (46.74B)
                for value in matches:
                    value = value.strip()
                    if 'B' in value:
                        # Prefer 26.42B or values ending in .42B
                        if '26.42B' in value or '.42B' in value:
                            return value
                        elif value != '46.74B' and value != '29.11B':  # Exclude revenue and EBITDA
                            return value
        return None
    
    def calculate_price_changes(self, current_price: Optional[str], previous_close: Optional[str]) -> Dict[str, str]:
        """Calculate percentage change and difference between current price and previous close"""
        changes = {}
        
        if current_price and previous_close:
            try:
                current_price_str = current_price.replace('$', '').replace(',', '')
                previous_close_str = previous_close.replace('$', '').replace(',', '')
                
                current_price_val = float(current_price_str)
                previous_close_val = float(previous_close_str)
                
                # Calculate raw difference to determine direction
                raw_difference = current_price_val - previous_close_val
                
                # Calculate percentage change: (currentValue - previousClose) / previousClose * 100
                percentage_change = raw_difference / previous_close_val * 100
                changes["calculated_percentage_change"] = f"{percentage_change:+.2f}%"
                
                # Calculate difference: currentValue - previousClose
                changes["calculated_difference"] = f"${raw_difference:+.2f}"
                
            except (ValueError, ZeroDivisionError) as e:
                print(f"⚠️  Could not calculate percentage change: {e}")
        
        return changes
    
    def extract_stock_data_from_html(self, html_content: str, filename: str) -> Dict[str, any]:
        """
        Extract stock data from Google Finance HTML content
        
        Args:
            html_content: The HTML content to parse
            filename: The filename (used to extract symbol)
        
        Returns:
            Dictionary containing extracted stock data
        """
        # Extract symbol from filename (e.g., "NVDA_20250101_120000.html" -> "NVDA")
        symbol = os.path.basename(filename).split('_')[0]
        
        print(f"🔍 Extracting {symbol} stock data from {filename}...")
        
        # Get company name and exchange from the page
        company_name = self.extract_company_name(html_content, symbol)
        exchange = self.extract_exchange(html_content, symbol)
        
        data = {
            "company": company_name,
            "symbol": symbol,
            "exchange": exchange,
            "source_file": filename,
            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # Extract all financial data using individual methods
        current_price = self.extract_current_price(html_content)
        if current_price:
            data["current_price"] = current_price
        
        market_cap = self.extract_market_cap(html_content)
        if market_cap:
            data["market_cap"] = market_cap
        
        founded_year = self.extract_founded_year(html_content)
        if founded_year:
            data["founded"] = founded_year
        
        employees = self.extract_employees(html_content)
        if employees:
            data["employees"] = employees
        
        revenue = self.extract_revenue(html_content)
        if revenue:
            data["revenue"] = revenue
        
        net_income = self.extract_net_income(html_content)
        if net_income:
            data["net_income"] = net_income
        
        ebitda = self.extract_ebitda(html_content)
        if ebitda:
            data["ebitda"] = ebitda
        
        previous_close = self.extract_previous_close(html_content)
        if previous_close:
            data["previous_close"] = previous_close
        
        # Calculate price changes if we have both prices
        price_changes = self.calculate_price_changes(current_price, previous_close)
        data.update(price_changes)
        
        return data


def get_html_files(html_dir: str = "html") -> List[str]:
    """
    Get all HTML files from the html directory
    
    Args:
        html_dir: Directory containing HTML files
    
    Returns:
        List of HTML file paths
    """
    if not os.path.exists(html_dir):
        print(f"❌ HTML directory '{html_dir}' does not exist!")
        return []
    
    html_files = glob.glob(os.path.join(html_dir, "*.html"))
    print(f"📁 Found {len(html_files)} HTML files in '{html_dir}' directory")
    
    return sorted(html_files)


def save_to_tsv(stock_data: Dict[str, any], filename: str = "data/extracted_data.tsv") -> bool:
    """
    Save stock data to TSV file with tab delimiter and header columns
    
    Args:
        stock_data: Dictionary containing stock data
        filename: Output TSV filename
    
    Returns:
        True if successful, False otherwise
    """
    if not stock_data:
        print("❌ No data to save to TSV")
        return False
    
    try:
        # Define the order of columns for the TSV
        fieldnames = [
            "company",
            "symbol", 
            "exchange",
            "source_file",
            "timestamp",
            "current_price",
            "previous_close",
            "calculated_percentage_change",
            "calculated_difference",
            "market_cap",
            "founded",
            "employees",
            "revenue",
            "net_income",
            "ebitda"
        ]
        
        # Check if file exists to determine if we need headers
        file_exists = False
        try:
            with open(filename, 'r') as f:
                file_exists = True
        except FileNotFoundError:
            file_exists = False
        
        # Write data to TSV with tab delimiter
        with open(filename, 'a', newline='', encoding='utf-8') as tsvfile:
            writer = csv.DictWriter(tsvfile, fieldnames=fieldnames, delimiter='\t')
            
            # Write header only if file doesn't exist
            if not file_exists:
                writer.writeheader()
                print(f"📝 Created new TSV file: {filename}")
            
            # Write the data row
            writer.writerow(stock_data)
            print(f"💾 Data saved to {filename}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error saving to TSV: {e}")
        return False


def print_stock_data(stock_data: Dict[str, any]):
    """
    Print stock data to console in a formatted way
    
    Args:
        stock_data: Dictionary containing stock data
    """
    print(f"\n📊 {stock_data['symbol']} Stock Data:")
    print("-" * 40)
    
    for key, value in stock_data.items():
        if key == "calculated_percentage_change":
            print(f"Calculated Percentage Change: {value}")
        elif key == "calculated_difference":
            print(f"Calculated Difference: {value}")
        elif key == "revenue":
            print(f"Revenue (2025): {value}")
        elif key == "net_income":
            print(f"Net Income (2025): {value}")
        elif key == "ebitda":
            print(f"EBITDA (2025): {value}")
        elif key == "founded":
            print(f"Founded: {value}")
        elif key == "employees":
            print(f"Number of Employees: {value}")
        elif key == "source_file":
            print(f"Source File: {value}")
        else:
            print(f"{key.replace('_', ' ').title()}: {value}")


def process_html_file(html_file: str, output_file: str = "data/extracted_data.tsv") -> Dict[str, any]:
    """
    Process a single HTML file and extract stock data
    
    Args:
        html_file: Path to HTML file to process
        output_file: Output TSV filename
    
    Returns:
        Dictionary containing extracted stock data
    """
    print(f"\n{'='*60}")
    print(f"Processing: {os.path.basename(html_file)}")
    print(f"{'='*60}")
    
    extractor = StockDataExtractor()
    
    try:
        # Read HTML file
        with open(html_file, 'r', encoding='utf-8') as f:
            html_content = f.read()
        
        print(f"📖 Read HTML file ({len(html_content):,} characters)")
        
        # Extract stock data
        stock_data = extractor.extract_stock_data_from_html(html_content, html_file)
        
        if stock_data:
            # Print to console
            print_stock_data(stock_data)
            
            # Save to TSV
            if save_to_tsv(stock_data, output_file):
                print(f"✅ Successfully processed {stock_data['symbol']}")
                return stock_data
            else:
                print(f"❌ Failed to save data for {stock_data['symbol']}")
                return None
        else:
            print(f"❌ Failed to extract data from {html_file}")
            return None
            
    except Exception as e:
        print(f"❌ Error processing {html_file}: {e}")
        return None


def print_summary(extracted_data: List[Dict[str, any]]):
    """Print a summary of extraction results"""
    print(f"\n{'='*60}")
    print("📊 EXTRACTION SUMMARY")
    print(f"{'='*60}")
    
    print(f"✅ Successfully processed: {len(extracted_data)} files")
    
    if extracted_data:
        print(f"\n📈 Extracted stock data:")
        for data in extracted_data:
            print(f"   • {data['symbol']} ({data['company']}) - {data.get('current_price', 'N/A')}")
    
    print(f"\n📁 Data saved to: extracted_data.tsv")


def main():
    """
    Main function to extract stock data from HTML files
    """
    print("=" * 60)
    print("🎯 Google Finance HTML Extractor")
    print("=" * 60)
    
    # Process all HTML files
    # extracted_data = process_html_files()
    
    # # Print summary
    # print_summary(extracted_data)
    
    print(f"\n🎉 Extraction completed!")


if __name__ == "__main__":
    main()