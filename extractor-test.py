from extractor import StockDataExtractor


def test_extraction(path):
    success = True

    with open(path, 'r') as file:
        html = file.read()
        
    # Extract the stock data from the HTML file
    extractor = StockDataExtractor()
    company_name = extractor.extract_company_name(html, path)
    exchange = extractor.extract_exchange(html, path)
    current_price = extractor.extract_current_price(html)
    previous_close = extractor.extract_previous_close(html)
    market_cap = extractor.extract_market_cap(html)
    founded_year = extractor.extract_founded_year(html)
    employees = extractor.extract_employees(html)
    revenue = extractor.extract_revenue(html)
    ebitda = extractor.extract_ebitda(html)

    # Check if the extraction was successful
    if company_name is None:
        print(f"ERROR: Company name is None for {path}")
        success = False
    if exchange is None:
        print(f"ERROR: Exchange is None for {path}")
        success = False
    if current_price is None:
        print(f"ERROR: Current price is None for {path}")
        success = False
    if previous_close is None:
        print(f"ERROR: Previous close is None for {path}")
        success = False
    if market_cap is None:
        print(f"ERROR: Market cap is None for {path}")
        success = False
    if founded_year is None:
        print(f"ERROR: Founded year is None for {path}")
        success = False
    if employees is None:
        print(f"ERROR: Employees is None for {path}")
        success = False
    if revenue is None:
        print(f"ERROR: Revenue is None for {path}")
        success = False
    if ebitda is None:
        print(f"ERROR: EBITDA is None for {path}")
        success = False
        
    return success

# Load all the paths from the unit-tests/paths.txt file
# The paths lead to the HTML files that contain the stock data
def load_paths():
    paths = []
    with open('unit-tests/paths.txt', 'r') as file:
        paths = [line.strip() for line in file.readlines()]
    return paths


def main():
    paths = load_paths()
    total_tests = len(paths)
    failed_tests = 0
    
    # Test each path and check if the extraction was successful
    for path in paths:
        status = test_extraction(path)
        if not status:
            failed_tests += 1
    print(f"Total tests: {total_tests}")
    print(f"Failed tests: {failed_tests}")
    print(f"Success rate: {100 - (failed_tests / total_tests) * 100}%")
    
if __name__ == "__main__":
    main()