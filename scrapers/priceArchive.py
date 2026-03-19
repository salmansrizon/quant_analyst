import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from datetime import datetime, timedelta
from urllib.parse import urlencode

# Dagster
from dagster import op, job, OpExecutionContext

# logging utility
from utils.logger import Log

# create a module-level logger with file output
from datetime import datetime as _dt

log_filename = f"logs/priceArchive_{_dt.now().strftime('%Y%m%d_%H%M%S')}.log"
logger = Log(name="priceArchive", filename=log_filename)

# Headers to mimic a real browser
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Cache-Control': 'max-age=0',
}


def get_session():
    """Create a session with retry strategy"""
    session = requests.Session()
    retry_strategy = requests.adapters.Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504]
    )
    adapter = requests.adapters.HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def get_date_range(years=3):
    """Calculate date range for last N years"""
    today = datetime.now()
    start_date = today - timedelta(days=365*years)
    return start_date.strftime('%Y-%m-%d'), today.strftime('%Y-%m-%d')


def get_symbols_from_sectors():
    """Extract all symbols from the all sectors CSV file"""
    try:
        df = pd.read_csv('lankabd_data_all_sectors.csv')
        symbols = df['Symbol'].unique()
        logger.info(f"Extracted {len(symbols)} unique symbols from lankabd_data_all_sectors.csv")
        return list(symbols)
    except FileNotFoundError:
        logger.error("lankabd_data_all_sectors.csv not found. Please run main.py first.")
        return []
    except Exception as e:
        logger.error(f"Error reading CSV: {e}")
        return []


def scrape_price_archive(symbol, from_date, to_date):
    """Scrape price archive data for a specific symbol and date range"""
    url = "https://lankabd.com/Home/PriceArchive"
    
    try:
        logger.info(f"Starting scrape for symbol={symbol}, range={from_date} to {to_date}")
        session = get_session()
        
        headers = HEADERS.copy()
        headers.update({
            'Referer': 'https://lankabd.com/',
            'Dnt': '1',
        })
        
        # First request to set cookies
        logger.debug("Making initial request to root to obtain cookies")
        session.get("https://lankabd.com/", headers=headers, timeout=30)
        
        # Build URL with parameters
        params = {
            'symbol': symbol,
            'fromDate': from_date,
            'toDate': to_date
        }
        
        request_url = f"{url}?{urlencode(params)}"
        logger.debug(f"Constructed request URL: {request_url}")
        
        response = session.get(request_url, headers=headers, timeout=30)
        response.raise_for_status()
        logger.debug("Received response from price archive page")
        
        # Parse the HTML content
        soup = BeautifulSoup(response.text, 'lxml')
        
        # Find the table with price data
        table = soup.find('table', {'class': ['table', 'dataTable', 'price-table']})
        
        if not table:
            # Try to find any table
            tables = soup.find_all('table')
            if len(tables) > 1:
                table = tables[1]  # Usually the second table is the data table
        
        if not table:
            logger.warning(f"No table found for {symbol}")
            return None
        
        # Extract headers
        thead = table.find('thead')
        if thead:
            headers_list = [th.text.strip() for th in thead.find_all('th')]
        else:
            headers_list = None
        
        # Extract data rows
        tbody = table.find('tbody')
        if not tbody:
            return None
        
        rows = tbody.find_all('tr')
        
        if len(rows) == 0:
            return None
        
        data = []
        for row in rows:
            cols = row.find_all('td')
            if cols:
                data.append([col.text.strip() for col in cols])
        
        if not data:
            logger.warning(f"Parsed table but no rows present for {symbol}")
            return None
        
        # Create DataFrame
        if headers_list and len(headers_list) == len(data[0]):
            df = pd.DataFrame(data, columns=headers_list)
        else:
            df = pd.DataFrame(data)
        
        # Add symbol column
        df['Symbol'] = symbol
        logger.info(f"Successfully parsed data for {symbol} ({len(df)} rows)")
        
        return df
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Request error while fetching {symbol}: {str(e)[:50]}")
        return None
    except Exception as e:
        logger.error(f"General error processing {symbol}: {str(e)[:50]}")
        return None


def scrape_all_symbols_price_data(from_date=None, to_date=None):
    """Scrape price data for all symbols from the sectors file"""
    
    if from_date is None or to_date is None:
        from_date, to_date = get_date_range(years=3)
    
    symbols = get_symbols_from_sectors()
    
    if not symbols:
        logger.error("No symbols found to scrape")
        return None
    
    logger.info(f"Fetching price data for last 3 years: {from_date} to {to_date}")
    logger.info(f"Total symbols to process: {len(symbols)}")
    
    all_data = []
    success_count = 0
    failed_symbols = []
    
    for idx, symbol in enumerate(symbols, 1):
        logger.info(f"[{idx}/{len(symbols)}] Fetching price data for {symbol}...")
        
        df = scrape_price_archive(symbol, from_date, to_date)
        
        if df is not None and len(df) > 0:
            all_data.append(df)
            logger.debug(f"Received {len(df)} rows for {symbol}")
            success_count += 1
        else:
            logger.warning(f"No data returned for {symbol}")
            failed_symbols.append(symbol)
        
        # Be polite to the server
        time.sleep(0.5)
    
    # Combine all data
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        
        # Save to CSV
        output_file = 'lankabd_price_archive_3years.csv'
        combined_df.to_csv(output_file, index=False)
        
        logger.info("\n" + "="*60)
        logger.info(f"Price data successfully saved to {output_file}")
        logger.info("="*60)
        logger.info(f"Total symbols with data: {success_count}/{len(symbols)}")
        logger.info(f"Total rows: {len(combined_df)}")
        logger.debug(f"Columns: {list(combined_df.columns)}")
        
        if failed_symbols:
            logger.warning(f"\nSymbols with no data ({len(failed_symbols)}): {', '.join(failed_symbols[:10])}" + (f", ... and {len(failed_symbols)-10} more" if len(failed_symbols) > 10 else ""))
        
        return combined_df
    else:
        print("\nNo data collected from any symbol")
        return None


def scrape_price_archive_by_symbol(symbol, from_date=None, to_date=None):
    """Scrape price data for a specific symbol"""
    
    if from_date is None or to_date is None:
        from_date, to_date = get_date_range(years=3)
    
    logger.info(f"Fetching price data for {symbol}")
    logger.info(f"Date range: {from_date} to {to_date}\n")
    
    df = scrape_price_archive(symbol, from_date, to_date)
    
    if df is not None and len(df) > 0:
        output_file = f'lankabd_price_{symbol}_{from_date}_to_{to_date}.csv'
        df.to_csv(output_file, index=False)
        logger.info(f"Data saved to {output_file}")
        logger.info(f"Total rows: {len(df)}")
        return df
    else:
        logger.warning(f"No price data found for {symbol}")
        return None


def scrape_price_archive_by_sector(sector=None, from_date=None, to_date=None):
    """Scrape price data for all symbols in a specific sector"""
    
    if from_date is None or to_date is None:
        from_date, to_date = get_date_range(years=3)
    
    # Load sector data
    try:
        df = pd.read_csv('lankabd_data_all_sectors.csv')
    except FileNotFoundError:
        logger.error("lankabd_data_all_sectors.csv not found. Please run main.py first.")
        return None
    
    # Get symbols for the sector
    if sector:
        sector_data = df[df['Sector'] == sector]
        symbols = sector_data['Symbol'].unique()
        logger.info(f"Found {len(symbols)} symbols in {sector} sector")
    else:
        symbols = df['Symbol'].unique()
        logger.info(f"Found {len(symbols)} total symbols")
    
    if len(symbols) == 0:
        logger.error(f"No symbols found for sector: {sector}")
        return None
    
    logger.info(f"\nFetching price data for sector {sector or 'all'}")
    logger.info(f"Date range: {from_date} to {to_date}")
    logger.info(f"Total symbols: {len(symbols)}\n")
    
    all_data = []
    success_count = 0
    
    for idx, symbol in enumerate(symbols, 1):
        logger.info(f"[{idx}/{len(symbols)}] Fetching {symbol}...")
        
        price_df = scrape_price_archive(symbol, from_date, to_date)
        
        if price_df is not None and len(price_df) > 0:
            all_data.append(price_df)
            logger.debug(f"Received {len(price_df)} rows for {symbol}")
            success_count += 1
        else:
            logger.warning(f"No price records for {symbol}")
        
        time.sleep(0.5)
    
    # Combine and save
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        
        sector_name = sector.replace(" ", "_") if sector else "all_sectors"
        output_file = f'lankabd_price_{sector_name}_3years.csv'
        combined_df.to_csv(output_file, index=False)
        
        logger.info("\n" + "="*60)
        logger.info(f"Price data saved to {output_file}")
        logger.info("="*60)
        logger.info(f"Total symbols with data: {success_count}/{len(symbols)}")
        logger.info(f"Total rows: {len(combined_df)}")
        
        return combined_df
    else:
        logger.warning("No price data collected")
        return None


# ── Dagster Ops ───────────────────────────────────────────────────────────────
# Thin wrappers around the existing plain-Python functions above.
# Existing functions are NOT modified so they remain directly callable/testable.

@op(name="price_archive_fetch_all_symbols")
def price_archive_fetch_all_op(context: OpExecutionContext):
    """Dagster op: scrape price archive for all symbols over the last 3 years."""
    from_date, to_date = get_date_range(years=3)
    context.log.info(f"Fetching price data for all symbols: {from_date} → {to_date}")
    return scrape_all_symbols_price_data(from_date, to_date)


@op(name="price_archive_fetch_by_sector")
def price_archive_by_sector_op(context: OpExecutionContext):
    """Dagster op: scrape price archive grouped by sector over the last 3 years."""
    from_date, to_date = get_date_range(years=3)
    context.log.info(f"Fetching sector price data: {from_date} → {to_date}")
    return scrape_price_archive_by_sector(from_date=from_date, to_date=to_date)


# ── Dagster Jobs ──────────────────────────────────────────────────────────────

@job(name="price_archive_job")
def price_archive_job():
    """Default job: fetch price archive for all symbols."""
    price_archive_fetch_all_op()


@job(name="price_archive_by_sector_job")
def price_archive_by_sector_job():
    """Job: fetch price archive grouped by sector."""
    price_archive_by_sector_op()


if __name__ == "__main__":
    # Runs the job locally without a running Dagster server.
    # Use `dagster dev` to launch the UI with full scheduling support.
    price_archive_job.execute_in_process()
