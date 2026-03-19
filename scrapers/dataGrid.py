import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import json

# Dagster
from dagster import op, job, OpExecutionContext, asset

# logging utility
from utils.logger import Log
from datetime import datetime as _dt

# module logger
log_filename = f"logs/main_{_dt.now().strftime('%Y%m%d_%H%M%S')}.log"
logger = Log(name="main", filename=log_filename)

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
    session = requests.Session()
    # Add retry strategy
    retry_strategy = requests.adapters.Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504]
    )
    adapter = requests.adapters.HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def get_available_sectors():
    """Fetch all available sectors from the DataMatrix page"""
    try:
        session = get_session()
        headers = HEADERS.copy()
        headers['Referer'] = 'https://lankabd.com/'
        
        logger.info("Fetching available sectors...")
        response = session.get("https://lankabd.com/", headers=headers, timeout=30)
        response = session.get("https://lankabd.com/Home/DataMatrix", headers=headers, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'lxml')
        select = soup.find('select', {'id': 'sectorddl'})
        
        sectors = []
        if select:
            options = select.find_all('option')
            for option in options:
                value = option.get('value', '').strip()
                text = option.text.strip()
                if value and value != '--Select--':
                    sectors.append(value)
        
        logger.info(f"Found {len(sectors)} sectors")
        return sectors
    except Exception as e:
        logger.error(f"Error fetching sectors: {e}")
        return []

def scrape_lankabd(sector=None):
    url = "https://lankabd.com/Home/DataMatrix"
    
    try:
        logger.info(f"Fetching data from {url}...")
        
        # First, get the main page to set cookies
        session = get_session()
        
        # Add more headers to mimic a real browser
        headers = HEADERS.copy()
        headers.update({
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Sec-Ch-Ua': '"Not.A/Brand";v="8", "Chromium";v="114"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': '"macOS"',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1',
            'Referer': 'https://lankabd.com/',
            'Dnt': '1',
        })
        
        # First request to get cookies
        logger.debug("Making initial request to get cookies...")
        response = session.get("https://lankabd.com/", headers=headers, timeout=30)
        response.raise_for_status()
        
        # Now try to access the DataMatrix page
        logger.debug("Accessing DataMatrix page...")
        response = session.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        logger.info("Successfully fetched the page")
        
        # Parse the HTML content
        soup = BeautifulSoup(response.text, 'lxml')
        
        # Find the table with ID 'TableDataMatrix'
        table = soup.find('table', {'id': 'TableDataMatrix'})
        
        if not table:
            logger.warning("No data table found on the page")
            return None
            
        # Extract table headers
        headers_list = [th.text.strip() for th in table.find('thead').find_all('th')]
        
        # Extract table rows
        data = []
        rows = table.find('tbody').find_all('tr')
        
        for row in rows:
            cols = row.find_all('td')
            data.append([col.text.strip() for col in cols])
        
        # Create a DataFrame
        df = pd.DataFrame(data, columns=headers_list)
        
        # Apply sector filter if specified
        if sector:
            if 'Sector' in df.columns:
                df = df[df['Sector'] == sector]
                logger.info(f"Filtered data for sector: {sector} ({len(df)} rows)")
            else:
                logger.warning("'Sector' column not found in data")
        
        return df
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching the page: {e}")
        return None
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        return None

def scrape_all_sectors():
    """Fetch and save data for all sectors"""
    sectors = get_available_sectors()
    
    if not sectors:
        logger.error("No sectors found")
        return None
    
    all_data = []
    
    for sector in sectors:
        logger.info(f"\n--- Fetching data for sector: {sector} ---")
        df = scrape_lankabd(sector=sector)
        
        if df is not None and len(df) > 0:
            all_data.append(df)
            logger.info(f"Successfully fetched {len(df)} rows")
        else:
            logger.warning(f"No data for sector: {sector}")
        
        time.sleep(1)  # Be polite to the server
    
    # Combine all data
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        output_file = 'lankabd_data_all_sectors.csv'
        combined_df.to_csv(output_file, index=False)
        logger.info(f"\n✓ Combined data saved to {output_file}")
        logger.info(f"Total rows: {len(combined_df)}")
        return combined_df
    else:
        logger.warning("\nNo data collected from any sector")
        return None

# ── Dagster Assets ────────────────────────────────────────────────────────────

@asset(group_name="datagrid", compute_kind="python")
def available_sectors(context: OpExecutionContext) -> list:
    """Asset: The list of available sector names from LankaBD."""
    sectors = get_available_sectors()
    context.add_output_metadata({"num_sectors": len(sectors)})
    return sectors


@asset(group_name="datagrid", compute_kind="pandas")
def lankabd_data_all_sectors(context: OpExecutionContext, available_sectors: list) -> pd.DataFrame:
    """
    Asset: Scraped DataMatrix for all sectors.
    Depends on: available_sectors
    """
    context.log.info(f"Starting scrape for {len(available_sectors)} sectors...")
    
    all_data = []
    for sector in available_sectors:
        df = scrape_lankabd(sector=sector)
        if df is not None and not df.empty:
            all_data.append(df)
        time.sleep(0.5)
        
    if not all_data:
        raise ValueError("No data collected from any sector")
        
    combined_df = pd.concat(all_data, ignore_index=True)
    
    # Save precisely as requested in the script logic
    output_file = 'lankabd_data_all_sectors.csv'
    combined_df.to_csv(output_file, index=False)
    
    # Metadata for Dagster UI
    context.add_output_metadata({
        "total_rows": len(combined_df),
        "file_path": output_file,
        "preview": combined_df.head().to_string()
    })
    
    return combined_df


if __name__ == "__main__":
    # Local execution support
    scrape_all_sectors()
    # Local execution support
    scrape_all_sectors()