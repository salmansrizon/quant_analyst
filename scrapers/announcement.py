from bs4 import BeautifulSoup
import pandas as pd
import time
from datetime import datetime, timedelta
from urllib.parse import urlencode
import requests

# Dagster
from dagster import op, job, OpExecutionContext

# logging utility
from utils.logger import Log

# Create a module-level logger with timestamped file output in logs/ directory
log_filename = f"logs/announcement_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logger = Log(name="announcement", filename=log_filename)

# Browser headers to mimic a real browser and avoid being blocked by the server
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
    
    today = datetime.now()
    past_date = today - timedelta(days=365*years)
    return past_date.strftime('%Y-%m-%d'), today.strftime('%Y-%m-%d')


def get_symbols_from_sectors():
    try:
        df = pd.read_csv('lankabd_data_all_sectors.csv')
        symbols = df['Symbol'].unique()
        logger.info(f"Extracted {len(symbols)} unique symbols from lankabd_data_all_sectors.csv")
        return list(symbols)
    except FileNotFoundError:
        logger.error("lankabd_data_all_sectors.csv not found. Please run main.py first.")
        return [] 
    except (IOError, ValueError, pd.errors.ParserError) as e:
        logger.error(f"Error reading CSV: {e}")
        return []


def scrape_announcement(sn, fromdate, todate, page=None, page_size=None):
    try:
        logger.info(f"Starting scrape for sn={sn}, range={fromdate} to {todate}")
        session = get_session()

        headers = HEADERS.copy()
        headers.update({'Referer': 'https://lankabd.com/', 'Dnt': '1', 'X-Requested-With': 'XMLHttpRequest'})

        # 1. Initial GET to obtain cookies and CSRF token
        initial_url = "https://lankabd.com/Home/MarketAnnouncements?catName=Archive"
        logger.debug(f"Making initial request to {initial_url}")
        init_resp = session.get(initial_url, headers=headers, timeout=30)
        init_soup = BeautifulSoup(init_resp.text, 'lxml')
        
        token_input = init_soup.find('input', {'name': '__RequestVerificationToken'})
        token = token_input.get('value') if token_input else None

        # 2. Prepare POST payload
        payload = {
            'sn': sn,
            'fromdate': fromdate,
            'todate': todate,
            'catName': 'Archive'
        }
        
        if page is not None:
            payload['page'] = page
        if page_size is not None:
            payload['pageSize'] = page_size
            
        if token:
            payload['__RequestVerificationToken'] = token
            headers['RequestVerificationToken'] = token

        post_url = f"https://lankabd.com/Home/MarketAnnouncements?fromdate={fromdate}"
        logger.debug(f"Posting to: {post_url}")
        response = session.post(post_url, data=payload, headers=headers, timeout=30)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'lxml')

        # Newer page layout: announcements are rendered as list-group items (divs)
        items = soup.find_all('div', class_='list-group-item')
        data = []

        if items:
            for item in items:
                # Extract company/symbol name from the announcement link
                title_tag = item.find('a')
                title = title_tag.text.strip() if title_tag else ''
                link = title_tag.get('href') if title_tag and title_tag.get('href') else ''
                
                # Convert relative URLs (e.g., /Company/Overview?cid=123) to absolute URLs
                if link and not link.startswith('http'):
                    link = f"https://lankabd.com{link}"

                # Extract announcement date from span with specific styling
                date_tag = item.find('span', class_='small text-dark font-weight-bold')
                date = date_tag.text.strip() if date_tag else ''

                # Extract announcement text/description from paragraph element
                para = item.find('p')
                text = para.text.strip() if para else ''

                data.append({'Symbol': title, 'Date': date, 'Text': text, 'Link': link})

            df = pd.DataFrame(data)
            logger.info(f"Successfully parsed announcements for {sn} ({len(df)} rows)")
            return df

        # Fallback: try table parsing if no list items found (for older page layouts)
        # This handles legacy HTML table structures if the site used table-based layout previously
        table = soup.find('table')
        if table:
            # Extract column headers from thead if present
            thead = table.find('thead')
            if thead:
                headers_list = [th.text.strip() for th in thead.find_all('th')]
            else:
                headers_list = None

            # Extract table rows from tbody or tr elements
            tbody = table.find('tbody')
            rows = tbody.find_all('tr') if tbody else table.find_all('tr')

            if not rows:
                logger.warning(f"No rows found in announcement table for {sn}")
                return None

            # Parse each table row into a list of column values
            tab_data = []
            for row in rows:
                cols = row.find_all('td')
                if cols:
                    tab_data.append([col.text.strip() for col in cols])

            if not tab_data:
                logger.warning(f"Parsed table but no data rows for {sn}")
                return None

            # Create DataFrame with or without headers depending on availability
            if headers_list and len(headers_list) == len(tab_data[0]):
                df = pd.DataFrame(tab_data, columns=headers_list)
            else:
                df = pd.DataFrame(tab_data)

            # Add symbol column to match list-based layout output format
            df['Symbol'] = sn
            logger.info(f"Successfully parsed announcements table for {sn} ({len(df)} rows)")
            return df

        logger.warning(f"No announcements found for {sn}")
        return None

    except requests.exceptions.RequestException as e:
        # Handle network errors (connection timeout, invalid URL, HTTP errors, etc.)
        logger.error(f"Request error while fetching {sn}: {str(e)[:50]}")
        return None
    except (AttributeError, ValueError, KeyError) as e:
        # Handle HTML parsing errors (missing elements, type mismatches, etc.)
        logger.error(f"Error parsing response for {sn}: {str(e)[:50]}")
        return None


def scrape_all_symbols_announcements(fromdate=None, todate=None, page_size=None, fetch_all_pages=False, max_pages=None):
    if fromdate is None or todate is None:
        fromdate, todate = get_date_range(years=3)

    symbols = get_symbols_from_sectors()
    if not symbols:
        logger.error("No symbols found to scrape")
        return None

    logger.info(f"Fetching announcements for last 3 years: {fromdate} to {todate}")
    logger.info(f"Total symbols to process: {len(symbols)}")

    all_data = []
    success_count = 0
    failed_symbols = []

    for idx, symbol in enumerate(symbols, 1):
        logger.info(f"[{idx}/{len(symbols)}] Fetching announcements for {symbol}...")
        if fetch_all_pages:
            page_num = 1
            symbol_parts = []
            while True:
                if max_pages and page_num > max_pages:
                    logger.debug(f"Reached max_pages limit {max_pages} for {symbol}")
                    break
                df_page = scrape_announcement(symbol, fromdate, todate, page=page_num, page_size=page_size)
                if df_page is None or len(df_page) == 0:
                    break
                symbol_parts.append(df_page)
                logger.debug(f"Received {len(df_page)} rows for {symbol} page {page_num}")
                page_num += 1
                time.sleep(0.3)

            if symbol_parts:
                df = pd.concat(symbol_parts, ignore_index=True)
                all_data.append(df)
                success_count += 1
            else:
                logger.warning(f"No data returned for {symbol}")
                failed_symbols.append(symbol)
        else:
            df = scrape_announcement(symbol, fromdate, todate, page=1 if page_size else None, page_size=page_size)
            if df is not None and len(df) > 0:
                all_data.append(df)
                logger.debug(f"Received {len(df)} rows for {symbol}")
                success_count += 1
            else:
                logger.warning(f"No data returned for {symbol}")
                failed_symbols.append(symbol)

        time.sleep(0.5)

    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        output_file = 'lankabd_announcements_3years.csv'
        combined_df.to_csv(output_file, index=False)
        logger.info("\n" + "="*60)
        logger.info(f"Announcements data successfully saved to {output_file}")
        logger.info("="*60)
        logger.info(f"Total symbols with data: {success_count}/{len(symbols)}")
        logger.info(f"Total rows: {len(combined_df)}")
        if failed_symbols:
            logger.warning(f"\nSymbols with no data ({len(failed_symbols)}): {', '.join(failed_symbols[:10])}" + (f", ... and {len(failed_symbols)-10} more" if len(failed_symbols) > 10 else ""))
        return combined_df
    else:
        logger.warning("\nNo announcements collected from any symbol")
        return None


def scrape_announcement_by_symbol(sn, fromdate=None, todate=None, page=None, page_size=None):
    if fromdate is None or todate is None:
        fromdate, todate = get_date_range(years=3)

    logger.info(f"Fetching announcements for {sn}")
    logger.info(f"Date range: {fromdate} to {todate}\n")

    df = scrape_announcement(sn, fromdate, todate, page=page, page_size=page_size)
    if df is not None and len(df) > 0:
        output_file = f'lankabd_announcements_{sn}_{fromdate}_to_{todate}.csv'
        df.to_csv(output_file, index=False)
        logger.info(f"Data saved to {output_file}")
        logger.info(f"Total rows: {len(df)}")
        return df
    else:
        logger.warning(f"No announcement data found for {sn}")
        return None


def scrape_announcements_by_sector(sector=None, fromdate=None, todate=None, page_size=None, fetch_all_pages=False, max_pages=None):
    if fromdate is None or todate is None:
        fromdate, todate = get_date_range(years=3)

    try:
        df = pd.read_csv('lankabd_data_all_sectors.csv')
    except FileNotFoundError:
        logger.error("lankabd_data_all_sectors.csv not found. Please run main.py first.")
        return None

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

    logger.info(f"\nFetching announcements for sector {sector or 'all'}")
    logger.info(f"Date range: {fromdate} to {todate}")
    logger.info(f"Total symbols: {len(symbols)}\n")

    all_data = []
    success_count = 0

    for idx, symbol in enumerate(symbols, 1):
        logger.info(f"[{idx}/{len(symbols)}] Fetching {symbol}...")

        if fetch_all_pages:
            page_num = 1
            symbol_parts = []
            while True:
                if max_pages and page_num > max_pages:
                    logger.debug(f"Reached max_pages limit {max_pages} for {symbol}")
                    break
                df_page = scrape_announcement(symbol, fromdate, todate, page=page_num, page_size=page_size)
                if df_page is None or len(df_page) == 0:
                    break
                symbol_parts.append(df_page)
                logger.debug(f"Received {len(df_page)} rows for {symbol} page {page_num}")
                page_num += 1
                time.sleep(0.3)

            if symbol_parts:
                ann_df = pd.concat(symbol_parts, ignore_index=True)
                all_data.append(ann_df)
                success_count += 1
            else:
                logger.warning(f"No announcements for {symbol}")
        else:
            ann_df = scrape_announcement(symbol, fromdate, todate, page=1 if page_size else None, page_size=page_size)
            if ann_df is not None and len(ann_df) > 0:
                all_data.append(ann_df)
                logger.debug(f"Received {len(ann_df)} rows for {symbol}")
                success_count += 1
            else:
                logger.warning(f"No announcements for {symbol}")

        time.sleep(0.5)

    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        sector_name = sector.replace(" ", "_") if sector else "all_sectors"
        output_file = f'lankabd_announcements_{sector_name}_3years.csv'
        combined_df.to_csv(output_file, index=False)
        logger.info("\n" + "="*60)
        logger.info(f"Announcements data saved to {output_file}")
        logger.info("="*60)
        logger.info(f"Total symbols with data: {success_count}/{len(symbols)}")
        logger.info(f"Total rows: {len(combined_df)}")
        return combined_df
    else:
        logger.warning("No announcement data collected")
        return None


# ── Dagster Ops ───────────────────────────────────────────────────────────────
# Thin wrappers around the existing plain-Python functions above.
# Existing functions are NOT modified so they remain directly callable/testable.

@op(name="announcement_fetch_all_symbols")
def announcement_fetch_all_op(context: OpExecutionContext):
    """Dagster op: scrape announcements for all symbols over the last 3 years."""
    start_date, end_date = get_date_range(years=3)
    context.log.info(f"Fetching announcements for all symbols: {start_date} → {end_date}")
    return scrape_all_symbols_announcements(start_date, end_date)


@op(name="announcement_fetch_by_sector")
def announcement_by_sector_op(context: OpExecutionContext):
    """Dagster op: scrape announcements grouped by sector over the last 3 years."""
    start_date, end_date = get_date_range(years=3)
    context.log.info(f"Fetching sector announcements: {start_date} → {end_date}")
    return scrape_announcements_by_sector(fromdate=start_date, todate=end_date)


# ── Dagster Jobs ──────────────────────────────────────────────────────────────

@job(name="announcement_job")
def announcement_job():
    """Default job: scrape announcements for all symbols."""
    announcement_fetch_all_op()


@job(name="announcement_by_sector_job")
def announcement_by_sector_job():
    """Job: scrape announcements grouped by sector."""
    announcement_by_sector_op()


if __name__ == "__main__":
    # Runs the job locally without a running Dagster server.
    # Use `dagster dev` to launch the UI with full scheduling support.
    announcement_job.execute_in_process()