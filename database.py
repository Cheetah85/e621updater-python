"""
e621 tagger - database updater

Author: AyoKeito
Version: 1.4.1
GitHub: https://github.com/AyoKeito/e621updater-python
"""

import re
import asyncio
import aiohttp
import gzip
import pandas as pds
import argparse
import io
import os
import zipfile
import traceback
import time
import datetime as dt
import sys
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn, TimeElapsedColumn, DownloadColumn, TransferSpeedColumn
from rich.console import Console

parser = argparse.ArgumentParser(description="Download and process CSV files. Download gz archives, extract & filter irrelevant data, and save as compressed parquet files.")
parser.add_argument("--proxy", help="The proxy to use for all network calls (optional). Usage examples: http://proxy.server:8888 or http://user:password@proxy.server:8888")
parser.add_argument("-m", "--multithreaded", action="store_true", help="[LEGACY] Use Modin RAY engine for multithreaded operations. Note: Single-threaded mode is now faster.")
args = parser.parse_args()

# Check for proxy.txt file if no proxy argument provided
if not args.proxy and os.path.exists("proxy.txt"):
    try:
        with open("proxy.txt", "r", encoding="utf-8") as f:
            proxy_from_file = f.read().strip()
            if proxy_from_file:
                args.proxy = proxy_from_file
                print(f"Using proxy from proxy.txt: {args.proxy}")
    except Exception as e:
        print(f"Warning: Could not read proxy.txt: {e}")

if args.multithreaded:
    import ray
    import modin.pandas as pd
    import modin

# Try to use Polars for better performance, fall back to pandas if not available
try:
    import polars as pl
    use_polars = True
    print("Using Polars for optimized performance")
except ImportError:
    use_polars = False
    print("Polars not available, using pandas")  

def check_database_update(web_date_str):
    """Check if local database is up-to-date compared to web date (ISO 8601 format)"""
    if os.path.exists("artists.parquet"):
        modification_time = os.path.getmtime("artists.parquet")
        # Create offset-aware datetime in UTC from file modification time
        modification_datetime = dt.datetime.fromtimestamp(modification_time, tz=dt.timezone.utc)
        
        # Parse ISO 8601 format from JSON endpoint (e.g., "2026-06-13T21:04:22.055+02:00")
        try:
            web_datetime = dt.datetime.fromisoformat(web_date_str.replace('Z', '+00:00'))
            # Convert to UTC for comparison
            web_datetime = web_datetime.astimezone(dt.timezone.utc)
        except Exception as e:
            print(f"Warning: Could not parse web date '{web_date_str}': {e}")
            return False

        print(f"Local posts database date: \033[96m{modification_datetime.strftime('%d-%b-%Y %H:%M')}\033[0m")

        time_difference = modification_datetime - web_datetime
        if modification_datetime >= web_datetime:
            print(f"Database is up-to-date.")
            return True
        else:
            print(f"Database is outdated by \033[1m{abs(time_difference.days)}\033[0m days.")
            return False

    return False

async def get_db_exports_metadata(session):
    """Fetch metadata from e621's official JSON endpoint"""
    try:
        async with session.get(
            "https://e621.net/db_exports.json",
            headers={'User-Agent': 'e621 tagger'},
            proxy=args.proxy,
            timeout=aiohttp.ClientTimeout(total=10)
        ) as resp:
            if resp.status == 200:
                exports = await resp.json()
                # Extract posts and tags metadata
                posts_meta = next((e for e in exports if e['name'] == 'posts'), None)
                tags_meta = next((e for e in exports if e['name'] == 'tags'), None)
                
                if posts_meta and tags_meta:
                    return posts_meta, tags_meta
                else:
                    print("Error: Could not find posts or tags in metadata")
                    return None, None
            else:
                print(f"Error: Failed to fetch metadata (status {resp.status})")
                return None, None
    except asyncio.TimeoutError:
        print(f"Error: Timeout while fetching metadata from e621.net/db_exports.json")
        return None, None
    except Exception as e:
        print(f"Error fetching metadata: {e}")
        return None, None

async def download_file(session, url, destination=None, progress_bar=None, task_id=None, description="Downloading"):
    try:
        async with session.get(url, headers={'User-Agent': 'e621 tagger'}, proxy=args.proxy, timeout=aiohttp.ClientTimeout(total=None)) as resp:
            if resp.status == 200:
                total_size = int(resp.headers.get('content-length', 0))
                content = bytearray()

                # Use provided progress bar or create a simple one
                if progress_bar and task_id is not None:
                    # Update the existing task with total size and description
                    progress_bar.update(task_id, total=total_size, description=description)

                    async for chunk in resp.content.iter_any():
                        content.extend(chunk)
                        chunk_size = len(chunk)
                        progress_bar.update(task_id, advance=chunk_size)
                else:
                    # Fallback: create simple progress bar for standalone downloads
                    progress = Progress(
                        TextColumn("[bold blue]Downloading", justify="right"),
                        BarColumn(bar_width=40),
                        "[progress.percentage]{task.percentage:>3.1f}%",
                        "•",
                        DownloadColumn(),
                        "•",
                        TransferSpeedColumn(),
                        console=Console(),
                        transient=False
                    )

                    with progress:
                        if total_size > 0:
                            task = progress.add_task("download", total=total_size)
                        else:
                            task = progress.add_task("download", total=None)

                        async for chunk in resp.content.iter_any():
                            content.extend(chunk)
                            chunk_size = len(chunk)
                            progress.update(task, advance=chunk_size)

                if destination:
                    with open(destination, 'wb') as f:
                        f.write(content)

                return content
            else:
                print(f"Error: Download failed with status {resp.status} for {url}")
                return None
    except asyncio.TimeoutError:
        print(f"Error: Download timeout for {url}")
        return None
    except Exception as e:
        print(f"Error downloading {url}: {e}")
        traceback.print_exc()
        return None
            
async def download_exiftool(session):
    exiftool_url = "https://sourceforge.net/projects/exiftool/files/latest/download"
    if not os.path.exists("exiftool.exe"):
        print(f"Downloading ExifTool from {exiftool_url}")
        exiftool_content = await download_file(session, exiftool_url, destination="exiftool.zip")

        if exiftool_content:
            print("Extracting ExifTool executable")
            try:
                with zipfile.ZipFile(io.BytesIO(exiftool_content), 'r') as zip_ref:
                    # Find exiftool(-k).exe in the versioned folder
                    exiftool_path = None
                    version_dir = None
                    for name in zip_ref.namelist():
                        if name.endswith('exiftool(-k).exe'):
                            exiftool_path = name
                            version_dir = name.split('/')[0]
                            break

                    if exiftool_path and version_dir:
                        # Extract the entire contents to preserve dependencies
                        zip_ref.extractall()
                        # Move exiftool(-k).exe to working directory and rename
                        os.rename(exiftool_path, "exiftool.exe")
                        # Move exiftool_files directory to working directory (needed for dependencies)
                        import shutil
                        exiftool_files_path = f"{version_dir}/exiftool_files"
                        if os.path.exists(exiftool_files_path):
                            if os.path.exists("exiftool_files"):
                                shutil.rmtree("exiftool_files")
                            shutil.move(exiftool_files_path, "exiftool_files")
                        # Clean up the extracted version directory
                        if os.path.exists(version_dir):
                            shutil.rmtree(version_dir)
                    else:
                        print("Error: Could not find exiftool(-k).exe in the archive")
            except zipfile.BadZipFile:
                print("Error: Failed to extract ExifTool - archive may be corrupted")
            except Exception as e:
                print(f"Error: Failed to extract ExifTool: {e}")

            if os.path.exists('exiftool.zip'):
                os.remove('exiftool.zip')
        else:
            print("Error: Failed to download ExifTool")
    else:
        print("ExifTool already exists. Skipping download.")

async def main(proxy, use_multithreaded=False):
    try:
        async with aiohttp.ClientSession() as session:
            print(f"\033[1mStep 1:\033[0m Fetching database exports metadata from \033[96me621.net\033[0m")
            
            # Get metadata from official JSON endpoint
            posts_meta, tags_meta = await get_db_exports_metadata(session)
            
            if posts_meta is None or tags_meta is None:
                print("Error: Could not fetch database metadata. Aborting.")
                return
            
            # Extract data from metadata
            posts_url = posts_meta['url']
            posts_size = posts_meta['file_size']
            posts_date = posts_meta['updated_at']
            posts_file = posts_meta['file_name']
            
            tags_url = tags_meta['url']
            tags_size = tags_meta['file_size']
            tags_date = tags_meta['updated_at']
            tags_file = tags_meta['file_name']
            
            # Display file information
            posts_size_mb = posts_size / (1024 ** 2)
            tags_size_mb = tags_size / (1024 ** 2)
            
            print(f"\033[1mStep 2:\033[0m Database files info")
            print(f"Latest posts file: \033[96m{posts_file}\033[0m")
            print(f"Filesize: \033[96m{posts_size_mb:.2f} MB\033[0m")
            print(f"Last modified: \033[96m{posts_date}\033[0m")
            
            print(f"\nLatest tags file: \033[96m{tags_file}\033[0m")
            print(f"Filesize: \033[96m{tags_size_mb:.2f} MB\033[0m")
            print(f"Last modified: \033[96m{tags_date}\033[0m")
            
            # Check if the database is up-to-date
            database_updated = check_database_update(posts_date)
                
            if not os.path.exists("artists.parquet"):
                # If the file doesn't exist, update unconditionally
                print("\nDownloading the database since the file doesn't exist.")
                update_choice = 'y'
            elif not database_updated:
                # If the file exists but is outdated, prompt the user
                update_choice = input("\n\033[1mThe local database is outdated. Do you want to update? (Y/N):\033[0m ").lower().strip()
                if update_choice not in ['y', 'yes', 'n', 'no']:
                    print("Invalid input, defaulting to 'no'")
                    update_choice = 'n'
            else:
                # If the file exists and is up-to-date, skip the update
                print("\nRecent database, skipping downloads.")
                return

            # Check if user wants to proceed with the update
            if update_choice in ['n', 'no']:
                print("Database update skipped by user choice.")
                return

            # Download exiftool before processing
            await download_exiftool(session)

            # Continue with the update process
            try:
                # Create unified progress bar for all operations
                main_progress = Progress(
                    TextColumn("[bold cyan]{task.description}", justify="right"),
                    BarColumn(bar_width=40),
                    "[progress.percentage]{task.percentage:>3.1f}%",
                    "•",
                    DownloadColumn(),
                    "•",
                    TransferSpeedColumn(),
                    "•",
                    TimeRemainingColumn(),
                    console=Console(),
                    transient=False
                )

                with main_progress:
                    print(f"\n\033[1mStep 3:\033[0m Downloading posts database...")
                    start_time = time.time()

                    # Create download task
                    download_task = main_progress.add_task("Downloading posts database...", total=posts_size)
                    posts_content = await download_file(session, posts_url,
                                                       progress_bar=main_progress, task_id=download_task)

                    if posts_content is None:
                        print("Error: Failed to download posts database. Aborting.")
                        return

                    end_time = time.time()
                    time_taken = end_time - start_time
                    print(f"Downloaded {posts_file} in {time_taken:.2f} seconds.")

                    # Decompress the content
                    try:
                        posts_content = gzip.decompress(posts_content)
                    except Exception as e:
                        print(f"Error: Failed to decompress posts file: {e}")
                        return

                    if use_multithreaded:
                        with open('latest_posts.csv', 'wb') as f:
                            f.write(posts_content)
                        del posts_content
                        print(f"Processing in \033[92mmultithreaded\033[0m mode, \033[92m{modin.config.NPartitions.get()}\033[0m threads detected, initializing Modin RAY engine...")
                        ray.init()
                    else:
                        print(f"Processing in \033[93mfast\033[0m mode...")

                    print(f"\033[1mStep 4:\033[0m Reading extracted posts CSV as a DataFrame")
                    try:
                        if use_polars and not use_multithreaded:
                            # Use Polars for optimal performance (6x faster)
                            posts_df = pl.read_csv(io.BytesIO(posts_content), columns=["id", "md5", "tag_string"])
                            del posts_content
                        elif use_multithreaded:
                            posts_df = pd.read_csv('latest_posts.csv', usecols=["id", "md5", "tag_string"])
                        else:
                            posts_df = pds.read_csv(io.BytesIO(posts_content), usecols=["id", "md5", "tag_string"])
                            del posts_content
                    except Exception as e:
                        print(f"Error: Failed to read posts CSV: {e}")
                        return

                    print(f"\033[1mStep 5:\033[0m Saving DataFrame to posts.parquet")
                    try:
                        if use_multithreaded:
                            os.remove('latest_posts.csv')  # Delete the temporary file
                            posts_df.to_parquet("posts.parquet", engine='pyarrow', compression='zstd')
                            ray.shutdown()
                        elif use_polars:
                            posts_df.write_parquet("posts.parquet", compression="zstd")
                        else:
                            posts_df.to_parquet("posts.parquet", engine='pyarrow', compression='zstd')
                    except Exception as e:
                        print(f"Error: Failed to save posts.parquet: {e}")
                        return

                    del posts_df
                    print(f"\033[32mStep 6:\033[0m posts.parquet done!\033[0m")
                    
                    # Download tags
                    print(f"\n\033[1mStep 7:\033[0m Downloading tags database...")
                    tags_download_task = main_progress.add_task("Downloading tags database...", total=tags_size)
                    tags_content = await download_file(session, tags_url,
                                                      progress_bar=main_progress, task_id=tags_download_task)

                    if tags_content is None:
                        print("Error: Failed to download tags database. Aborting.")
                        return

                    # Decompress the content
                    try:
                        tags_content = gzip.decompress(tags_content).decode()
                    except Exception as e:
                        print(f"Error: Failed to decompress tags file: {e}")
                        return

                    print(f"\033[1mStep 8:\033[0m Reading tags CSV as a DataFrame")
                    try:
                        if use_polars:
                            # Use Polars for faster processing
                            df = pl.read_csv(io.BytesIO(bytes(tags_content, "utf-8")),
                                           schema_overrides={"id": pl.Int64, "name": pl.Utf8, "category": pl.Int64, "post_count": pl.Int64})

                            print(f"\033[1mStep 9:\033[0m Filtering DataFrame to only include rows where category is equal to 1 (artists)")
                            print(f"\033[1mStep 10:\033[0m Keeping only the 'name' column from the DataFrame")
                            # Filter and select in one operation with Polars
                            df = df.filter(pl.col("category") == 1).select("name")
                        else:
                            df = pds.read_csv(io.BytesIO(bytes(tags_content, "utf-8")), header=0, dtype={"id": int, "name": str, "category": int, "post_count": int})

                            print(f"\033[1mStep 9:\033[0m Filtering DataFrame to only include rows where category is equal to 1 (artists)")
                            df = df[df["category"] == 1]

                            print(f"\033[1mStep 10:\033[0m Keeping only the 'name' column from the DataFrame")
                            df = df[["name"]]
                    except Exception as e:
                        print(f"Error: Failed to read tags CSV: {e}")
                        return

                    print(f"\033[1mStep 11:\033[0m Saving DataFrame to artists.parquet")
                    try:
                        if os.path.exists('artists.parquet'):
                            os.remove('artists.parquet')

                        if use_polars:
                            df.write_parquet("artists.parquet", compression="zstd")
                        else:
                            df.to_parquet("artists.parquet", engine='pyarrow', compression='zstd')
                    except Exception as e:
                        print(f"Error: Failed to save artists.parquet: {e}")
                        return

                    print(f"\033[32mStep 12:\033[0m artists.parquet done!\033[0m")
                    del df
                    
            except Exception as e:
                print(f"Error: An error occurred during processing: {e}")
                traceback.print_exc()
                return
                
    except Exception as e:
        print(f"Error: A network error occurred: {e}")
        if not args.proxy:
            print("Try to restart the script with the --proxy argument if you're behind a proxy.")
        traceback.print_exc()

# Usage:
try:
    asyncio.run(main(proxy=args.proxy, use_multithreaded=args.multithreaded))
except KeyboardInterrupt:
    print("\nScript interrupted by user.")
except Exception as e:
    print(f"An unexpected error occurred: {e}")
    traceback.print_exc()
