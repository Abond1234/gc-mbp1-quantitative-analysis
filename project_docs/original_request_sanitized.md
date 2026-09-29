# Original request notebook (sanitized archival transcript)

Inspected only; never executed during migration. Outputs and metadata were omitted.
Credential/client lines are redacted. This is historical documentation, not a runnable pipeline.
The source notebook remains in Project 1. Its original output path is historical.

## Original cell 1

```text
import databento as db

from datetime import datetime, date, time, timedelta
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import time as tm


# ============================================================
# SETTINGS
# ============================================================

[REDACTED CREDENTIAL/CLIENT LINE]

DATASET = "GLBX.MDP3"
SYMBOL = "GC.v.0"
STYPE_IN = "continuous"

START_DATE = date(2021, 9, 27)
END_DATE   = date(2026, 9, 26)

NY = ZoneInfo("America/New_York")

# Conservative vs Databento's 20 metadata requests/sec limit.
MAX_WORKERS = 8


# ============================================================
# ONE CLIENT PER THREAD
# ============================================================

thread_local = threading.local()


def get_client():
    if not hasattr(thread_local, "client"):
[REDACTED CREDENTIAL/CLIENT LINE]

    return thread_local.client


# ============================================================
# COST REQUEST WITH RETRIES
# ============================================================

def get_cost(schema, start, end, retries=5):

    client = get_client()

    for attempt in range(retries):

        try:
            return client.metadata.get_cost(
                dataset=DATASET,
                symbols=SYMBOL,
                stype_in=STYPE_IN,
                schema=schema,
                start=start,
                end=end,
            )

        except Exception as e:

            if attempt == retries - 1:
                raise

            # Small backoff if Databento temporarily rate-limits
            # or a network request fails.
            tm.sleep(1.5 * (attempt + 1))


# ============================================================
# CHECK ONE TRADING DAY
# ============================================================

def check_day(day):

    start = datetime.combine(
        day,
        time(7, 0),
        tzinfo=NY
    )

    end = datetime.combine(
        day,
        time(12, 0),
        tzinfo=NY
    )

    tbbo_cost = get_cost(
        schema="tbbo",
        start=start,
        end=end
    )

    mbp1_cost = get_cost(
        schema="mbp-1",
        start=start,
        end=end
    )

    return day, tbbo_cost, mbp1_cost


# ============================================================
# BUILD WEEKDAY LIST
# ============================================================

days = []

day = START_DATE

while day <= END_DATE:

    # Monday-Friday only
    if day.weekday() < 5:
        days.append(day)

    day += timedelta(days=1)


print(f"Checking {len(days)} weekdays...")
print("Estimating TBBO + MBP-1 costs concurrently...\n")


# ============================================================
# PARALLEL COST ESTIMATION
# ============================================================

tbbo_total = 0.0
mbp1_total = 0.0

completed = 0


with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

    futures = {
        executor.submit(check_day, day): day
        for day in days
    }

    for future in as_completed(futures):

        day, tbbo_cost, mbp1_cost = future.result()

        tbbo_total += tbbo_cost
        mbp1_total += mbp1_cost

        completed += 1

        if completed % 100 == 0:
            print(
                f"{completed}/{len(days)} days checked..."
            )


# ============================================================
# FINAL OUTPUT
# ============================================================

print("\n" + "=" * 55)
print("GC.v.0 | 07:00\u201312:00 NEW YORK | 2021-09-27 \u2192 2026-09-26")
print("=" * 55)

print(f"\nTBBO estimated cost : ${tbbo_total:,.2f}")
print(f"MBP-1 estimated cost: ${mbp1_total:,.2f}")
```

## Original cell 2

```text
import databento as db

from datetime import datetime, date, time, timedelta, timezone
from zoneinfo import ZoneInfo
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading


# ============================================================
# CONFIGURATION
# ============================================================

[REDACTED CREDENTIAL/CLIENT LINE]
try:
[REDACTED CREDENTIAL/CLIENT LINE]
except NameError:
    raise RuntimeError(
[REDACTED CREDENTIAL/CLIENT LINE]
        "from your Databento pricing cell."
    )


DATASET = "GLBX.MDP3"
SCHEMA = "mbp-1"

SYMBOL = "GC.v.0"
STYPE_IN = "continuous"

# EXACT SAME RANGE USED IN OUR COST ESTIMATE
START_DATE = date(2021, 9, 27)
END_DATE   = date(2026, 9, 26)

# EXACT RESEARCH WINDOW
NY = ZoneInfo("America/New_York")
SESSION_START = time(7, 0)
SESSION_END   = time(12, 0)

# Estimated from the pricing cell we already ran.
EXPECTED_COST = 105.48

# Number of simultaneous Databento streams.
# Databento permits far more, but 8 is deliberately conservative.
MAX_WORKERS = 8


# ============================================================
# OUTPUT DIRECTORY
# ============================================================

OUTPUT_DIR = Path(
    r"C:\Users\abond\Desktop\WORK FILES\Systemic\Project 1"
    r"\data\raw\DB MBP-1 DATA"
)

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# THREAD-LOCAL DATABENTO CLIENTS
# ============================================================

thread_local = threading.local()


def get_client():
    """
[REDACTED CREDENTIAL/CLIENT LINE]
    """
    if not hasattr(thread_local, "client"):
[REDACTED CREDENTIAL/CLIENT LINE]

    return thread_local.client


# ============================================================
# BUILD DATE LIST
# ============================================================

days = []

day = START_DATE

while day <= END_DATE:

    # Monday-Friday only.
    # Exchange holidays may return empty/minimal files, which is fine.
    if day.weekday() < 5:
        days.append(day)

    day += timedelta(days=1)


# ============================================================
# DOWNLOAD ONE DAY
# ============================================================

def download_day(day):

    # --------------------------------------------------------
    # Create yearly subfolder
    # --------------------------------------------------------

    year_dir = OUTPUT_DIR / str(day.year)
    year_dir.mkdir(parents=True, exist_ok=True)


    # --------------------------------------------------------
    # File names
    # --------------------------------------------------------

    filename = (
        f"GC.v.0_{day.isoformat()}_"
        f"0700-1200_NY_mbp-1.dbn.zst"
    )

    final_path = year_dir / filename

    # Marker proves that Databento returned successfully.
    done_path = Path(str(final_path) + ".done")


    # --------------------------------------------------------
    # RESTART SAFETY
    # --------------------------------------------------------

    # Completed previously -> NEVER request again.
    if final_path.exists() and done_path.exists():
        return {
            "day": day,
            "status": "skipped",
            "size": final_path.stat().st_size,
        }

    # File exists but no .done marker.
    #
    # This could mean a previous request was interrupted.
    # DO NOT automatically download it again because Databento
    # may already have billed the bytes that were transmitted.
    if final_path.exists() and not done_path.exists():
        return {
            "day": day,
            "status": "partial",
            "size": final_path.stat().st_size,
        }


    # --------------------------------------------------------
    # BUILD 07:00 -> 12:00 NEW YORK WINDOW
    # --------------------------------------------------------

    start_ny = datetime.combine(
        day,
        SESSION_START,
        tzinfo=NY,
    )

    end_ny = datetime.combine(
        day,
        SESSION_END,
        tzinfo=NY,
    )

    # Explicit conversion makes the requested interval
    # unambiguous to Databento and automatically handles DST.
    start_utc = start_ny.astimezone(timezone.utc)
    end_utc   = end_ny.astimezone(timezone.utc)


    # --------------------------------------------------------
    # STREAM DIRECTLY TO DISK
    # --------------------------------------------------------

    client = get_client()

    try:

        client.timeseries.get_range(
            dataset=DATASET,
            schema=SCHEMA,

            symbols=SYMBOL,
            stype_in=STYPE_IN,

            start=start_utc,
            end=end_utc,

            # Stream straight into compressed DBN.
            # Avoids loading the entire day into RAM.
            path=str(final_path),
        )


        # Only create this marker AFTER successful completion.
        done_path.write_text(
            f"COMPLETE\n"
            f"date={day.isoformat()}\n"
            f"window=07:00-12:00 America/New_York\n"
            f"schema=mbp-1\n"
            f"symbol=GC.v.0\n",
            encoding="utf-8",
        )


        return {
            "day": day,
            "status": "downloaded",
            "size": final_path.stat().st_size,
        }


    except Exception as e:

        # IMPORTANT:
        # We intentionally DO NOT retry automatically.
        #
        # A retry could cause already-transmitted bytes to be
        # billed again if the first request partially succeeded.

        return {
            "day": day,
            "status": "failed",
            "error": repr(e),
        }


# ============================================================
# CONFIRM PLAN BEFORE BILLABLE REQUESTS BEGIN
# ============================================================

print("=" * 70)
print("DATABENTO MBP-1 DOWNLOAD")
print("=" * 70)

print(f"Dataset       : {DATASET}")
print(f"Schema        : {SCHEMA}")
print(f"Symbol        : {SYMBOL}")
print(f"Symbology     : {STYPE_IN}")
print(f"Date range    : {START_DATE} -> {END_DATE}")
print("NY window     : 07:00 -> 12:00")
print(f"Weekdays      : {len(days)}")
print(f"Expected cost : ${EXPECTED_COST:.2f}")
print(f"Output        : {OUTPUT_DIR}")
print(f"Workers       : {MAX_WORKERS}")

print("\nSTARTING BILLABLE DOWNLOADS...\n")


# ============================================================
# PARALLEL DOWNLOAD
# ============================================================

downloaded = 0
skipped = 0
partials = []
failures = []

bytes_downloaded = 0
completed = 0


with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

    futures = {
        executor.submit(download_day, day): day
        for day in days
    }

    for future in as_completed(futures):

        result = future.result()

        completed += 1

        status = result["status"]

        if status == "downloaded":

            downloaded += 1
            bytes_downloaded += result["size"]

        elif status == "skipped":

            skipped += 1

        elif status == "partial":

            partials.append(result)

        elif status == "failed":

            failures.append(result)


        if completed % 50 == 0 or completed == len(days):

            downloaded_gb = bytes_downloaded / (1024 ** 3)

            print(
                f"{completed}/{len(days)} complete | "
                f"new={downloaded} | "
                f"skipped={skipped} | "
                f"partial={len(partials)} | "
                f"failed={len(failures)} | "
                f"disk={downloaded_gb:.2f} GB"
            )


# ============================================================
# FINAL REPORT
# ============================================================

print("\n" + "=" * 70)
print("DOWNLOAD FINISHED")
print("=" * 70)

print(f"Successfully downloaded : {downloaded}")
print(f"Already existed/skipped : {skipped}")
print(f"Partial files detected  : {len(partials)}")
print(f"Failed requests         : {len(failures)}")

total_disk_gb = bytes_downloaded / (1024 ** 3)

print(f"New compressed data     : {total_disk_gb:.2f} GB")
print(f"Location                : {OUTPUT_DIR}")


# ------------------------------------------------------------
# REPORT PARTIAL FILES
# ------------------------------------------------------------

if partials:

    print("\nPARTIAL FILES \u2014 NOT RE-DOWNLOADED:")

    for item in partials:
        print(
            f"  {item['day']} | "
            f"{item['size'] / (1024 ** 2):.2f} MB"
        )


# ------------------------------------------------------------
# REPORT FAILED REQUESTS
# ------------------------------------------------------------

if failures:

    print("\nFAILED REQUESTS:")

    for item in failures:
        print(
            f"  {item['day']} | "
            f"{item['error']}"
        )


if not partials and not failures:

    print("\nSUCCESS: all requested trading days were handled cleanly.")
```

## Original cell 3

```text
from pathlib import Path

OUTPUT_DIR = Path(
    r"C:\Users\abond\Desktop\WORK FILES\Systemic\Project 1"
    r"\data\raw\DB MBP-1 DATA"
)

failed_file = (
    OUTPUT_DIR
    / "2026"
    / "GC.v.0_2026-09-09_0700-1200_NY_mbp-1.dbn.zst"
)

done_file = Path(str(failed_file) + ".done")

print("Data file exists :", failed_file.exists())

if failed_file.exists():
    print(
        "File size        :",
        f"{failed_file.stat().st_size / (1024**2):.2f} MB"
    )

print("Done marker exists:", done_file.exists())
```

## Original cell 4

```text
DATASET = "GLBX.MDP3"
SCHEMA = "mbp-1"

SYMBOL = "GC.v.0"
STYPE_IN = "continuous"

DAY = date(2026, 9, 9)

NY = ZoneInfo("America/New_York")

OUTPUT_DIR = Path(
    r"C:\Users\abond\Desktop\WORK FILES\Systemic\Project 1"
    r"\data\raw\DB MBP-1 DATA"
)

year_dir = OUTPUT_DIR / "2026"

final_path = (
    year_dir /
    "GC.v.0_2026-09-09_0700-1200_NY_mbp-1.dbn.zst"
)

done_path = Path(str(final_path) + ".done")


# ============================================================
# BUILD EXACT SAME WINDOW
# ============================================================

start_ny = datetime.combine(
    DAY,
    time(7, 0),
    tzinfo=NY
)

end_ny = datetime.combine(
    DAY,
    time(12, 0),
    tzinfo=NY
)

start_utc = start_ny.astimezone(timezone.utc)
end_utc = end_ny.astimezone(timezone.utc)


# ============================================================
# CLIENT
# ============================================================

[REDACTED CREDENTIAL/CLIENT LINE]


# ============================================================
# CHECK COST FIRST \u2014 FREE METADATA REQUEST
# ============================================================

repair_cost = client.metadata.get_cost(
    dataset=DATASET,
    schema=SCHEMA,
    symbols=SYMBOL,
    stype_in=STYPE_IN,
    start=start_utc,
    end=end_utc,
)

print(f"Full-day replacement cost estimate: ${repair_cost:.4f}")


# ============================================================
# REMOVE THE INCOMPLETE FILE
# ============================================================

if final_path.exists():
    print(
        f"Removing incomplete file: "
        f"{final_path.stat().st_size / (1024**2):.2f} MB"
    )
    final_path.unlink()

if done_path.exists():
    done_path.unlink()


# ============================================================
# REDOWNLOAD ONLY 2026-09-09
# ============================================================

print("\nDownloading 2026-09-09...")

client.timeseries.get_range(
    dataset=DATASET,
    schema=SCHEMA,
    symbols=SYMBOL,
    stype_in=STYPE_IN,
    start=start_utc,
    end=end_utc,
    path=str(final_path),
)


# ============================================================
# MARK SUCCESS
# ============================================================

done_path.write_text(
    "COMPLETE\n"
    "date=2026-09-09\n"
    "window=07:00-12:00 America/New_York\n"
    "schema=mbp-1\n"
    "symbol=GC.v.0\n",
    encoding="utf-8",
)


# ============================================================
# VERIFY
# ============================================================

print("\nSUCCESS")
print(
    f"Final file size : "
    f"{final_path.stat().st_size / (1024**2):.2f} MB"
)
print(f"Done marker     : {done_path.exists()}")
```
