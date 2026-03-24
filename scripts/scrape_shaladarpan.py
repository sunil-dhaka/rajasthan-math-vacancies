"""
Scrape Rajasthan Shala Darpan for schools with vacant Lecturer (I Gr.) Mathematics positions.

Target districts: Sikar, Churu, Nagaur.

Uses discovered ASP.NET WebMethod APIs:
- Getdata (flg=23): Find schools with Math vacancies (no auth needed)
- Getdatabyschool: Get full school profile (no auth needed)
- Staff table: Session-based HTML scrape of schoolprofile.aspx
"""

import json
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://rajshaladarpan.rajasthan.gov.in/SD3/Home/Public2/CitizenCorner"
SEARCH_URL = f"{BASE_URL}/SchoolSearch_Dashboard.aspx"
PROFILE_URL = f"{BASE_URL}/schoolprofile.aspx"
GETDATA_URL = f"{SEARCH_URL}/Getdata"
GETID_URL = f"{SEARCH_URL}/Getid"
GETPROFILE_URL = f"{PROFILE_URL}/Getdatabyschool"

DISTRICTS = {
    "0813": "SIKAR",
    "0804": "CHURU",
    "0814": "NAGAUR",
}

MATH_SUBJECT_ID = "39"  # Mathematics (Science Stream)
MAX_WORKERS = 10
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds, doubles each retry
REQUEST_DELAY = 0.3  # seconds between requests per worker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent / "data"


def api_post(url: str, payload: dict, session: requests.Session | None = None) -> dict:
    """POST JSON to an ASP.NET WebMethod endpoint with retries."""
    headers = {
        "Content-Type": "application/json; charset=utf-8",
    }
    requester = session or requests
    for attempt in range(MAX_RETRIES):
        try:
            resp = requester.post(url, json=payload, headers=headers, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except (requests.RequestException, json.JSONDecodeError) as e:
            if attempt < MAX_RETRIES - 1:
                delay = RETRY_DELAY * (2 ** attempt)
                log.warning("Retry %d for %s: %s (waiting %.1fs)", attempt + 1, url, e, delay)
                time.sleep(delay)
            else:
                raise


def get_candidate_schools(district_code: str) -> list[dict]:
    """Get schools with ANY Mathematics vacancy in a district using flg=23."""
    payload = {
        "flg": "23",
        "District": district_code,
        "Block": "0",  # all blocks
        "eleSecType": "",
        "pin": "0",
        "subid": MATH_SUBJECT_ID,
    }
    result = api_post(GETDATA_URL, payload)
    schools = json.loads(result["d"])
    log.info(
        "District %s (%s): %d candidate schools with Math vacancies",
        district_code, DISTRICTS[district_code], len(schools),
    )
    return schools


def get_school_profile(school_code: str) -> dict | None:
    """Get full school profile via stateless API."""
    try:
        result = api_post(GETPROFILE_URL, {"schoolcode": str(school_code)})
        data = json.loads(result["d"])
        if isinstance(data, list) and data:
            profile = data[0]
        elif isinstance(data, dict):
            profile = data
        else:
            return None
        # Strip trailing whitespace from all string values
        return {k: v.strip() if isinstance(v, str) else v for k, v in profile.items()}
    except Exception as e:
        log.error("Failed to get profile for %s: %s", school_code, e)
        return None


def get_staff_table(school_code: str) -> list[dict] | None:
    """Get the staff table for a school via session-based HTML scrape.

    Steps:
    1. GET search page to establish session cookies
    2. POST Getid to store school code in session
    3. GET profile page and parse the staff HTML table
    """
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    })

    for attempt in range(MAX_RETRIES):
        try:
            # Step 1: establish session
            resp = session.get(SEARCH_URL, timeout=30)
            resp.raise_for_status()

            # Step 2: store school ID in session
            resp = session.post(
                GETID_URL,
                json={"id": str(school_code)},
                headers={
                    "Content-Type": "application/json; charset=utf-8",
                    "X-Requested-With": "XMLHttpRequest",
                },
                timeout=30,
            )
            resp.raise_for_status()

            # Step 3: get profile page with staff table
            resp = session.get(PROFILE_URL, timeout=60)
            resp.raise_for_status()

            # Check we didn't get redirected to search page
            if "SchoolSearch_Dashboard" in resp.url:
                raise ValueError("Redirected to search page -- session not established")

            return parse_staff_table(resp.text)

        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                delay = RETRY_DELAY * (2 ** attempt)
                log.warning(
                    "Staff table retry %d for %s: %s (waiting %.1fs)",
                    attempt + 1, school_code, e, delay,
                )
                time.sleep(delay)
                session = requests.Session()  # fresh session on retry
                session.headers.update({
                    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                })
            else:
                log.error("Failed to get staff table for %s after %d attempts: %s", school_code, MAX_RETRIES, e)
                return None
    return None


def parse_staff_table(html: str) -> list[dict]:
    """Parse the staff GridView table from the school profile HTML."""
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", id="ContentPlaceHolder1_grdSummary3A")
    if not table:
        return []

    staff = []
    rows = table.find_all("tr")
    # Skip header row (first tr)
    for i, row in enumerate(rows[1:], start=1):
        cells = row.find_all("td")
        if len(cells) < 8:
            continue

        # Extract text from spans or direct cell text
        def cell_text(cell):
            span = cell.find("span")
            if span:
                return span.get_text(strip=True)
            return cell.get_text(strip=True)

        entry = {
            "sr_no": i,
            "sanction_post": cell_text(cells[1]),
            "sanction_subject": cell_text(cells[2]),
            "employee_name": cell_text(cells[3]),
            "original_post": cell_text(cells[4]),
            "original_subject": cell_text(cells[5]),
            "current_status": cell_text(cells[6]),
            "joining_date": cell_text(cells[7]),
            # Skip cells[8] -- employee photo
        }
        staff.append(entry)

    return staff


def has_vacant_math_lecturer(staff: list[dict]) -> bool:
    """Check if any row is a vacant Lecturer (I Gr.) Mathematics position."""
    for entry in staff:
        post = entry["sanction_post"].lower()
        subject = entry["sanction_subject"].lower()
        name = entry["employee_name"].strip()

        is_lecturer_first_grade = "lecturer" in post and ("i gr" in post or "1 gr" in post or "first" in post.lower())
        is_math = "mathematics" in subject
        is_vacant = name == "" or name == "---"

        if is_lecturer_first_grade and is_math and is_vacant:
            return True
    return False


def process_school(school: dict, district_code: str) -> dict | None:
    """Process a single candidate school: get staff, check vacancy, get profile."""
    school_code = str(school["SCHCD"])
    school_name = school.get("SchoolName", school_code)

    time.sleep(REQUEST_DELAY)

    # Get staff table
    staff = get_staff_table(school_code)
    if staff is None:
        log.warning("Skipping %s -- could not get staff table", school_name)
        return None

    # Check for vacant Lecturer (I Gr.) Mathematics
    if not has_vacant_math_lecturer(staff):
        log.debug("No vacant Math Lecturer at %s", school_name)
        return None

    log.info("VACANT Math Lecturer found: %s", school_name)

    # Get full profile
    profile = get_school_profile(school_code)

    result = {
        "district": DISTRICTS[district_code],
        "block": school.get("Block", ""),
        "school_name": school_name,
        "shaladarpan_code": school_code,
        "udise_code": profile.get("DISE_CODE", "") if profile else "",
        "contact": {
            "headmaster": profile.get("HM_Name", school.get("HM_Name", "")) if profile else school.get("HM_Name", ""),
            "email": profile.get("HM_Email", "") if profile else "",
            "pincode": profile.get("pincode", "") if profile else "",
            "locality": profile.get("locality", school.get("locality", "")) if profile else school.get("locality", ""),
        },
        "school_info": {},
        "staff": staff,
        "math_lecturer_vacant": True,
    }

    if profile:
        result["school_info"] = {
            "rural_urban": profile.get("Rural_Urban", ""),
            "category": profile.get("SchoolCategory", ""),
            "management": profile.get("SchoolManagement", ""),
            "low_class": profile.get("Low_Class", ""),
            "high_class": profile.get("High_Class", ""),
            "village": profile.get("Village", ""),
            "panchayat": profile.get("Panchayat", ""),
            "assembly": profile.get("AssemblyName", ""),
            "lok_sabha": profile.get("LokSabhaName", ""),
            "division": profile.get("Division", ""),
            "department": profile.get("Department", ""),
            "is_adarsh": profile.get("Is_AadarshSchool", ""),
            "is_model": profile.get("Is_ModelSchool", ""),
            "is_peeo": profile.get("IS_PEEO", ""),
            "streams": profile.get("schSubList", []),
        }

    return result


def scrape_district(district_code: str) -> list[dict]:
    """Scrape all vacant Math Lecturer positions in a district."""
    district_name = DISTRICTS[district_code]
    log.info("=== Starting district: %s (%s) ===", district_name, district_code)

    # Phase 1: enumerate candidates
    candidates = get_candidate_schools(district_code)
    if not candidates:
        log.info("No candidate schools found for %s", district_name)
        return []

    # Phase 2: verify and extract (parallelized)
    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(process_school, school, district_code): school
            for school in candidates
        }
        for future in as_completed(futures):
            school = futures[future]
            try:
                result = future.result()
                if result:
                    results.append(result)
            except Exception as e:
                log.error("Error processing %s: %s", school.get("SchoolName", "?"), e)

    # Sort by block then school name
    results.sort(key=lambda x: (x["block"], x["school_name"]))

    log.info(
        "District %s: %d/%d candidates have vacant Lecturer (I Gr.) Mathematics",
        district_name, len(results), len(candidates),
    )
    return results


def main():
    DATA_DIR.mkdir(exist_ok=True)

    # Allow passing specific district codes as args
    district_codes = sys.argv[1:] if len(sys.argv) > 1 else list(DISTRICTS.keys())

    all_results = []
    for code in district_codes:
        if code not in DISTRICTS:
            log.error("Unknown district code: %s", code)
            continue

        results = scrape_district(code)

        # Save per-district file
        district_name = DISTRICTS[code].lower()
        output_path = DATA_DIR / f"{district_name}_vacancies.json"
        output_path.write_text(json.dumps(results, indent=2, ensure_ascii=False))
        log.info("Saved %d results to %s", len(results), output_path)

        all_results.extend(results)

    # Save combined file
    combined_path = DATA_DIR / "all_vacancies.json"
    combined_path.write_text(json.dumps(all_results, indent=2, ensure_ascii=False))
    log.info("=== TOTAL: %d schools with vacant Lecturer (I Gr.) Mathematics ===", len(all_results))
    log.info("Saved combined results to %s", combined_path)

    # Print summary
    print(f"\nSummary:")
    for code in district_codes:
        if code in DISTRICTS:
            district_name = DISTRICTS[code]
            count = sum(1 for r in all_results if r["district"] == district_name)
            print(f"  {district_name}: {count} schools with vacant Math Lecturer")
    print(f"  TOTAL: {len(all_results)} schools")


if __name__ == "__main__":
    main()
