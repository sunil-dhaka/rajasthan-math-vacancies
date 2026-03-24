"""
Thorough scrape: get ALL non-Mahatma Gandhi Science/Math schools,
check each one's staff table for vacant Lecturer (I Gr.) Mathematics.

No reliance on flg=23 vacancy filter. We check every school individually.
"""

import json
import logging
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

MAX_WORKERS = 8
MAX_RETRIES = 4
RETRY_DELAY = 3
REQUEST_DELAY = 0.5

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent / "data"
RAW_DIR = DATA_DIR / "raw_school_data"


def api_post(url, payload, session=None):
    headers = {"Content-Type": "application/json; charset=utf-8"}
    requester = session or requests
    for attempt in range(MAX_RETRIES):
        try:
            resp = requester.post(url, json=payload, headers=headers, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except (requests.RequestException, json.JSONDecodeError) as e:
            if attempt < MAX_RETRIES - 1:
                delay = RETRY_DELAY * (2 ** attempt)
                log.warning("Retry %d for %s: %s (%.1fs)", attempt + 1, url.split("/")[-1], e, delay)
                time.sleep(delay)
            else:
                raise


def get_all_science_math_schools(district_code):
    """Get ALL schools offering Science/Mathematics in a district (flg=7)."""
    payload = {
        "flg": "7",
        "District": district_code,
        "Block": "0",
        "eleSecType": "1",  # Science stream
        "pin": "0",
        "subid": "39",  # Mathematics
    }
    result = api_post(GETDATA_URL, payload)
    return json.loads(result["d"])


def get_school_profile(school_code):
    try:
        result = api_post(GETPROFILE_URL, {"schoolcode": str(school_code)})
        data = json.loads(result["d"])
        if isinstance(data, list) and data:
            profile = data[0]
        elif isinstance(data, dict):
            profile = data
        else:
            return None
        return {k: v.strip() if isinstance(v, str) else v for k, v in profile.items()}
    except Exception as e:
        log.error("Profile failed for %s: %s", school_code, e)
        return None


def get_staff_table(school_code):
    """Get staff table via session-based HTML scrape."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    })

    for attempt in range(MAX_RETRIES):
        try:
            resp = session.get(SEARCH_URL, timeout=30)
            resp.raise_for_status()

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

            resp = session.get(PROFILE_URL, timeout=60)
            resp.raise_for_status()

            if "SchoolSearch_Dashboard" in resp.url:
                raise ValueError("Redirected -- session not established")

            return parse_staff_table(resp.text)

        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                delay = RETRY_DELAY * (2 ** attempt)
                log.warning("Staff retry %d for %s: %s (%.1fs)", attempt + 1, school_code, e, delay)
                time.sleep(delay)
                session = requests.Session()
                session.headers.update({
                    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                })
            else:
                log.error("Staff table FAILED for %s after %d attempts: %s", school_code, MAX_RETRIES, e)
                return None
    return None


def parse_staff_table(html):
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", id="ContentPlaceHolder1_grdSummary3A")
    if not table:
        return []

    staff = []
    rows = table.find_all("tr")
    for i, row in enumerate(rows[1:], start=1):
        cells = row.find_all("td")
        if len(cells) < 8:
            continue

        def cell_text(cell):
            span = cell.find("span")
            return (span.get_text(strip=True) if span else cell.get_text(strip=True))

        staff.append({
            "sr_no": i,
            "sanction_post": cell_text(cells[1]),
            "sanction_subject": cell_text(cells[2]),
            "employee_name": cell_text(cells[3]),
            "original_post": cell_text(cells[4]),
            "original_subject": cell_text(cells[5]),
            "current_status": cell_text(cells[6]),
            "joining_date": cell_text(cells[7]),
        })
    return staff


def check_math_lecturer_vacancy(staff):
    """Check staff list for vacant Lecturer (I Gr.) Mathematics.

    Returns a dict with details about the math lecturer row, or None if no vacancy.
    """
    for entry in staff:
        post = entry["sanction_post"]
        subject = entry["sanction_subject"]
        name = entry["employee_name"].strip()

        post_lower = post.lower()
        is_lecturer = "lecturer" in post_lower
        is_first_grade = "i gr" in post_lower or "1 gr" in post_lower or "first" in post_lower
        is_math = "mathematics" in subject.lower()
        is_vacant = name == "" or name == "---"

        if is_lecturer and is_first_grade and is_math:
            return {
                "post": post,
                "subject": subject,
                "employee_name": name,
                "is_vacant": is_vacant,
            }
    return None


def process_school(school, district_code):
    """Process a single school: get staff, check vacancy, get profile if vacant."""
    school_code = str(school["SCHCD"])
    school_name = school.get("SchoolName", school_code)

    time.sleep(REQUEST_DELAY)

    # Get staff table
    staff = get_staff_table(school_code)
    if staff is None:
        return {"school_code": school_code, "school_name": school_name, "status": "FAILED", "error": "could not get staff table"}

    # Check for Lecturer (I Gr.) Mathematics row
    math_row = check_math_lecturer_vacancy(staff)

    if math_row is None:
        return {
            "school_code": school_code,
            "school_name": school_name,
            "status": "NO_MATH_LECTURER_POST",
            "staff_count": len(staff),
        }

    if not math_row["is_vacant"]:
        return {
            "school_code": school_code,
            "school_name": school_name,
            "status": "FILLED",
            "math_lecturer_name": math_row["employee_name"],
            "staff_count": len(staff),
        }

    # VACANT -- get full details
    log.info("VACANT: %s", school_name)

    profile = get_school_profile(school_code)

    result = {
        "school_code": school_code,
        "school_name": school_name,
        "status": "VACANT",
        "district": DISTRICTS[district_code],
        "block": school.get("Block", ""),
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


def slugify(name):
    return name.lower().replace(" ", "_").replace("(", "").replace(")", "").strip("_")


def main():
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    all_vacant = []
    all_audit = []  # every school's status for cross-check

    for district_code, district_name in DISTRICTS.items():
        log.info("=== %s (%s) ===", district_name, district_code)

        # Get ALL Science/Math schools
        all_schools = get_all_science_math_schools(district_code)
        log.info("Total Science/Math schools: %d", len(all_schools))

        # Filter out Mahatma Gandhi
        non_mg = [s for s in all_schools if "MAHATMA GANDHI" not in s["SchoolName"].upper()]
        log.info("Non-MG schools to check: %d (filtered out %d MG)", len(non_mg), len(all_schools) - len(non_mg))

        # Process each school
        district_vacant = []
        district_audit = []

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(process_school, school, district_code): school
                for school in non_mg
            }
            for future in as_completed(futures):
                school = futures[future]
                try:
                    result = future.result()
                    district_audit.append(result)
                    if result["status"] == "VACANT":
                        district_vacant.append(result)
                except Exception as e:
                    log.error("Error processing %s: %s", school.get("SchoolName", "?"), e)
                    district_audit.append({
                        "school_code": str(school["SCHCD"]),
                        "school_name": school.get("SchoolName", "?"),
                        "status": "ERROR",
                        "error": str(e),
                    })

        district_vacant.sort(key=lambda x: (x.get("block", ""), x["school_name"]))
        district_audit.sort(key=lambda x: x["school_name"])

        # Save raw audit for this district
        audit_path = RAW_DIR / f"{district_name.lower()}_audit.json"
        audit_path.write_text(json.dumps(district_audit, indent=2, ensure_ascii=False))

        # Summary
        filled = sum(1 for r in district_audit if r["status"] == "FILLED")
        no_post = sum(1 for r in district_audit if r["status"] == "NO_MATH_LECTURER_POST")
        failed = sum(1 for r in district_audit if r["status"] in ("FAILED", "ERROR"))
        vacant = len(district_vacant)

        log.info(
            "%s results: %d checked -> %d VACANT, %d FILLED, %d NO_POST, %d FAILED",
            district_name, len(non_mg), vacant, filled, no_post, failed,
        )

        all_vacant.extend(district_vacant)
        all_audit.extend(district_audit)

    # Save vacancy results with proper folder structure
    govt_dir = DATA_DIR / "govt_schools"
    govt_dir.mkdir(exist_ok=True)

    for district_name in DISTRICTS.values():
        dist_dir = govt_dir / district_name.lower()
        dist_dir.mkdir(exist_ok=True)

        dist_schools = [s for s in all_vacant if s["district"] == district_name]
        dist_schools.sort(key=lambda x: (x.get("block", ""), x["school_name"]))

        # Remove status/error fields from final output
        clean = [{k: v for k, v in s.items() if k not in ("status", "error")} for s in dist_schools]

        # Write district all.json
        (dist_dir / "all.json").write_text(json.dumps(clean, indent=2, ensure_ascii=False))

        # Write per-block files
        by_block = {}
        for s in clean:
            by_block.setdefault(s.get("block", "unknown"), []).append(s)
        for block, schools in sorted(by_block.items()):
            block_file = dist_dir / f"{slugify(block)}.json"
            block_file.write_text(json.dumps(schools, indent=2, ensure_ascii=False))

    # Write combined
    all_clean = [{k: v for k, v in s.items() if k not in ("status", "error")} for s in all_vacant]
    (govt_dir / "all.json").write_text(json.dumps(all_clean, indent=2, ensure_ascii=False))

    # Save full audit
    (RAW_DIR / "full_audit.json").write_text(json.dumps(all_audit, indent=2, ensure_ascii=False))

    # Print final summary
    print("\n" + "=" * 60)
    print("THOROUGH CROSS-CHECK RESULTS")
    print("=" * 60)
    print(f"\nTotal non-MG Science/Math schools checked: {len(all_audit)}")
    print(f"  VACANT Lecturer (I Gr.) Math:    {sum(1 for r in all_audit if r['status'] == 'VACANT')}")
    print(f"  FILLED Lecturer (I Gr.) Math:    {sum(1 for r in all_audit if r['status'] == 'FILLED')}")
    print(f"  No Math Lecturer post exists:    {sum(1 for r in all_audit if r['status'] == 'NO_MATH_LECTURER_POST')}")
    print(f"  Failed to retrieve:              {sum(1 for r in all_audit if r['status'] in ('FAILED', 'ERROR'))}")
    print()
    for district_name in DISTRICTS.values():
        dist_v = [r for r in all_audit if r.get("district") == district_name and r["status"] == "VACANT"]
        dist_f = [r for r in all_audit if r["status"] == "FILLED" and district_name.lower() in r.get("school_name", "").lower()]
        print(f"  {district_name}: {len(dist_v)} vacant")
    print()

    # Show filled schools (for cross-check visibility)
    filled = [r for r in all_audit if r["status"] == "FILLED"]
    if filled:
        print("Schools with FILLED Math Lecturer (I Gr.) positions:")
        for r in sorted(filled, key=lambda x: x["school_name"]):
            print(f"  {r['school_name']} -> {r['math_lecturer_name']}")
    print()

    no_post = [r for r in all_audit if r["status"] == "NO_MATH_LECTURER_POST"]
    if no_post:
        print(f"Schools with no Lecturer (I Gr.) Math post at all: {len(no_post)}")
        for r in sorted(no_post, key=lambda x: x["school_name"]):
            print(f"  {r['school_name']} (staff rows: {r.get('staff_count', '?')})")


if __name__ == "__main__":
    main()
