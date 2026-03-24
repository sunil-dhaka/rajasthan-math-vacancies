"""
Fetch Lecturer (I Gr.) vacancy counts for ALL districts x ALL subjects.
Uses flg=23, eleSecType=3 (Lecturer I Gr.), iterating over all subject IDs.
Outputs a CSV matrix: districts as rows, subjects as columns, vacancy counts as values.
"""

import csv
import json
import sys
import time
from pathlib import Path

import requests

URL = "https://rajshaladarpan.rajasthan.gov.in/SD3/Home/Public2/CitizenCorner/SchoolSearch_Dashboard.aspx/Getdata"
HEADERS = {"Content-Type": "application/json; charset=utf-8"}

DISTRICTS = {
    "0821": "AJMER", "0806": "ALWAR", "0835": "BALOTARA", "0828": "BANSWARA",
    "0831": "BARAN", "0817": "BARMER", "0836": "BEAWAR", "0807": "BHARATPUR",
    "0824": "BHILWARA", "0803": "BIKANER", "0823": "BUNDI", "0829": "CHITTAURGARH",
    "0804": "CHURU", "0811": "DAUSA", "0837": "DEEG", "0808": "DHAULPUR",
    "0838": "DIDWANA-KUCHAMAN", "0827": "DUNGARPUR", "0801": "GANGANAGAR",
    "0802": "HANUMANGARH", "0812": "JAIPUR", "0816": "JAISALMER", "0818": "JALOR",
    "0832": "JHALAWAR", "0805": "JHUNJHUNUN", "0815": "JODHPUR", "0809": "KARAULI",
    "0844": "KHAIRTHAL-TIJARA", "0830": "KOTA", "0845": "KOTPUTLI-BEHROR",
    "0814": "NAGAUR", "0820": "PALI", "0847": "PHALODI", "0833": "PRATAPGARH",
    "0825": "RAJSAMAND", "0810": "S.MADHOPUR", "0848": "SALUMBAR", "0813": "SIKAR",
    "0819": "SIROHI", "0822": "TONK", "0826": "UDAIPUR",
}

# Key Lecturer (I Gr.) subjects -- Science, Commerce, Arts core subjects
SUBJECTS = {
    # Science
    "35": "Physics", "36": "Chemistry", "37": "Biology",
    "39": "Mathematics (Sci)", "40": "Agri Science",
    # Commerce
    "45": "Accountancy", "46": "Business Studies", "47": "Economics (Com)",
    "48": "Mathematics (Com)",
    # Arts
    "55": "Economics (Arts)", "56": "Political Science", "57": "Sanskrit Lit",
    "58": "History", "59": "Geography", "60": "Mathematics (Arts)",
    "61": "English Lit", "62": "Hindi Lit", "72": "Home Science",
    "76": "Physical Education",
    # Others
    "38": "Geology", "109": "Economics (Sci)", "110": "Phys Ed (Sci)",
}

DATA_DIR = Path(__file__).parent / "data"
PROGRESS_FILE = DATA_DIR / "vacancy_matrix_progress.json"


def fetch(district_code, subject_id):
    payload = {
        "flg": "23",
        "District": district_code,
        "Block": "0",
        "eleSecType": "3",  # Lecturer (I Gr.)
        "pin": "0",
        "subid": subject_id,
    }
    for attempt in range(6):
        try:
            r = requests.post(URL, json=payload, headers=HEADERS, timeout=90)
            r.raise_for_status()
            schools = json.loads(r.json()["d"])
            # Filter out Mahatma Gandhi schools
            non_mg = [s for s in schools if "MAHATMA GANDHI" not in s["SchoolName"].upper()]
            return len(non_mg)
        except Exception as e:
            if attempt == 5:
                print(f"  FAILED {DISTRICTS.get(district_code, district_code)} x {subject_id}: {e}", file=sys.stderr)
                return -1
            time.sleep(3 * (attempt + 1))
    return -1


def load_progress():
    if PROGRESS_FILE.exists():
        return json.loads(PROGRESS_FILE.read_text())
    return {}


def save_progress(progress):
    PROGRESS_FILE.write_text(json.dumps(progress))


def main():
    DATA_DIR.mkdir(exist_ok=True)
    progress = load_progress()

    sorted_districts = sorted(DISTRICTS.items(), key=lambda x: x[1])
    sorted_subjects = sorted(SUBJECTS.items(), key=lambda x: x[1])

    total_calls = len(sorted_districts) * len(sorted_subjects)
    done = len(progress)
    print(f"Fetching {total_calls} combinations ({done} cached)")

    for dist_code, dist_name in sorted_districts:
        for sub_id, sub_name in sorted_subjects:
            key = f"{dist_code}_{sub_id}"
            if key in progress:
                continue

            time.sleep(1.5)
            count = fetch(dist_code, sub_id)
            progress[key] = count
            save_progress(progress)

            if count > 0:
                print(f"  {dist_name:25s} x {sub_name:20s} = {count}")
            elif count == -1:
                print(f"  {dist_name:25s} x {sub_name:20s} = FAILED")

        done_now = sum(1 for k in progress if k.startswith(dist_code))
        if done_now == len(sorted_subjects):
            total = sum(progress.get(f"{dist_code}_{s}", 0) for s, _ in sorted_subjects)
            print(f"  {dist_name}: total vacancies = {total}")

    # Build matrix
    print(f"\nBuilding CSV matrix...")

    # CSV output
    csv_path = DATA_DIR / "vacancy_matrix.csv"
    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f)

        # Header row
        header = ["District"] + [sub_name for _, sub_name in sorted_subjects] + ["TOTAL"]
        writer.writerow(header)

        # Data rows
        grand_totals = {sub_id: 0 for sub_id, _ in sorted_subjects}
        grand_total = 0

        for dist_code, dist_name in sorted_districts:
            row = [dist_name]
            row_total = 0
            for sub_id, _ in sorted_subjects:
                key = f"{dist_code}_{sub_id}"
                val = progress.get(key, 0)
                if val < 0:
                    val = 0  # treat failures as 0
                row.append(val)
                row_total += val
                grand_totals[sub_id] += val
            row.append(row_total)
            grand_total += row_total
            writer.writerow(row)

        # Totals row
        totals_row = ["TOTAL"] + [grand_totals[sub_id] for sub_id, _ in sorted_subjects] + [grand_total]
        writer.writerow(totals_row)

    print(f"Saved: {csv_path}")

    # Also save as JSON for programmatic use
    json_path = DATA_DIR / "vacancy_matrix.json"
    matrix = {
        "districts": {dist_name: {} for _, dist_name in sorted_districts},
        "subjects": [sub_name for _, sub_name in sorted_subjects],
        "totals_by_subject": {},
        "totals_by_district": {},
        "grand_total": 0,
    }

    for dist_code, dist_name in sorted_districts:
        dist_total = 0
        for sub_id, sub_name in sorted_subjects:
            key = f"{dist_code}_{sub_id}"
            val = max(progress.get(key, 0), 0)
            matrix["districts"][dist_name][sub_name] = val
            dist_total += val
        matrix["totals_by_district"][dist_name] = dist_total

    for sub_id, sub_name in sorted_subjects:
        matrix["totals_by_subject"][sub_name] = grand_totals[sub_id]

    matrix["grand_total"] = grand_total
    json_path.write_text(json.dumps(matrix, indent=2, ensure_ascii=False))

    # Print summary
    print(f"\n{'='*60}")
    print(f"VACANCY MATRIX SUMMARY")
    print(f"{'='*60}")
    print(f"Grand total: {grand_total} vacant Lecturer (I Gr.) positions")
    print(f"\nTop subjects by vacancy count:")
    for sub_id, sub_name in sorted(sorted_subjects, key=lambda x: grand_totals[x[0]], reverse=True):
        if grand_totals[sub_id] > 0:
            print(f"  {sub_name:25s} {grand_totals[sub_id]:>5d}")

    print(f"\nTop districts by vacancy count:")
    dist_totals = []
    for dist_code, dist_name in sorted_districts:
        t = sum(max(progress.get(f"{dist_code}_{s}", 0), 0) for s, _ in sorted_subjects)
        dist_totals.append((dist_name, t))
    for name, t in sorted(dist_totals, key=lambda x: x[1], reverse=True)[:15]:
        if t > 0:
            print(f"  {name:25s} {t:>5d}")


if __name__ == "__main__":
    main()
