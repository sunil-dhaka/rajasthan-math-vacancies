"""Fetch Math vacancy schools (flg=23) for ALL Rajasthan districts. Fast -- just API calls."""

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

DATA_DIR = Path(__file__).parent / "data"


def fetch_district(code, name):
    payload = {"flg": "23", "District": code, "Block": "0", "eleSecType": "", "pin": "0", "subid": "39"}
    for attempt in range(8):
        try:
            r = requests.post(URL, json=payload, headers=HEADERS, timeout=90)
            r.raise_for_status()
            return json.loads(r.json()["d"])
        except Exception as e:
            if attempt == 7:
                print(f"  {name}: FAILED after 8 attempts: {e}", file=sys.stderr)
                return None
            delay = min(5 * (attempt + 1), 30)
            print(f"  {name}: attempt {attempt+1} failed, waiting {delay}s...", file=sys.stderr)
            time.sleep(delay)
    return None


PROGRESS_FILE = DATA_DIR / "vacancy_fetch_progress.json"


def load_progress():
    if PROGRESS_FILE.exists():
        return json.loads(PROGRESS_FILE.read_text())
    return {}


def save_partial(results):
    """Save progress so we can resume."""
    # Save without school lists (just counts + codes of done districts)
    partial = {}
    for name, d in results.items():
        partial[name] = {"code": d["code"], "all": d["all"], "non_mg": d["non_mg"], "mg": d["mg"], "schools": d["schools"]}
    PROGRESS_FILE.write_text(json.dumps(partial, indent=2, ensure_ascii=False))


def main():
    DATA_DIR.mkdir(exist_ok=True)

    # Resume from previous progress
    results = load_progress()
    if results:
        print(f"Resuming -- {len(results)} districts already fetched")

    failed = []

    for code, name in sorted(DISTRICTS.items(), key=lambda x: x[1]):
        if name in results:
            print(f"{name:25s}  (cached) non-MG={results[name]['non_mg']:3d}")
            continue
        time.sleep(3)
        schools = fetch_district(code, name)
        if schools is None:
            failed.append(name)
            continue

        non_mg = [s for s in schools if "MAHATMA GANDHI" not in s["SchoolName"].upper()]
        mg = len(schools) - len(non_mg)
        results[name] = {"code": code, "all": len(schools), "non_mg": len(non_mg), "mg": mg, "schools": non_mg}
        save_partial(results)
        print(f"{name:25s}  total={len(schools):3d}  non-MG={len(non_mg):3d}  MG={mg:3d}")

    # Retry failed
    for name in list(failed):
        code = [c for c, n in DISTRICTS.items() if n == name][0]
        time.sleep(5)
        schools = fetch_district(code, name)
        if schools is not None:
            non_mg = [s for s in schools if "MAHATMA GANDHI" not in s["SchoolName"].upper()]
            mg = len(schools) - len(non_mg)
            results[name] = {"code": code, "all": len(schools), "non_mg": len(non_mg), "mg": mg, "schools": non_mg}
            failed.remove(name)
            print(f"{name:25s}  total={len(schools):3d}  non-MG={len(non_mg):3d}  MG={mg:3d}  (retry)")

    # Save
    all_non_mg = []
    for name in sorted(results):
        for s in results[name]["schools"]:
            s["_district"] = name
            all_non_mg.append(s)

    DATA_DIR.mkdir(exist_ok=True)
    (DATA_DIR / "all_rajasthan_vacancy_non_mg.json").write_text(json.dumps(all_non_mg, indent=2, ensure_ascii=False))

    summary = {name: {k: v for k, v in d.items() if k != "schools"} for name, d in results.items()}
    (DATA_DIR / "all_rajasthan_vacancy_summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False))

    # Print summary
    total_all = sum(d["all"] for d in results.values())
    total_non_mg = sum(d["non_mg"] for d in results.values())
    total_mg = sum(d["mg"] for d in results.values())

    print(f"\n{'='*60}")
    print(f"ALL RAJASTHAN - Math Lecturer Vacancy (flg=23)")
    print(f"{'='*60}")
    print(f"Districts: {len(results)}/{len(DISTRICTS)}" + (f" ({len(failed)} failed: {failed})" if failed else ""))
    print(f"Total vacancy schools: {total_all}")
    print(f"  Non-MG: {total_non_mg}")
    print(f"  MG:     {total_mg}")
    print(f"\nPer district (non-MG only):")
    print(f"{'District':<25s} {'Non-MG':>6s}")
    print("-" * 33)
    for name in sorted(results, key=lambda n: results[n]["non_mg"], reverse=True):
        if results[name]["non_mg"] > 0:
            print(f"{name:<25s} {results[name]['non_mg']:>6d}")
    print("-" * 33)
    print(f"{'TOTAL':<25s} {total_non_mg:>6d}")
    print(f"\nSaved to data/all_rajasthan_vacancy_non_mg.json")


if __name__ == "__main__":
    main()
