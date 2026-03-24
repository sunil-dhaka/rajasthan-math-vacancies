"""Geocode all 444 schools using Nominatim with IPv4 via curl subprocess."""

import json
import subprocess
import time
from pathlib import Path

DATA_DIR = Path(__file__).parent / "data"
GEOCODE_CACHE = DATA_DIR / "geocode_cache.json"


def load_cache():
    if GEOCODE_CACHE.exists():
        return json.loads(GEOCODE_CACHE.read_text())
    return {}


def save_cache(cache):
    GEOCODE_CACHE.write_text(json.dumps(cache, indent=2))


def clean_village(village):
    v = village
    for pfx in ["NP_", "ND_", "NN_"]:
        if v.startswith(pfx):
            v = v[len(pfx):]
    if " - WARD" in v:
        v = v.split(" - WARD")[0]
    return v.strip()


def nominatim_curl(query):
    """Geocode via curl -4 (IPv4 forced) to avoid IPv6 issues."""
    import urllib.parse
    encoded = urllib.parse.quote(query)
    url = f"https://nominatim.openstreetmap.org/search?q={encoded}&format=json&limit=1&countrycodes=in"
    try:
        result = subprocess.run(
            ["curl", "-4", "-s", "--max-time", "15", "-H", "User-Agent: ShalaDarpanMap/1.0", url],
            capture_output=True, text=True, timeout=20,
        )
        if result.returncode == 0 and result.stdout.strip():
            data = json.loads(result.stdout)
            if data:
                lat, lon = float(data[0]["lat"]), float(data[0]["lon"])
                if 23.5 < lat < 30.5 and 69.0 < lon < 79.0:
                    return lat, lon
    except Exception:
        pass
    return None


def geocode_school(school):
    village = clean_village(school.get("Village", ""))
    district = school["District"]
    pin = school.get("pincode", "")
    block = school.get("Block", "").split("(")[0].strip()

    queries = []
    if village and pin:
        queries.append(f"{village}, {pin}, Rajasthan, India")
    if village:
        queries.append(f"{village}, {district}, Rajasthan, India")
    if block:
        queries.append(f"{block}, {district}, Rajasthan, India")
    if pin:
        queries.append(f"{pin}, Rajasthan, India")

    for query in queries:
        time.sleep(1.1)
        result = nominatim_curl(query)
        if result:
            return result[0], result[1], query
    return None


def main():
    schools = json.load(open(DATA_DIR / "all_444_schools_enriched.json"))
    cache = load_cache()
    print(f"Loaded {len(schools)} schools, cache has {len(cache)} entries")

    to_geocode = [s for s in schools if str(s["SCHCD"]) not in cache]
    print(f"Need to geocode: {len(to_geocode)}")

    for i, school in enumerate(to_geocode):
        code = str(school["SCHCD"])
        result = geocode_school(school)
        if result:
            lat, lon, query = result
            cache[code] = {"lat": lat, "lon": lon, "query": query}
        else:
            cache[code] = None
            print(f"  MISS: {school['SchoolName'][:50]}")

        if (i + 1) % 20 == 0:
            save_cache(cache)
            found = sum(1 for v in cache.values() if v)
            print(f"  {i+1}/{len(to_geocode)} done ({found} found)")

    save_cache(cache)
    found = sum(1 for v in cache.values() if v)
    missed = sum(1 for v in cache.values() if not v)
    print(f"\nDone. {found} found, {missed} missed out of {len(cache)}")


if __name__ == "__main__":
    main()
