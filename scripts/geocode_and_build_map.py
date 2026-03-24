"""
Geocode all 444 schools and generate an interactive map HTML page.
Uses Nominatim with aggressive retries and progress caching.
Outputs: map/index.html (self-contained, deployable to Vercel)
"""

import json
import time
import sys
from pathlib import Path

import requests

DATA_DIR = Path(__file__).parent / "data"
MAP_DIR = Path(__file__).parent / "map"
GEOCODE_CACHE = DATA_DIR / "geocode_cache.json"

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
HEADERS = {"User-Agent": "ShalaDarpanMap/1.0 (education-research)"}


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


def geocode_query(query):
    """Single Nominatim geocode attempt."""
    try:
        r = requests.get(
            NOMINATIM_URL,
            params={"q": query, "format": "json", "limit": 1, "countrycodes": "in"},
            headers=HEADERS,
            timeout=20,
        )
        r.raise_for_status()
        results = r.json()
        if results:
            lat, lon = float(results[0]["lat"]), float(results[0]["lon"])
            if 23.5 < lat < 30.5 and 69.0 < lon < 79.0:  # Rajasthan bbox
                return lat, lon
    except Exception:
        pass
    return None


def geocode_school(school):
    """Try multiple query strategies for a school."""
    village = clean_village(school.get("Village", ""))
    district = school["District"]
    pin = school.get("pincode", "")
    block = school.get("Block", "").split("(")[0].strip()
    locality = school.get("locality", "")

    queries = []
    # Strategy 1: village + pincode
    if village and pin:
        queries.append(f"{village}, {pin}, Rajasthan, India")
    # Strategy 2: village + district
    if village:
        queries.append(f"{village}, {district}, Rajasthan, India")
    # Strategy 3: locality + district
    if locality:
        loc_clean = locality.replace("VPO-", "").replace("VPO ", "").split(",")[0].strip()
        if loc_clean:
            queries.append(f"{loc_clean}, {district}, Rajasthan, India")
    # Strategy 4: block + district
    if block:
        queries.append(f"{block}, {district}, Rajasthan, India")
    # Strategy 5: pincode only
    if pin:
        queries.append(f"{pin}, Rajasthan, India")

    for query in queries:
        time.sleep(1.1)
        result = geocode_query(query)
        if result:
            return result[0], result[1], query
    return None


def generate_map_html(schools_with_coords):
    """Generate self-contained Leaflet map HTML."""
    # Build markers JS
    markers_js = "var markers = [\n"
    for s in schools_with_coords:
        name_esc = s["SchoolName"].replace("'", "\\'").replace('"', '\\"')
        hm = (s.get("HM_Name") or "").replace("'", "\\'")
        email = (s.get("hm_email") or "").replace("'", "\\'")
        pin = s.get("pincode", "")
        udise = s.get("udise_code", "")
        rural = s.get("rural_urban", "")
        cat = (s.get("school_category") or "").strip()
        block = s.get("Block", "")
        district = s["District"]
        village = s.get("Village", "")
        adarsh = s.get("is_adarsh", "")
        model = s.get("is_model", "")
        low_c = s.get("low_class", "")
        high_c = s.get("high_class", "")
        streams = ""
        for st in s.get("streams", []):
            streams += f"<b>{st.get('streamname', '')}</b>: {st.get('subjectname', '')}<br>"

        popup = (
            f"<div style='min-width:280px;max-width:350px;font-family:system-ui;font-size:13px;'>"
            f"<div style='font-size:14px;font-weight:700;margin-bottom:6px;color:#1e40af;'>{name_esc}</div>"
            f"<table style='width:100%;border-collapse:collapse;font-size:12px;'>"
            f"<tr><td style='padding:2px 8px 2px 0;color:#666;'>District</td><td><b>{district}</b></td></tr>"
            f"<tr><td style='padding:2px 8px 2px 0;color:#666;'>Block</td><td>{block}</td></tr>"
            f"<tr><td style='padding:2px 8px 2px 0;color:#666;'>Village</td><td>{village}</td></tr>"
            f"<tr><td style='padding:2px 8px 2px 0;color:#666;'>PIN</td><td>{pin}</td></tr>"
            f"<tr><td style='padding:2px 8px 2px 0;color:#666;'>UDISE</td><td>{udise}</td></tr>"
            f"<tr><td style='padding:2px 8px 2px 0;color:#666;'>Principal</td><td>{hm}</td></tr>"
            f"<tr><td style='padding:2px 8px 2px 0;color:#666;'>Email</td><td><a href='mailto:{email}'>{email}</a></td></tr>"
            f"<tr><td style='padding:2px 8px 2px 0;color:#666;'>Type</td><td>{rural} | {cat}</td></tr>"
            f"<tr><td style='padding:2px 8px 2px 0;color:#666;'>Classes</td><td>{low_c} - {high_c}</td></tr>"
            f"<tr><td style='padding:2px 8px 2px 0;color:#666;'>Adarsh</td><td>{adarsh}</td></tr>"
            f"<tr><td style='padding:2px 8px 2px 0;color:#666;'>Model</td><td>{model}</td></tr>"
            f"</table>"
            f"<div style='margin-top:6px;padding-top:6px;border-top:1px solid #eee;font-size:11px;'>"
            f"{streams if streams else 'No stream data'}"
            f"</div>"
            f"<div style='margin-top:6px;'>"
            f"<a href='https://www.google.com/maps/search/{name_esc.replace(' ', '+')}+{pin}' target='_blank' "
            f"style='color:#1e40af;font-size:11px;'>Open in Google Maps</a>"
            f"</div>"
            f"</div>"
        )
        popup_esc = popup.replace("'", "\\'").replace("\n", "")

        markers_js += f"  [{s['_lat']}, {s['_lon']}, '{popup_esc}', '{district}'],\n"
    markers_js += "];\n"

    # District colors
    districts = sorted(set(s["District"] for s in schools_with_coords))
    colors = [
        "#ef4444", "#f97316", "#f59e0b", "#eab308", "#84cc16", "#22c55e",
        "#14b8a6", "#06b6d4", "#0ea5e9", "#3b82f6", "#6366f1", "#8b5cf6",
        "#a855f7", "#d946ef", "#ec4899", "#f43f5e", "#fb7185", "#fda4af",
        "#fdba74", "#fcd34d", "#bef264", "#86efac", "#5eead4", "#67e8f9",
        "#7dd3fc", "#93c5fd", "#a5b4fc", "#c4b5fd", "#d8b4fe", "#f0abfc",
        "#f9a8d4", "#fca5a5", "#fed7aa", "#fef08a", "#d9f99d", "#bbf7d0",
        "#99f6e4", "#a5f3fc", "#bae6fd", "#c7d2fe", "#e9d5ff",
    ]
    district_colors = {d: colors[i % len(colors)] for i, d in enumerate(districts)}

    colors_js = "var districtColors = " + json.dumps(district_colors) + ";\n"

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Rajasthan Math Lecturer Vacancies Map</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<style>
    * {{ margin: 0; padding: 0; box-sizing: border-box; }}
    body {{ font-family: system-ui, -apple-system, sans-serif; background: #0f172a; }}
    #map {{ width: 100%; height: 100vh; }}
    .info-panel {{
        position: absolute; top: 10px; right: 10px; z-index: 1000;
        background: rgba(15,23,42,0.95); color: #e2e8f0; padding: 16px 20px;
        border-radius: 10px; border: 1px solid #334155; max-width: 300px;
        font-size: 13px; backdrop-filter: blur(8px);
    }}
    .info-panel h2 {{ font-size: 16px; margin-bottom: 4px; color: #f1f5f9; }}
    .info-panel .subtitle {{ color: #94a3b8; font-size: 11px; margin-bottom: 12px; }}
    .info-panel .stat {{ display: flex; justify-content: space-between; padding: 3px 0; }}
    .info-panel .stat-val {{ font-weight: 700; color: #60a5fa; }}
    .filter-panel {{
        position: absolute; top: 10px; left: 55px; z-index: 1000;
        background: rgba(15,23,42,0.95); color: #e2e8f0; padding: 12px 16px;
        border-radius: 10px; border: 1px solid #334155;
        font-size: 12px; backdrop-filter: blur(8px); max-height: 90vh; overflow-y: auto;
    }}
    .filter-panel h3 {{ font-size: 13px; margin-bottom: 8px; }}
    .filter-item {{
        display: flex; align-items: center; gap: 6px; padding: 2px 0; cursor: pointer;
    }}
    .filter-item:hover {{ color: #fff; }}
    .color-dot {{
        width: 10px; height: 10px; border-radius: 50%; display: inline-block; flex-shrink: 0;
    }}
    .filter-count {{ color: #64748b; margin-left: auto; font-size: 11px; }}
    .search-box {{
        position: absolute; top: 10px; left: 50%; transform: translateX(-50%); z-index: 1000;
        background: rgba(15,23,42,0.95); border: 1px solid #334155; border-radius: 8px;
        padding: 8px 12px; backdrop-filter: blur(8px);
    }}
    .search-box input {{
        background: transparent; border: none; color: #e2e8f0; font-size: 14px;
        width: 300px; outline: none;
    }}
    .search-box input::placeholder {{ color: #64748b; }}
</style>
</head>
<body>
<div id="map"></div>

<div class="info-panel">
    <h2>Lecturer (I Gr.) Mathematics</h2>
    <div class="subtitle">Vacant positions across Rajasthan</div>
    <div class="stat"><span>Total schools</span><span class="stat-val" id="total-count">{len(schools_with_coords)}</span></div>
    <div class="stat"><span>Districts</span><span class="stat-val">{len(districts)}</span></div>
    <div class="stat"><span>Showing</span><span class="stat-val" id="showing-count">{len(schools_with_coords)}</span></div>
    <div style="margin-top:8px;color:#64748b;font-size:10px;">
        Data: rajshaladarpan.rajasthan.gov.in<br>
        Click any pin for school details
    </div>
</div>

<div class="search-box">
    <input type="text" id="search" placeholder="Search school, district, or village..." />
</div>

<div class="filter-panel" id="filter-panel">
    <h3>Districts</h3>
    <div id="district-filters"></div>
</div>

<script>
{markers_js}
{colors_js}

var map = L.map('map', {{
    center: [26.5, 73.8],
    zoom: 7,
    zoomControl: true
}});

L.tileLayer('https://{{s}}.google.com/vt/lyrs=m&x={{x}}&y={{y}}&z={{z}}', {{
    maxZoom: 20,
    subdomains: ['mt0', 'mt1', 'mt2', 'mt3'],
    attribution: 'Google Maps'
}}).addTo(map);

var allMarkerLayers = [];
var districtVisible = {{}};

// Count per district
var districtCounts = {{}};
markers.forEach(function(m) {{
    districtCounts[m[3]] = (districtCounts[m[3]] || 0) + 1;
}});

// Create markers
markers.forEach(function(m) {{
    var color = districtColors[m[3]] || '#3b82f6';
    var icon = L.divIcon({{
        className: 'custom-pin',
        html: '<div style="width:12px;height:12px;border-radius:50%;background:' + color + ';border:2px solid #fff;box-shadow:0 1px 4px rgba(0,0,0,0.4);"></div>',
        iconSize: [12, 12],
        iconAnchor: [6, 6],
        popupAnchor: [0, -8]
    }});
    var marker = L.marker([m[0], m[1]], {{icon: icon}}).bindPopup(m[2], {{maxWidth: 380}});
    marker._district = m[3];
    marker._popup_text = m[2].toLowerCase();
    marker.addTo(map);
    allMarkerLayers.push(marker);
    districtVisible[m[3]] = true;
}});

// Build district filter checkboxes
var filterDiv = document.getElementById('district-filters');
var sortedDistricts = Object.keys(districtCounts).sort();
sortedDistricts.forEach(function(d) {{
    var div = document.createElement('label');
    div.className = 'filter-item';
    div.innerHTML = '<input type="checkbox" checked data-district="' + d + '"> ' +
        '<span class="color-dot" style="background:' + (districtColors[d] || '#3b82f6') + '"></span> ' +
        d + '<span class="filter-count">' + districtCounts[d] + '</span>';
    div.querySelector('input').addEventListener('change', function() {{
        districtVisible[d] = this.checked;
        updateVisibility();
    }});
    filterDiv.appendChild(div);
}});

function updateVisibility() {{
    var searchTerm = document.getElementById('search').value.toLowerCase();
    var showing = 0;
    allMarkerLayers.forEach(function(marker) {{
        var visible = districtVisible[marker._district];
        if (searchTerm && !marker._popup_text.includes(searchTerm)) {{
            visible = false;
        }}
        if (visible) {{
            marker.addTo(map);
            showing++;
        }} else {{
            map.removeLayer(marker);
        }}
    }});
    document.getElementById('showing-count').textContent = showing;
}}

document.getElementById('search').addEventListener('input', function() {{
    updateVisibility();
}});
</script>
</body>
</html>"""
    return html


def main():
    schools = json.load(open(DATA_DIR / "all_444_schools_enriched.json"))
    print(f"Loaded {len(schools)} schools")

    cache = load_cache()
    print(f"Geocode cache: {len(cache)} entries")

    # Geocode missing schools
    to_geocode = [s for s in schools if str(s["SCHCD"]) not in cache]
    print(f"Need to geocode: {len(to_geocode)} schools")

    for i, school in enumerate(to_geocode):
        code = str(school["SCHCD"])
        if code in cache:
            continue

        result = geocode_school(school)
        if result:
            lat, lon, query = result
            cache[code] = {"lat": lat, "lon": lon, "query": query}
            if (i + 1) % 20 == 0:
                save_cache(cache)
                print(f"  {i+1}/{len(to_geocode)} geocoded...")
        else:
            cache[code] = None
            print(f"  MISS: {school['SchoolName'][:50]}")

    save_cache(cache)
    print(f"Geocoding done. {sum(1 for v in cache.values() if v)} found, {sum(1 for v in cache.values() if not v)} missed")

    # Attach coords to schools
    schools_with_coords = []
    for s in schools:
        code = str(s["SCHCD"])
        geo = cache.get(code)
        if geo:
            s["_lat"] = geo["lat"]
            s["_lon"] = geo["lon"]
            schools_with_coords.append(s)

    print(f"Schools with coordinates: {len(schools_with_coords)}/{len(schools)}")

    # Generate map
    MAP_DIR.mkdir(exist_ok=True)
    html = generate_map_html(schools_with_coords)
    map_path = MAP_DIR / "index.html"
    map_path.write_text(html)
    print(f"Map saved to {map_path}")
    print(f"Open: file://{map_path.resolve()}")


if __name__ == "__main__":
    main()
