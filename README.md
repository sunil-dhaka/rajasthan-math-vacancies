# Rajasthan Math Lecturer Vacancy Map

Interactive map of **444 schools** (379 geocoded) with vacant **Lecturer (I Gr.) Mathematics** positions across all 41 districts of Rajasthan.

**Live:** https://rajasthan-math-vacancies.vercel.app

## What This Shows

Every colored dot is a government school that has an unfilled Lecturer First Grade Mathematics position. Click any dot to see:
- School name, UDISE code, Shaladarpan code
- District, block, village, PIN code
- Principal name and email
- Rural/Urban classification
- Class range and school category
- Streams and subjects offered
- Direct Google Maps link

## Data

| File | Description |
|------|-------------|
| `data/all_444_schools_enriched.json` | All 444 schools with full profile data |
| `data/all_rajasthan_math_lecturer_vacancies.json` | Raw vacancy search results |
| `data/geocode_cache.json` | Geocoded coordinates (379/444 found) |
| `data/vacancy_matrix.csv` | District x Subject vacancy matrix (41 districts, 22 subjects) |
| `data/vacancy_matrix.json` | Same matrix in JSON |

## Scripts

All scripts use `uv run --with requests --with beautifulsoup4`.

| Script | Purpose |
|--------|---------|
| `scripts/fetch_vacancies.py` | Fetch vacancy school lists per district (flg=23 API) |
| `scripts/fetch_vacancy_matrix.py` | Build the full district x subject vacancy matrix |
| `scripts/scrape_shaladarpan.py` | Original scraper with staff table verification |
| `scripts/scrape_thorough.py` | Thorough cross-check scraper (all schools, not just vacancy API) |
| `scripts/geocode_schools.py` | Geocode schools using Nominatim (IPv4 forced) |
| `scripts/geocode_and_build_map.py` | Geocode + generate map HTML |

## Refreshing Data

```bash
# 1. Fetch latest vacancy data (41 API calls, ~2 min)
uv run --with requests python scripts/fetch_vacancies.py

# 2. Geocode any new schools (saves to cache, only geocodes missing ones)
uv run python scripts/geocode_schools.py

# 3. Rebuild the map HTML
uv run --with requests python scripts/geocode_and_build_map.py

# 4. Deploy
cd /path/to/this/repo
vercel --prod --yes
```

## Deploying

```bash
vercel --prod --yes
```

The Vercel project is linked as `rajasthan-math-vacancies`. Only `index.html` is deployed -- data and scripts are for reference.

## Source

Data from [rajshaladarpan.rajasthan.gov.in](https://rajshaladarpan.rajasthan.gov.in) via the Getdata API (flg=23, eleSecType=3, subid=39). See [shala-darpan](https://github.com/sunil-dhaka/shala-darpan) for full API documentation.

## License

MIT
