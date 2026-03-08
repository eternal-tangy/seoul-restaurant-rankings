# Seoul Restaurant Rankings — YouTube Script Generator

## Project Overview
Data-driven pipeline that ranks the top 5 restaurants by category and neighborhood in Seoul,
then generates short-form YouTube video scripts. Data sourced from Seoul Open Data Portal.

## Tech Stack
- Language: Python 3.11+
- Package manager: pip + venv
- Data processing: pandas
- HTTP requests: requests or httpx
- Database: SQLite (via sqlite3 or SQLAlchemy)
- Testing: pytest (`pytest`)
- Linting: ruff (`ruff check .`)
- Formatting: black (`black .`)

## Project Structure
- `data/raw/`        — downloaded data from Seoul Open Data Portal (never committed)
- `data/processed/`  — cleaned, scored datasets
- `src/pipeline/`    — data fetching and processing modules
- `src/scoring/`     — restaurant ranking and scoring logic
- `src/scripts/`     — video script generation
- `tests/`           — pytest test files mirroring src/ structure
- `outputs/`         — generated video scripts (markdown or txt)

## Workflow
- Always run `ruff check .` before committing
- Always run `pytest` after code changes
- Use `python -m src.pipeline.fetch` to pull fresh data
- Use `python -m src.scripts.generate` to produce video scripts

## Data Sources (Seoul Open Data Portal)
- Commercial district analysis: 상권분석 데이터
- Card payment data: 카드소비 데이터
- Foot traffic data: 유동인구 데이터
- District/neighborhood codes: 행정동 코드 기준

## Scoring Logic
- Restaurants are scored and ranked within a specific (구, 동, category) combination
- Score is a weighted composite of: foot traffic, card payment volume, commercial density
- Output: ranked top 5 per query (e.g., "Top 5 Sushi in Dohwa-dong, Mapo-gu")

## Script Output Format
- Title: "Top 5 [Category] in [Neighborhood], [District], Seoul"
- One section per restaurant (#1 through #5), in ranked order
- Each entry: name, address, score rationale, 1–2 highlight sentences
- Tone: informative, upbeat, suitable for short-form video narration

## What NOT to Do
- Do not hardcode API keys — use .env file with python-dotenv
- Do not commit data/raw/ or .env files
- Do not mix Korean and English in the same variable/function names
- Do not skip null checks on API responses — Seoul Open Data Portal returns inconsistent formats
