import html
import json
import os
import re
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, quote, urlparse
from urllib.request import Request, urlopen

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Accept": "application/json, text/html;q=0.9,*/*;q=0.8",
}

DB_PATH = os.path.join("data", "stackledger.db")
CACHE_TTL_MINUTES = 15

# East Africa Time (UTC+3) for Nairobi display timestamps
EAT = timezone(timedelta(hours=3))


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def eat_now() -> datetime:
    return datetime.now(EAT)


def parse_iso_utc(ts: str) -> datetime:
    dt = datetime.fromisoformat(ts)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


# Step 1: Expanded verified Kenyan & East African ATS career boards
KENYA_ATS_BOARDS = [
    ("ashby", "andela", "Andela"),
    ("ashby", "M-KOPA", "M-KOPA"),
    ("lever", "apolloagriculture", "Apollo Agriculture"),
    ("lever", "paystack", "Paystack"),
    ("greenhouse", "canonical", "Canonical"),
    ("greenhouse", "givedirectly", "GiveDirectly"),
    ("greenhouse", "zipline", "Zipline"),
    ("greenhouse", "tala", "Tala"),
    ("greenhouse", "wave", "Wave Mobile Money"),
    ("greenhouse", "flutterwave", "Flutterwave"),
    ("greenhouse", "wasoko", "Wasoko"),
    ("greenhouse", "sunking", "Sun King"),
    ("greenhouse", "oneacrefund", "One Acre Fund"),
]

TECH_RULES = [
    {"name": "AWS", "category": "Cloud Platforms", "patterns": [r"\b(?:aws|amazon web services|ec2|s3|eks|iam|lambda)\b"]},
    {"name": "Azure", "category": "Cloud Platforms", "patterns": [r"\b(?:azure|microsoft azure|aks)\b"]},
    {"name": "GCP", "category": "Cloud Platforms", "patterns": [r"\b(?:gcp|google cloud|gke|bigquery)\b"]},
    {"name": "Linux", "category": "Infrastructure & DevOps", "patterns": [r"\b(?:linux|ubuntu|rhel|centos|bash|shell scripting)\b"]},
    {"name": "Networking", "category": "Infrastructure & DevOps", "patterns": [r"\b(?:networking|vpc|dns|tcp/ip|load balanc(?:er|ing)|firewall|vpn|subnet)\b"]},
    {"name": "Terraform", "category": "Infrastructure & DevOps", "patterns": [r"\b(?:terraform|iac|infrastructure as code|cloudformation)\b"]},
    {"name": "Docker", "category": "Infrastructure & DevOps", "patterns": [r"\b(?:docker|containers?|containeri[sz]ation)\b"]},
    {"name": "Kubernetes", "category": "Infrastructure & DevOps", "patterns": [r"\b(?:kubernetes|k8s|helm|eks|gke|aks)\b"]},
    {"name": "CI/CD", "category": "Infrastructure & DevOps", "patterns": [r"\b(?:ci/cd|github actions|gitlab ci|jenkins|argo ?cd)\b"]},
    {"name": "Python", "category": "Programming & Scripting", "patterns": [r"\bpython\b"]},
    {"name": "Go", "category": "Programming & Scripting", "patterns": [r"\b(?:golang|go)\b"]},
    {"name": "TypeScript", "category": "Programming & Scripting", "patterns": [r"\btypescript\b"]},
    {"name": "JavaScript", "category": "Programming & Scripting", "patterns": [r"\b(?:javascript|node(?:\.js|js)?)\b"]},
    {"name": "Java", "category": "Programming & Scripting", "patterns": [r"\bjava\b"]},
    {"name": "PostgreSQL", "category": "Databases & Data", "patterns": [r"\bpostgres(?:ql)?\b"]},
    {"name": "SQL", "category": "Databases & Data", "patterns": [r"\b(?:sql|mysql|rds|dynamodb|mongodb|redis)\b"]},
    {"name": "Data / ETL", "category": "Databases & Data", "patterns": [r"\b(?:etl|airflow|spark|kafka|dbt|snowflake)\b"]},
    {"name": "React", "category": "Frameworks & Web", "patterns": [r"\b(?:react(?:js|\.js)?|react native)\b"]},
    {"name": "Django / FastAPI", "category": "Frameworks & Web", "patterns": [r"\b(?:django|fastapi|flask)\b"]},
]


# --- STEP 3: SQLITE PERSISTENCE, CACHE & HISTORICAL SNAPSHOTS ---

def init_db():
    os.makedirs("data", exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS search_cache (
                query_key TEXT PRIMARY KEY,
                payload_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS skill_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                snapshot_date TEXT NOT NULL,
                query_key TEXT NOT NULL,
                scope TEXT NOT NULL,
                sample_size INTEGER NOT NULL,
                skill_name TEXT NOT NULL,
                category TEXT NOT NULL,
                mention_count INTEGER NOT NULL,
                demand_pct REAL NOT NULL,
                UNIQUE(snapshot_date, query_key, scope, skill_name)
            )
        """)


def get_cached_feed(query_key):
    try:
        with sqlite3.connect(DB_PATH) as conn:
            row = conn.execute(
                "SELECT payload_json, created_at FROM search_cache WHERE query_key = ?",
                (query_key,),
            ).fetchone()
            if row:
                created_at = parse_iso_utc(row[1])
                if utc_now() - created_at < timedelta(minutes=CACHE_TTL_MINUTES):
                    payload = json.loads(row[0])
                    payload["from_cache"] = True
                    return payload
    except Exception:
        pass
    return None


def save_feed_and_snapshot(query_key, payload):
    now_utc = utc_now()
    today = eat_now().strftime("%Y-%m-%d")
    now_iso = now_utc.isoformat()
    jobs = payload.get("jobs", [])

    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO search_cache (query_key, payload_json, created_at) VALUES (?, ?, ?)",
                (query_key, json.dumps(payload), now_iso),
            )

            # Record daily demand snapshots per scope ("ALL" and "Kenya (Local)")
            for scope_name in ("ALL", "Kenya (Local)"):
                scoped = jobs if scope_name == "ALL" else [j for j in jobs if j.get("scope") == scope_name]
                n = len(scoped)
                if n == 0:
                    continue
                counts = {}
                for j in scoped:
                    for hit in j.get("skill_hits", []):
                        key = (hit["name"], hit["category"])
                        counts[key] = counts.get(key, 0) + 1

                for (skill_name, category), count in counts.items():
                    pct = round((count / n) * 100.0, 2)
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO skill_snapshots
                        (snapshot_date, query_key, scope, sample_size, skill_name, category, mention_count, demand_pct)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (today, query_key, scope_name, n, skill_name, category, count, pct),
                    )
    except Exception:
        pass


def get_historical_trends(query_key, scope="ALL"):
    try:
        with sqlite3.connect(DB_PATH) as conn:
            rows = conn.execute(
                """
                SELECT snapshot_date, skill_name, category, demand_pct, mention_count, sample_size
                FROM skill_snapshots
                WHERE query_key = ? AND scope = ?
                ORDER BY snapshot_date ASC
                """,
                (query_key.lower().strip(), scope),
            ).fetchall()
            return [
                {
                    "date": r[0],
                    "skill": r[1],
                    "category": r[2],
                    "demand_pct": r[3],
                    "count": r[4],
                    "sample_size": r[5],
                }
                for r in rows
            ]
    except Exception:
        return []


# --- DATA FETCHERS ---

def fetch_url(url, timeout=12):
    req = Request(url, headers=HEADERS)
    with urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", errors="ignore")


def fetch_json(url, timeout=12):
    return json.loads(fetch_url(url, timeout=timeout))


def strip_html(text):
    clean = re.sub(r"<[^>]+>", " ", html.unescape(str(text or "")))
    return re.sub(r"\s+", " ", clean).strip()


def extract_skill_evidence(title, description):
    full_text = f"{title}. {strip_html(description)}"
    matched = []
    for rule in TECH_RULES:
        for pat in rule["patterns"]:
            m = re.search(pat, full_text, flags=re.IGNORECASE)
            if m:
                start = max(0, m.start() - 45)
                end = min(len(full_text), m.end() + 55)
                snippet = full_text[start:end].strip()
                if start > 0:
                    snippet = "..." + snippet
                if end < len(full_text):
                    snippet = snippet + "..."
                matched.append({
                    "name": rule["name"],
                    "category": rule["category"],
                    "snippet": snippet,
                })
                break
    return matched


def matches_query(text, query):
    if not query or query.strip().lower() in ("*", "all"):
        return True
    terms = [t.strip().lower() for t in query.split() if t.strip()]
    haystack = str(text or "").lower()
    return all(t in haystack for t in terms)


def classify_scope(location_text):
    loc = str(location_text or "").lower()
    if any(k in loc for k in ["kenya", "nairobi", "mombasa", "kisumu", "nakuru", "eldoret", "east africa", "africa", "emea"]):
        return "Kenya (Local)"
    return "International (Remote)"


def fetch_brightermonday_ke(query):
    jobs = []
    today_eat = eat_now().strftime("%Y-%m-%d")
    try:
        q_slug = quote(query) if query else ""
        url = f"https://www.brightermonday.co.ke/jobs/software-data?q={q_slug}" if q_slug else "https://www.brightermonday.co.ke/jobs/software-data"
        page = fetch_url(url, timeout=12)

        ld_blocks = re.findall(r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', page, flags=re.S | re.I)
        for block in ld_blocks:
            try:
                data = json.loads(block.strip())
                items = data if isinstance(data, list) else [data]
                for item in items:
                    if item.get("@type") == "JobPosting":
                        title = strip_html(item.get("title", ""))
                        company = strip_html((item.get("hiringOrganization") or {}).get("name", "Kenya Employer"))
                        desc = item.get("description", "")
                        loc_obj = item.get("jobLocation") or {}
                        if isinstance(loc_obj, list):
                            loc_obj = loc_obj[0] if loc_obj else {}
                        city = ((loc_obj.get("address") or {}).get("addressLocality")) or "Nairobi, Kenya"
                        if title and matches_query(f"{title} {desc}", query):
                            hits = extract_skill_evidence(title, desc)
                            jobs.append({
                                "title": title,
                                "company": company,
                                "location": city,
                                "scope": "Kenya (Local)",
                                "source": "BrighterMonday KE",
                                "posted": (item.get("datePosted") or "")[:10] or today_eat,
                                "salary": "",
                                "url": item.get("url") or url,
                                "skill_hits": hits,
                                "tech_stack": [s["name"] for s in hits],
                            })
            except Exception:
                continue

        if not jobs:
            cards = re.findall(r'<a[^>]+href="(https://www\.brightermonday\.co\.ke/listings/[^"]+)"[^>]*>(.*?)</a>', page, flags=re.S | re.I)
            seen_urls = set()
            for link, inner in cards:
                if link in seen_urls:
                    continue
                seen_urls.add(link)
                title = strip_html(inner)
                if len(title) > 4 and matches_query(title, query):
                    hits = extract_skill_evidence(title, title)
                    jobs.append({
                        "title": title[:100],
                        "company": "Kenyan Employer (BrighterMonday)",
                        "location": "Nairobi, Kenya",
                        "scope": "Kenya (Local)",
                        "source": "BrighterMonday KE",
                        "posted": today_eat,
                        "salary": "",
                        "url": link,
                        "skill_hits": hits,
                        "tech_stack": [s["name"] for s in hits],
                    })
    except Exception:
        pass
    return jobs


def fetch_myjobmag_ke(query):
    jobs = []
    today_eat = eat_now().strftime("%Y-%m-%d")
    try:
        url = "https://www.myjobmag.co.ke/jobs-by-field/information-technology"
        page = fetch_url(url, timeout=12)
        entries = re.findall(r'<a[^>]+href="(/job/[^"]+)"[^>]*>(.*?)</a>', page, flags=re.S | re.I)
        seen_links = set()
        for rel_link, raw_title in entries:
            if rel_link in seen_links:
                continue
            seen_links.add(rel_link)
            title = strip_html(raw_title)
            if len(title) > 5 and matches_query(title, query):
                parts = title.split(" at ")
                role_title = parts[0].strip()
                company = parts[1].strip() if len(parts) > 1 else "Kenyan Organization"
                hits = extract_skill_evidence(role_title, title)
                jobs.append({
                    "title": role_title,
                    "company": company,
                    "location": "Nairobi, Kenya",
                    "scope": "Kenya (Local)",
                    "source": "MyJobMag KE",
                    "posted": today_eat,
                    "salary": "",
                    "url": f"https://www.myjobmag.co.ke{rel_link}",
                    "skill_hits": hits,
                    "tech_stack": [s["name"] for s in hits],
                })
    except Exception:
        pass
    return jobs


def fetch_jobdata_ke(query):
    jobs = []
    today_eat = eat_now().strftime("%Y-%m-%d")
    try:
        data = fetch_json("https://jobdataapi.com/api/jobs/?country_code=KE", timeout=10)
        results = data.get("results", data) if isinstance(data, dict) else data
        for item in results[:20]:
            title = str(item.get("title", "")).strip()
            comp = item.get("company")
            company = comp.get("name") if isinstance(comp, dict) else (item.get("company_name") or "Kenya Employer")
            desc = item.get("description", "")
            loc = item.get("location") or "Nairobi, Kenya"
            if title and matches_query(f"{title} {desc}", query):
                hits = extract_skill_evidence(title, desc)
                jobs.append({
                    "title": title,
                    "company": company,
                    "location": loc,
                    "scope": "Kenya (Local)",
                    "source": "JobData KE",
                    "posted": str(item.get("published") or "")[:10] or today_eat,
                    "salary": "",
                    "url": item.get("url") or "#",
                    "skill_hits": hits,
                    "tech_stack": [s["name"] for s in hits],
                })
    except Exception:
        pass
    return jobs


def fetch_ats_board(board, query):
    ats, slug, company_name = board
    jobs = []
    today_eat = eat_now().strftime("%Y-%m-%d")
    try:
        if ats == "ashby":
            data = fetch_json(f"https://api.ashbyhq.com/posting-api/job-board/{slug}", timeout=10)
            for j in data.get("jobs", []):
                title = j.get("title", "").strip()
                loc = j.get("location", "Nairobi, Kenya").strip()
                desc = j.get("descriptionPlain") or j.get("descriptionHtml") or ""
                if title and matches_query(f"{title} {company_name} {desc}", query):
                    hits = extract_skill_evidence(title, desc)
                    jobs.append({
                        "title": title,
                        "company": company_name,
                        "location": loc,
                        "scope": classify_scope(loc),
                        "source": f"{company_name} (Ashby)",
                        "posted": (j.get("publishedAt") or "")[:10] or today_eat,
                        "salary": "",
                        "url": j.get("jobUrl") or j.get("applyUrl") or "#",
                        "skill_hits": hits,
                        "tech_stack": [s["name"] for s in hits],
                    })
        elif ats == "greenhouse":
            data = fetch_json(f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true", timeout=10)
            for j in data.get("jobs", []):
                title = j.get("title", "").strip()
                loc = ((j.get("location") or {}).get("name") or "Remote").strip()
                content = j.get("content", "")
                if title and matches_query(f"{title} {company_name}", query):
                    hits = extract_skill_evidence(title, content)
                    jobs.append({
                        "title": title,
                        "company": company_name,
                        "location": loc,
                        "scope": classify_scope(loc),
                        "source": f"{company_name} (Greenhouse)",
                        "posted": (j.get("updated_at") or "")[:10] or today_eat,
                        "salary": "",
                        "url": j.get("absolute_url", "#"),
                        "skill_hits": hits,
                        "tech_stack": [s["name"] for s in hits],
                    })
        elif ats == "lever":
            data = fetch_json(f"https://api.lever.co/v0/postings/{slug}?mode=json", timeout=10)
            for j in data:
                title = j.get("text", "").strip()
                loc = ((j.get("categories") or {}).get("location") or "Remote").strip()
                desc = j.get("descriptionPlain", "")
                if title and matches_query(f"{title} {company_name}", query):
                    hits = extract_skill_evidence(title, desc)
                    jobs.append({
                        "title": title,
                        "company": company_name,
                        "location": loc,
                        "scope": classify_scope(loc),
                        "source": f"{company_name} (Lever)",
                        "posted": today_eat,
                        "salary": "",
                        "url": j.get("hostedUrl", "#"),
                        "skill_hits": hits,
                        "tech_stack": [s["name"] for s in hits],
                    })
    except Exception:
        pass
    return jobs


def fetch_remotive(query):
    jobs = []
    today_eat = eat_now().strftime("%Y-%m-%d")
    try:
        q = quote(query or "software")
        data = fetch_json(f"https://remotive.com/api/remote-jobs?search={q}&limit=40", timeout=10)
        for item in data.get("jobs", []):
            title = item.get("title", "").strip()
            company = item.get("company_name", "Unknown").strip()
            loc = item.get("candidate_required_location", "Worldwide").strip()
            desc = item.get("description", "")
            if title and matches_query(f"{title} {company}", query):
                hits = extract_skill_evidence(title, desc)
                jobs.append({
                    "title": title,
                    "company": company,
                    "location": loc,
                    "scope": classify_scope(loc),
                    "source": "Remotive",
                    "posted": (item.get("publication_date") or "")[:10] or today_eat,
                    "salary": item.get("salary") or "",
                    "url": item.get("url", "#"),
                    "skill_hits": hits,
                    "tech_stack": [s["name"] for s in hits],
                })
    except Exception:
        pass
    return jobs


def fetch_remoteok(query):
    jobs = []
    today_eat = eat_now().strftime("%Y-%m-%d")
    try:
        data = fetch_json("https://remoteok.com/api", timeout=10)
        for item in data[1:]:
            title = item.get("position", "").strip()
            company = item.get("company", "Unknown").strip()
            tags = " ".join(item.get("tags") or [])
            loc = (item.get("location") or "Remote").strip()
            desc = item.get("description", "")
            if title and matches_query(f"{title} {tags}", query):
                hits = extract_skill_evidence(title, f"{tags}. {desc}")
                jobs.append({
                    "title": title,
                    "company": company,
                    "location": loc,
                    "scope": classify_scope(loc),
                    "source": "RemoteOK",
                    "posted": (item.get("date") or "")[:10] or today_eat,
                    "salary": "",
                    "url": item.get("url", "#"),
                    "skill_hits": hits,
                    "tech_stack": [s["name"] for s in hits],
                })
            if len(jobs) >= 35:
                break
    except Exception:
        pass
    return jobs


def fetch_github_momentum():
    since_date = (utc_now() - timedelta(days=14)).strftime("%Y-%m-%d")
    languages = ["Python", "Go", "TypeScript", "JavaScript", "Rust", "Java"]

    def query_lang(lang):
        try:
            q = quote(f"language:{lang} pushed:>={since_date} archived:false")
            data = fetch_json(f"https://api.github.com/search/repositories?q={q}&per_page=1", timeout=8)
            return {"language": lang, "active_repos": int(data.get("total_count", 0))}
        except Exception:
            return {"language": lang, "active_repos": 0}

    with ThreadPoolExecutor(max_workers=6) as pool:
        return list(pool.map(query_lang, languages))


def build_live_feed(query, force_refresh=False):
    query_key = (query or "software").lower().strip()
    if not force_refresh:
        cached = get_cached_feed(query_key)
        if cached:
            return cached

    raw_jobs = []
    with ThreadPoolExecutor(max_workers=16) as pool:
        futures = [
            pool.submit(fetch_brightermonday_ke, query),
            pool.submit(fetch_myjobmag_ke, query),
            pool.submit(fetch_jobdata_ke, query),
            pool.submit(fetch_remotive, query),
            pool.submit(fetch_remoteok, query),
        ]
        for board in KENYA_ATS_BOARDS:
            futures.append(pool.submit(fetch_ats_board, board, query))

        gh_future = pool.submit(fetch_github_momentum)

        for f in futures:
            raw_jobs.extend(f.result())
        github_momentum = gh_future.result()

    seen = set()
    jobs = []
    for job in raw_jobs:
        key = (job["title"].lower(), job["company"].lower())
        if key not in seen:
            seen.add(key)
            jobs.append(job)

    now_utc = utc_now()
    now_eat = now_utc.astimezone(EAT)

    payload = {
        "query": query,
        "synced_at": now_eat.strftime("%Y-%m-%d %H:%M:%S EAT"),
        "synced_at_utc": now_utc.isoformat(),
        "from_cache": False,
        "total_jobs": len(jobs),
        "jobs": jobs,
        "github_momentum": github_momentum,
    }
    save_feed_and_snapshot(query_key, payload)
    return payload


class StackLedgerHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory="dashboard", **kwargs)

    def send_json(self, obj, status=200):
        raw = json.dumps(obj).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self):
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)

        if parsed.path == "/api/live-feed":
            query = params.get("what", ["software"])[0].strip()
            force = params.get("refresh", ["0"])[0] == "1"
            self.send_json(build_live_feed(query, force_refresh=force))
        elif parsed.path == "/api/trends":
            query = params.get("what", ["software"])[0].strip()
            scope = params.get("scope", ["ALL"])[0].strip()
            self.send_json({
                "query": query,
                "scope": scope,
                "trends": get_historical_trends(query, scope),
            })
        else:
            super().do_GET()


if __name__ == "__main__":
    init_db()
    os.makedirs("dashboard", exist_ok=True)
    host = "0.0.0.0"
    port = int(os.environ.get("PORT", 8080))
    server = ThreadingHTTPServer((host, port), StackLedgerHandler)
    print(f"StackLedger live server running at http://127.0.0.1:{port} (bound to {host})")
    server.serve_forever()