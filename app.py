# -*- coding: utf-8 -*-
import csv
import io
import time
import re
import requests
from flask import Flask, request, render_template_string, send_file

from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

app = Flask(__name__)

API_BASE = "https://rest.arbeitsagentur.de/jobboerse/jobsuche-service/pc/v4"
SEARCH_URL = f"{API_BASE}/jobs"
JOBDETAIL_URL = "https://www.arbeitsagentur.de/jobsuche/jobdetail/{refnr}"

HEADERS = {
    "X-API-Key": "jobboerse-jobsuche",
    "User-Agent": "JobSearchBot/1.0",
}

LOGO_URL = (
    "https://ejzkepf.stripocdn.email/content/guids/"
    "CABINET_7fd94653830e80b0e60fc4cb1431872fa2b8115f0f3b7b9f53dd6a446bdc94bb/"
    "images/image_0BS.png"
)

MAX_PAGES = 50
EMAIL_RX = re.compile(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", re.I)
DATE_RX = re.compile(r"\d{4}-\d{2}-\d{2}")

LAST_RESULTS = [] 


def first_nonempty(*vals):
    for v in vals:
        if isinstance(v, str) and v.strip():
            return v.strip()
        if v:
            return v
    return None


def pick_date(item: dict):
    for v in item.values():
        if isinstance(v, str):
            m = DATE_RX.search(v)
            if m:
                return m.group(0)
    return None


def parse_jobdetail(refnr: str):

    if not refnr:
        return {"email": ""}

    url = JOBDETAIL_URL.format(refnr=refnr)
    try:
        r = requests.get(
            url,
            headers={"User-Agent": HEADERS["User-Agent"]},
            timeout=20,
            verify=False, 
        )
        if r.status_code != 200:
            return {"email": ""}
        html = r.text
    except Exception:
        return {"email": ""}

    emails = set(EMAIL_RX.findall(html))
    email = sorted(emails)[0] if emails else ""
    return {"email": email}


def fetch_jobs(keyword, wo, umkreis, exclude_pav):

    base_params = {
        "wo": wo,
        "umkreis": umkreis,
        "angebotsart": 1,
        "size": 100,
    }
    if exclude_pav:
        base_params["pav"] = "false"
        base_params["zeitarbeit"] = "false"

    jobs_raw = {}

    if isinstance(keyword, str):
        keywords = [k.strip() for k in keyword.split(",") if k.strip()]
    else:
        keywords = keyword

    for kw in keywords:
        if not kw:
            continue

        print(f"Fetching jobs for keyword: '{kw}'")
        for page in range(1, MAX_PAGES + 1):
            params = base_params.copy()
            params["was"] = kw
            params["page"] = page

            resp = requests.get(SEARCH_URL, headers=HEADERS, params=params, timeout=60)
            if resp.status_code == 400:
                break

            data = resp.json()
            items = data.get("stellenangebote") or data.get("content") or []
            print(f"Fetched {len(items)} items on page {page} for '{kw}'")
            if not items:
                break

            for it in items:
                refnr = it.get("refnr")
                titel = first_nonempty(
                    it.get("titel"), it.get("stellenbezeichnung"), it.get("beruf")
                )

                ag = it.get("arbeitgeber") or {}
                arbeitgeber = first_nonempty(
                    ag.get("name") if isinstance(ag, dict) else ag
                )

                ao = it.get("arbeitsort") or {}
                ort = first_nonempty(ao.get("ort") if isinstance(ao, dict) else None)

                key = first_nonempty(refnr, f"{titel}|{arbeitgeber}|{ort}")
                if key not in jobs_raw:
                    jobs_raw[key] = it

            if len(items) < base_params["size"]:
                break
            time.sleep(0.2)

    print(f"Total raw jobs collected: {len(jobs_raw)}")
    return jobs_raw


def enrich_jobs(jobs_raw):

    results = []
    for key, it in jobs_raw.items():
        refnr = it.get("refnr")
        contact = parse_jobdetail(refnr)
        time.sleep(0.25)

        ag = it.get("arbeitgeber") or {}
        arbeitgeber = first_nonempty(
            ag.get("name") if isinstance(ag, dict) else ag
        )

        ao = it.get("arbeitsort") or {}
        arbeitsort = first_nonempty(ao.get("ort") if isinstance(ao, dict) else None)
        plz = ao.get("plz") if isinstance(ao, dict) else ""

        titel = first_nonempty(
            it.get("titel"), it.get("stellenbezeichnung"), it.get("beruf")
        )
        published = pick_date(it)

        link = (
            f"https://www.arbeitsagentur.de/jobsuche/jobdetail/{refnr}" if refnr else ""
        )

        results.append(
            {
                "titel": titel or "",
                "arbeitgeber": arbeitgeber or "",
                "arbeitsort": arbeitsort or "",
                "plz": plz or "",
                "published": published or "",
                "refnr": refnr or "",
                "link": link,
                "email": contact.get("email", ""),
            }
        )

    return results




HTML_TEMPLATE = """
<!doctype html>
<html lang="de">
<head>
  <meta charset="utf-8">
  <title>Stellensuche (arbeitsagentur.de)</title>
  <link rel="stylesheet"
        href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css">
  <style>
    body {
        background: #f3fef3;
    }
    .brand-logo {
        height: 70px;
    }
    .table thead th {
        background: #2f7d32 !important;
        color: #ffffff !important;
        font-weight: 600;
        font-size: 14px;
    }
    .table tbody tr:nth-child(odd) { background: #eaf7ea; }
    .table tbody tr:nth-child(even) { background: #ffffff; }

    .sortable {
        cursor: pointer;
        user-select: none;
        white-space: nowrap;
    }
    .sort-arrow {
        font-size: 0.7rem;
        margin-left: 4px;
        visibility: hidden;
        opacity: 0.4;
    }
    th.sorted-asc .sort-arrow.asc,
    th.sorted-desc .sort-arrow.desc {
        visibility: visible;
        opacity: 1;
    }

    /* оверлей загрузки */
    #loadingOverlay {
        position: fixed;
        inset: 0;
        background: rgba(255,255,255,0.92);
        display: none;
        align-items: center;
        justify-content: center;
        z-index: 1050;
    }
    .loading-content {
        text-align: center;
        animation: fadeIn 0.35s ease-out;
    }
    .loading-logo {
        height: 80px;
        margin-bottom: 16px;
        animation: pulse 1.5s ease-in-out infinite;
    }
    .loader-circle {
        width: 40px;
        height: 40px;
        margin: 0 auto 12px;
        border-radius: 50%;
        border: 4px solid #2f7d32;
        border-top-color: transparent;
        animation: spin 0.8s linear infinite;
    }
    .loading-text {
        font-size: 1rem;
        color: #1b5e20;
        font-weight: 500;
    }

    @keyframes pulse {
        0%   { transform: scale(1);   opacity: 0.7; }
        50%  { transform: scale(1.08); opacity: 1; }
        100% { transform: scale(1);   opacity: 0.7; }
    }
    @keyframes fadeIn {
        from { opacity: 0; transform: translateY(6px); }
        to   { opacity: 1; transform: translateY(0); }
    }
    @keyframes spin {
        to { transform: rotate(360deg); }
    }
  </style>
</head>
<body>
<div id="loadingOverlay">
  <div class="loading-content">
    <img src="{{ logo_url }}" alt="Logo" class="loading-logo">
    <div class="loader-circle"></div>
    <div class="loading-text">Lade Stellenangebote...</div>
  </div>
</div>

<div class="container py-4">

  <!-- верхняя панель: заголовок слева, логотип справа -->
  <div class="d-flex justify-content-between align-items-center mb-4">
      <h1 class="m-0">Stellensuche</h1>
      <a href="/" id="resetLink">
        <img src="{{ logo_url }}" class="brand-logo" alt="V&amp;E / Work &amp; Life Logo">
      </a>
  </div>

  <form id="searchForm" class="row g-3 mb-3" method="get" action="/">
    <div class="col-md-4">
      <label class="form-label">Schlüsselwörter</label>
      <input type="text" name="kw" class="form-control"
             placeholder="z.B. Schweißer, Schlosser"
             value="{{ kw }}">
      <div class="form-text">Mehrere Begriffe mit Komma trennen.</div>
    </div>

    <div class="col-md-3">
      <label class="form-label">PLZ / Ort</label>
      <input type="text" name="wo" class="form-control"
             value="{{ wo }}">
    </div>

    <div class="col-md-2">
      <label class="form-label">Umkreis (km)</label>
      <input type="number" name="umkreis" class="form-control"
             value="{{ umkreis }}">
    </div>

    <div class="col-md-3">
      <label class="form-label">Dateiname (Export)</label>
      <input type="text" name="fname" class="form-control"
             value="{{ fname }}">
    </div>

    <div class="col-md-3">
      <label class="form-label">Keine Personaldienstleister</label>
      <select class="form-select" name="exclude_pav">
          <option value="1" {% if exclude_pav %}selected{% endif %}>Ja</option>
          <option value="0" {% if not exclude_pav %}selected{% endif %}>Nein</option>
      </select>
    </div>

    <div class="col-12 d-flex align-items-end gap-2 mt-2">
      <button type="submit" class="btn btn-success px-4">Suchen</button>
      {% if results %}
      <a href="/export.csv?fname={{ fname|urlencode }}" class="btn btn-outline-secondary">
        CSV herunterladen
      </a>
      {% endif %}
    </div>
  </form>

  {% if results is not none %}
    <div class="d-flex justify-content-between align-items-center mt-3 mb-2">
      <h5 class="mb-0">Gefundene Stellen: {{ results|length }}</h5>
      <div class="form-check">
        <input class="form-check-input" type="checkbox" id="emailOnly"
               name="email_only" value="1" form="searchForm"
               {% if email_only %}checked{% endif %}>
        <label class="form-check-label" for="emailOnly">
          Nur Stellen mit E-Mail
        </label>
      </div>
    </div>

    {% if results %}
      <div class="table-responsive">
      <table class="table table-bordered table-striped align-middle">
        <thead>
          <tr>
            <th class="sortable" data-field="titel">
              STELLENTITEL
              <span class="sort-arrow asc">▲</span>
              <span class="sort-arrow desc">▼</span>
            </th>
            <th class="sortable" data-field="arbeitgeber">
              ARBEITGEBER
              <span class="sort-arrow asc">▲</span>
              <span class="sort-arrow desc">▼</span>
            </th>
            <th class="sortable" data-field="arbeitsort">
              ORT
              <span class="sort-arrow asc">▲</span>
              <span class="sort-arrow desc">▼</span>
            </th>
            <th class="sortable" data-field="plz">
              PLZ
              <span class="sort-arrow asc">▲</span>
              <span class="sort-arrow desc">▼</span>
            </th>
            <th class="sortable" data-field="published">
              PUBLISHED
              <span class="sort-arrow asc">▲</span>
              <span class="sort-arrow desc">▼</span>
            </th>
            <th class="sortable" data-field="email">
              E-MAIL
              <span class="sort-arrow asc">▲</span>
              <span class="sort-arrow desc">▼</span>
            </th>
            <th>LINK</th>
          </tr>
        </thead>
        <tbody>
        {% for r in results %}
          <tr>
            <td>{{ r.titel }}</td>
            <td>{{ r.arbeitgeber }}</td>
            <td>{{ r.arbeitsort }}</td>
            <td>{{ r.plz }}</td>
            <td>{{ r.published }}</td>
            <td>{{ r.email }}</td>
            <td>
              {% if r.link %}
                <a href="{{ r.link }}" target="_blank">Öffnen</a>
              {% endif %}
            </td>
          </tr>
        {% endfor %}
        </tbody>
      </table>
      </div>
    {% else %}
      <p>Keine Ergebnisse für diese Suche.</p>
    {% endif %}
  {% endif %}
</div>

<script>
 
  const form = document.getElementById('searchForm');
  const overlay = document.getElementById('loadingOverlay');

  if (form && overlay) {
      form.addEventListener('submit', function () {
          overlay.style.display = 'flex';
      });
  }

 
  const resetLink = document.getElementById('resetLink');
  if (resetLink) {
      resetLink.addEventListener('click', function (e) {
          e.preventDefault();
          window.location.href = '/';
      });
  }

  
  document.querySelectorAll('th.sortable').forEach(function (th, index) {
      th.addEventListener('click', function () {
          const table = th.closest('table');
          const tbody = table.tBodies[0];
          const rows = Array.from(tbody.querySelectorAll('tr'));

        
          const currentDir = th.dataset.sortDir === 'asc' ? 'desc' : 'asc';

          
          table.querySelectorAll('th.sortable').forEach(function (other) {
              other.dataset.sortDir = '';
              other.classList.remove('sorted-asc', 'sorted-desc');
          });

          th.dataset.sortDir = currentDir;
          th.classList.add(currentDir === 'asc' ? 'sorted-asc' : 'sorted-desc');

          rows.sort(function (a, b) {
              const aText = a.children[index].innerText.trim().toLowerCase();
              const bText = b.children[index].innerText.trim().toLowerCase();
              if (aText === bText) return 0;
              if (currentDir === 'asc') {
                  return aText > bText ? 1 : -1;
              } else {
                  return aText < bText ? 1 : -1;
              }
          });

          rows.forEach(function (row) {
              tbody.appendChild(row);
          });
      });
  });
</script>
</body>
</html>
"""


# --------------------------- маршруты ---------------------------------


@app.route("/", methods=["GET"])
def index():
    global LAST_RESULTS


    if not request.args:
        LAST_RESULTS = []
        return render_template_string(
            HTML_TEMPLATE,
            results=None,
            kw="Schweißer",
            wo="33689 Bielefeld",
            umkreis=25,
            exclude_pav=True,
            fname="jobs_ba.csv",
            email_only=False,
            logo_url=LOGO_URL,
        )

    kw = request.args.get("kw", "Schweißer")
    wo = request.args.get("wo", "33689 Bielefeld")
    umkreis = int(request.args.get("umkreis", "25") or 25)
    exclude_pav = request.args.get("exclude_pav", "1") == "1"
    fname = request.args.get("fname", "jobs_ba.csv").strip() or "jobs_ba.csv"
    email_only = request.args.get("email_only", "0") == "1"

    jobs_raw = fetch_jobs(kw, wo, umkreis, exclude_pav)
    results = enrich_jobs(jobs_raw)

    if email_only:
        results = [r for r in results if r.get("email")]

    LAST_RESULTS = results

    return render_template_string(
        HTML_TEMPLATE,
        results=results,
        kw=kw,
        wo=wo,
        umkreis=umkreis,
        exclude_pav=exclude_pav,
        fname=fname,
        email_only=email_only,
        logo_url=LOGO_URL,
    )


@app.route("/export.csv", methods=["GET"])
def export_csv():
    global LAST_RESULTS
    if not LAST_RESULTS:
        return "Keine Daten zum Exportieren", 400

    fname = request.args.get("fname", "jobs_ba.csv").strip() or "jobs_ba.csv"
    if "." not in fname:
        fname += ".csv"

    output = io.StringIO()
    writer = csv.DictWriter(
        output,
        fieldnames=[
            "titel",
            "arbeitgeber",
            "arbeitsort",
            "plz",
            "published",
            "refnr",
            "link",
            "email",
        ],
    )
    writer.writeheader()
    writer.writerows(LAST_RESULTS)
    output.seek(0)

    return send_file(
        io.BytesIO(output.getvalue().encode("utf-8")),
        mimetype="text/csv; charset=utf-8",
        as_attachment=True,
        download_name=fname,
    )


if __name__ == "__main__":
    app.run(debug=True)
