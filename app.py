# -*- coding: utf-8 -*-
import csv
import io
import time
import re
import requests
from flask import Flask, request, render_template_string, send_file

app = Flask(__name__)

API_BASE = "https://rest.arbeitsagentur.de/jobboerse/jobsuche-service/pc/v4"
SEARCH_URL = f"{API_BASE}/jobs"
JOBDETAIL_URL = "https://www.arbeitsagentur.de/jobsuche/jobdetail/{refnr}"

HEADERS = {
    "X-API-Key": "jobboerse-jobsuche",
    "User-Agent": "JobSearchBot/1.0",
}

MAX_PAGES = 50
EMAIL_RX = re.compile(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", re.I)
DATE_RX = re.compile(r"\d{4}-\d{2}-\d{2}")

LAST_RESULTS = []  # хранит результаты последнего поиска для экспорта


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
        r = requests.get(url, headers={"User-Agent": HEADERS["User-Agent"]}, timeout=30)
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
        for page in range(1, MAX_PAGES + 1):
            params = base_params.copy()
            params["was"] = kw
            params["page"] = page

            resp = requests.get(SEARCH_URL, headers=HEADERS, params=params, timeout=60)
            if resp.status_code == 400:
                break

            data = resp.json()
            items = data.get("stellenangebote") or data.get("content") or []
            if not items:
                break

            for it in items:
                refnr = it.get("refnr")
                titel = first_nonempty(
                    it.get("titel"),
                    it.get("stellenbezeichnung"),
                    it.get("beruf"),
                )

                ag = it.get("arbeitgeber") or {}
                arbeitgeber = first_nonempty(
                    ag.get("name") if isinstance(ag, dict) else ag
                )

                ao = it.get("arbeitsort") or {}
                ort = first_nonempty(ao.get("ort") if isinstance(ao, dict) else None)

                # ключ для дедупликации — refnr либо комбинация полей
                key = first_nonempty(refnr, f"{titel}|{arbeitgeber}|{ort}")
                if key not in jobs_raw:
                    jobs_raw[key] = it


            if len(items) < base_params["size"]:
                break

            time.sleep(0.2)

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
            it.get("titel"),
            it.get("stellenbezeichnung"),
            it.get("beruf"),
        )
        published = pick_date(it)

        link = f"https://www.arbeitsagentur.de/jobsuche/jobdetail/{refnr}" if refnr else ""

        results.append({
            "titel": titel or "",
            "arbeitgeber": arbeitgeber or "",
            "arbeitsort": arbeitsort or "",
            "plz": plz or "",
            "published": published or "",
            "refnr": refnr or "",
            "link": link,
            "email": contact.get("email", ""),
        })

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
    .header-logo {
        height: 65px;
        object-fit: contain;
    }
    .table thead th {
        background: #2f7d32 !important;
        color: white !important;
        font-weight: 600;
        font-size: 15px;
    }
    .table tbody tr:nth-child(odd) { background: #eaf7ea; }
    .table tbody tr:nth-child(even) { background: #ffffff; }
    .table td:nth-child(5) {
        white-space: nowrap;
    }
  </style>
</head>

<body>
<div class="container py-4">

  <div class="d-flex justify-content-between align-items-center mb-4">
      <h1 class="m-0">Stellensuche (arbeitsagentur.de)</h1>
      <img src="https://ejzkepf.stripocdn.email/content/guids/CABINET_7fd94653830e80b0e60fc4cb1431872fa2b8115f0f3b7b9f53dd6a446bdc94bb/images/image_0BS.png"
           class="header-logo"
           alt="Logo">
  </div>

  <form class="row g-3 mb-4" method="get" action="/">

    <div class="col-md-4">
      <label class="form-label">Schlüsselwörter</label>
      <input type="text" name="kw" class="form-control"
             placeholder="z.B. Schweißer, Gabelstaplerfahrer"
             value="{{ kw }}">
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

    <div class="col-md-3 mt-2">
      <label class="form-label">Keine Personaldienstleister</label>
      <select class="form-select" name="exclude_pav">
          <option value="1" {% if exclude_pav %}selected{% endif %}>Ja</option>
          <option value="0" {% if not exclude_pav %}selected{% endif %}>Nein</option>
      </select>
    </div>

    <div class="col-12">
      <button type="submit" class="btn btn-success px-4">Suchen</button>

      {% if results %}
      <a href="/export.csv?fname={{ fname|urlencode }}" class="btn btn-outline-secondary ms-2">
        CSV herunterladen
      </a>
      {% endif %}
    </div>
  </form>

  {% if results is not none %}
    <h5 class="mb-3">Gefundene Stellen: {{ results|length }}</h5>

    <div class="table-responsive">
      <table class="table table-bordered table-striped">
        <thead>
          <tr>
            <th>STELLENTITEL</th>
            <th>ARBEITGEBER</th>
            <th>ORT</th>
            <th>PLZ</th>
            <th>PUBLISHED</th>
            <th>E-MAIL</th>
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
            <td><a href="{{ r.link }}" target="_blank">Öffnen</a></td>
          </tr>
          {% endfor %}
        </tbody>
      </table>
    </div>
  {% endif %}

</div>
</body>
</html>
"""


@app.route("/", methods=["GET"])
def index():
    global LAST_RESULTS

    if not request.args:
        # стартовая страница без результатов
        return render_template_string(
            HTML_TEMPLATE,
            results=None,
            kw="Schweißer",
            wo="33689 Bielefeld",
            umkreis=25,
            exclude_pav=True,
            fname="jobs_ba.csv",
        )

    kw = request.args.get("kw", "Schweißer")
    wo = request.args.get("wo", "33689 Bielefeld")
    umkreis = int(request.args.get("umkreis", "25") or 25)
    exclude_pav = request.args.get("exclude_pav", "1") == "1"
    fname = request.args.get("fname", "jobs_ba.csv")

    jobs_raw = fetch_jobs(kw, wo, umkreis, exclude_pav)
    LAST_RESULTS = enrich_jobs(jobs_raw)

    return render_template_string(
        HTML_TEMPLATE,
        results=LAST_RESULTS,
        kw=kw,
        wo=wo,
        umkreis=umkreis,
        exclude_pav=exclude_pav,
        fname=fname,
    )


@app.route("/export.csv")
def export_csv():
    global LAST_RESULTS
    if not LAST_RESULTS:
        return "Keine Daten zum Exportieren", 400

    fname = request.args.get("fname", "jobs_ba.csv")
    if "." not in fname:
        fname += ".csv"

    output = io.StringIO()
    writer = csv.DictWriter(
        output,
        fieldnames=["titel", "arbeitgeber", "arbeitsort", "plz", "published", "refnr", "link", "email"]
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
