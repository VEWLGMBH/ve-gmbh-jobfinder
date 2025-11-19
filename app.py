# -*- coding: utf-8 -*-
import csv
import io
import time
import re
import requests
import urllib3
from flask import Flask, request, render_template_string, send_file

# отключаем предупреждения об отключенной SSL-проверке
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

app = Flask(__name__)

# -------------------------------------------------------------------
# Конфиг API Agentur für Arbeit
# -------------------------------------------------------------------
API_BASE = "https://rest.arbeitsagentur.de/jobboerse/jobsuche-service/pc/v4"
SEARCH_URL = f"{API_BASE}/jobs"
JOBDETAIL_URL = "https://www.arbeitsagentur.de/jobsuche/jobdetail/{refnr}"

HEADERS = {
    "X-API-Key": "jobboerse-jobsuche",
    "User-Agent": "JobSearchBot/1.0",
}

# Ограничения, чтобы не перегружать API и не вылетать по тайм-ауту Render
MAX_PAGES = 30          # максимум страниц по одному ключевому слову
MAX_DETAILS = 40        # максимум вакансий, для которых тянем детали (email)
DETAIL_SLEEP = 0.05     # пауза между запросами деталей, сек

EMAIL_RX = re.compile(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", re.I)
DATE_RX = re.compile(r"\d{4}-\d{2}-\d{2}")

LAST_RESULTS = []  # хранит результаты последнего поиска для экспорта CSV


# -------------------------------------------------------------------
# Вспомогательные функции
# -------------------------------------------------------------------
def first_nonempty(*vals):
    """Вернуть первое непустое значение из списка."""
    for v in vals:
        if isinstance(v, str) and v.strip():
            return v.strip()
        if v:
            return v
    return None


def pick_date(item: dict):
    """Вытащить дату вида YYYY-MM-DD из любого текстового поля вакансии."""
    for v in item.values():
        if isinstance(v, str):
            m = DATE_RX.search(v)
            if m:
                return m.group(0)
    return None


def parse_jobdetail(refnr: str):
    """
    Зайти на HTML-страницу вакансии и вытащить email (если есть).
    Обернуто в try/except, чтобы любые ошибки сети не валили всё приложение.
    SSL-проверку отключаем (verify=False), чтобы избежать проблем с CA на Render.
    """
    if not refnr:
        return {"email": ""}

    url = JOBDETAIL_URL.format(refnr=refnr)
    try:
        r = requests.get(
            url,
            headers={"User-Agent": HEADERS["User-Agent"]},
            timeout=10,
            verify=False,  # <-- главное отличие для Render
        )
        if r.status_code != 200:
            return {"email": ""}

        html = r.text
        emails = set(EMAIL_RX.findall(html))
        email = sorted(emails)[0] if emails else ""
        return {"email": email}

    except Exception as e:
        # Логируем в stdout для Render, но не падаем
        print("Error in parse_jobdetail:", e)
        return {"email": ""}


def fetch_jobs(keyword, wo, umkreis, exclude_pav):
    """
    Поиск через API Agentur für Arbeit.
    Поддержка нескольких ключевых слов через запятую:
    'Schweißer, Gabelstaplerfahrer' -> два запроса, результаты объединяются.
    Все ошибки сети перехватываются, чтобы не падать.
    """
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

    # Разбиваем строку "kw" по запятым
    if isinstance(keyword, str):
        keywords = [k.strip() for k in keyword.split(",") if k.strip()]
    else:
        keywords = keyword

    for kw in keywords:
        print(f"Fetching jobs for keyword: {kw!r}")
        for page in range(1, MAX_PAGES + 1):
            params = base_params.copy()
            params["was"] = kw
            params["page"] = page

            try:
                resp = requests.get(
                    SEARCH_URL,
                    headers=HEADERS,
                    params=params,
                    timeout=25,
                )
            except Exception as e:
                print("Fetch error:", e)
                break

            if resp.status_code == 400:
                # Обычно означает, что вылезли за предел возможных страниц
                print("Got 400 from API, stop paging for this keyword.")
                break

            try:
                data = resp.json()
            except Exception as e:
                print("JSON parse error:", e)
                break

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

            print(f"Fetched {len(items)} items on page {page} for {kw!r}")

            # Если вернулось меньше, чем size — дальше страниц нет
            if len(items) < base_params["size"]:
                break

            time.sleep(0.2)  # небольшая пауза, чтобы не долбить API

    print("Total raw jobs collected:", len(jobs_raw))
    return jobs_raw


def enrich_jobs(jobs_raw):
    """
    Вызывает parse_jobdetail для каждой вакансии и собирает финальный список.
    Ограничиваемся MAX_DETAILS штук, чтобы не уходить в вечный запрос.
    """
    results = []
    count = 0

    for key, it in jobs_raw.items():
        if count >= MAX_DETAILS:
            print(f"Reached MAX_DETAILS={MAX_DETAILS}, stop fetching details.")
            break

        refnr = it.get("refnr")
        contact = parse_jobdetail(refnr)
        count += 1
        time.sleep(DETAIL_SLEEP)

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

        link = (
            f"https://www.arbeitsagentur.de/jobsuche/jobdetail/{refnr}"
            if refnr else ""
        )

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

    print("Total enriched jobs:", len(results))
    return results


# -------------------------------------------------------------------
# HTML-шаблон: зелёный дизайн + логотип + таблица результатов
# -------------------------------------------------------------------
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


# -------------------------------------------------------------------
# Flask-маршруты
# -------------------------------------------------------------------
def safe_int(val, default):
    try:
        return int(val)
    except (TypeError, ValueError):
        return default


@app.route("/", methods=["GET"])
def index():
    global LAST_RESULTS

    # Первый заход — просто форма без результатов
    if not request.args:
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
    umkreis = safe_int(request.args.get("umkreis", "25"), 25)
    exclude_pav = request.args.get("exclude_pav", "1") == "1"
    fname = request.args.get("fname", "jobs_ba.csv")

    try:
        jobs_raw = fetch_jobs(kw, wo, umkreis, exclude_pav)
        LAST_RESULTS = enrich_jobs(jobs_raw)
    except Exception as e:
        print("Fatal error in search:", e)
        LAST_RESULTS = []

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
