from pipeline import db
import datetime
import html
import os

def write_report(results):
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    rows = "".join(
        f"<tr><td>{html.escape(r['check'])}</td>"
        f"<td>{'✅' if r['passed'] else '❌'}</td>"
        f"<td>{html.escape(r['details'])}</td></tr>"
        for r in results
    )
    page = f"""<!doctype html><html><head><meta charset="utf-8">
<meta http-equiv="refresh" content="30"><title>Data Quality</title></head>
<body><h1>Data Quality: labeled_transactions</h1><p>Последняя проверка: {now}</p>
<table border="1"><tr><th>Проверка</th><th>Статус</th><th>Детали</th></tr>{rows}</table>
</body></html>"""
    tmp = db.REPORT_PATH.with_suffix(".tmp")
    tmp.write_text(page, encoding="utf-8")
    os.replace(tmp, db.REPORT_PATH)