import logging

YEAR = 2020
PERIOD_UP_TO_2020 = "<=2020"
PERIOD_FROM_2021 = ">=2021"

def year_of(value):
    if hasattr(value, "year"):
        return value.year
    return int(str(value)[:4])

def split_rows(rows):
    q1,q2, norm = [],[],[]

    for row in rows:
        try:
            old = year_of(row["date_modified"]) <= YEAR
            norm_row = {
                "url": row["url"],
                "language": row["language"],
                "abstract": row["abstract"],
                "period": PERIOD_UP_TO_2020 if old else PERIOD_FROM_2021
            }

            if old:
                q1_row = {"url": row["url"], "language": row["language"],
                          "date_modified": row["date_modified"]}
                q2_row = {"name": row["name"], "url": row["url"], "sections": row["sections"], "image_url": row["image_url"]}

        except (KeyError, ValueError, TypeError):
            logging.warning("fila inválida, se descarta: %s", row.get("url") if isinstance(row, dict) else row)
            continue
        norm.append(norm_row)

        if old:
            q1.append(q1_row)
            q2.append(q2_row)

    return q1 ,q2 ,norm