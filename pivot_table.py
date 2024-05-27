import psycopg2
from decouple import config
from tabulate import tabulate
from datetime import datetime

def run_pivot_table():
    conn_params = {
        'dbname': config("DB_NAME"),
        'user': config("DB_USER"),
        'password': config("DB_PASSWORD"),
        'host': config("DB_HOST"),
        'port': config("DB_PORT")
    }

    conn = psycopg2.connect(**conn_params)
    cursor = conn.cursor()

    countries = config("SELECTED_COUNTRIES")
    countries_filter = f"('{countries.replace(';', "','")}')"

    if config("SELECTED_YEARS") == 'last_five':
        current_year = datetime.now().year
        year_list = [str(current_year - i) for i in range(1,6)]
        selected_years = "(" + ", ".join(year_list) + ")"
    else:
        selected_years = config("SELECTED_YEARS")
        print(f"-- Creating Pivot table for years: {selected_years} --")
        year_list = sorted(list(eval(selected_years)), reverse=True)

    query = f"""
            SELECT
                c.id,
                c.name,
                c.iso3code,
                CASE 
                    WHEN SUM(CASE WHEN g.year = {year_list[4]} THEN g.value ELSE 0 END) = 0 THEN 'No data' 
                    ELSE ROUND(SUM(CASE WHEN g.year = {year_list[4]} THEN g.value / 1_000_000_000 ELSE 0 END), 2)::TEXT 
                END AS "{year_list[4]}",
                CASE 
                    WHEN SUM(CASE WHEN g.year = {year_list[3]} THEN g.value ELSE 0 END) = 0 THEN 'No data' 
                    ELSE ROUND(SUM(CASE WHEN g.year = {year_list[3]} THEN g.value / 1_000_000_000 ELSE 0 END), 2)::TEXT 
                END AS "{year_list[3]}",
                CASE 
                    WHEN SUM(CASE WHEN g.year = {year_list[2]} THEN g.value ELSE 0 END) = 0 THEN 'No data' 
                    ELSE ROUND(SUM(CASE WHEN g.year = {year_list[2]} THEN g.value / 1_000_000_000 ELSE 0 END), 2)::TEXT 
                END AS "{year_list[2]}",
                CASE 
                    WHEN SUM(CASE WHEN g.year = {year_list[1]} THEN g.value ELSE 0 END) = 0 THEN 'No data' 
                    ELSE ROUND(SUM(CASE WHEN g.year = {year_list[1]} THEN g.value / 1_000_000_000 ELSE 0 END), 2)::TEXT 
                END AS "{year_list[1]}",
                CASE 
                    WHEN SUM(CASE WHEN g.year = {year_list[0]} THEN g.value ELSE 0 END) = 0 THEN 'No data' 
                    ELSE ROUND(SUM(CASE WHEN g.year = {year_list[0]} THEN g.value / 1_000_000_000 ELSE 0 END), 2)::TEXT 
                END AS "{year_list[0]}"
            FROM
                gdp g
            JOIN
                country c ON g.country_id = c.id
            WHERE
                g.year IN {selected_years}
            GROUP BY
                c.id,
                c.name,
                c.iso3code
            ORDER BY
                c.id;
            """

    query2 = f"""
            SELECT
                c.id,
                c.name,
                c.iso3code,
                CASE WHEN ROUND(SUM(CASE WHEN g.year = {year_list[4]} THEN g.value / 1_000_000_000 ELSE NULL END), 2) IS NULL THEN 'No data' ELSE ROUND(SUM(CASE WHEN g.year = {year_list[4]} THEN g.value / 1000_000_000 ELSE NULL END), 2)::TEXT END AS "{year_list[4]}",
                CASE WHEN ROUND(SUM(CASE WHEN g.year = {year_list[3]} THEN g.value / 1_000_000_000 ELSE NULL END), 2) IS NULL THEN 'No data' ELSE ROUND(SUM(CASE WHEN g.year = {year_list[3]} THEN g.value / 1_000_000_000 ELSE NULL END), 2)::TEXT END AS "{year_list[3]}",
                CASE WHEN ROUND(SUM(CASE WHEN g.year = {year_list[2]} THEN g.value / 1_000_000_000 ELSE NULL END), 2) IS NULL THEN 'No data' ELSE ROUND(SUM(CASE WHEN g.year = {year_list[2]} THEN g.value / 1_000_000_000 ELSE NULL END), 2)::TEXT END AS "{year_list[2]}",
                CASE WHEN ROUND(SUM(CASE WHEN g.year = {year_list[1]} THEN g.value / 1_000_000_000 ELSE NULL END), 2) IS NULL THEN 'No data' ELSE ROUND(SUM(CASE WHEN g.year = {year_list[1]} THEN g.value / 1_000_000_000 ELSE NULL END), 2)::TEXT END AS "{year_list[1]}",
                CASE WHEN ROUND(SUM(CASE WHEN g.year = {year_list[0]} THEN g.value / 1_000_000_000 ELSE NULL END), 2) IS NULL THEN 'No data' ELSE ROUND(SUM(CASE WHEN g.year = {year_list[0]} THEN g.value / 1_000_000_000 ELSE NULL END), 2)::TEXT END AS "{year_list[0]}"
            FROM
                country c
            LEFT JOIN
                gdp g ON g.country_id = c.id AND g.year IN {selected_years}
            WHERE
                c.iso3code in {countries_filter}
            GROUP BY
                c.id,
                c.name,
                c.iso3code
            ORDER BY
                c.id;
            """

    cursor.execute(query)

    rows = cursor.fetchall()
    print(tabulate(rows, headers=[desc[0] for desc in cursor.description]))

    cursor.close()
    conn.close()


if __name__ == '__main__':
    run_pivot_table()