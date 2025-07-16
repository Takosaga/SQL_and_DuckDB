import marimo

__generated_with = "0.14.10"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _():
    import duckdb
    con = duckdb.connect("md:sample_data")
    return (con,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    **Exercise 4.01**

    Create a connection to MotherDuck and show all tables in your `sample_data` database. You can use the `SHOW TABLES` command that is documented [here](https://duckdb.org/docs/guides/meta/list_tables.html).
    """
    )
    return


@app.cell
def _(con):
    con.sql("SHOW ALL TABLES")
    return


@app.cell
def _(con, mo, sample_data, service_requests):
    _df = mo.sql(
        f"""
        SELECT
          created_date, agency_name, complaint_type,
          descriptor, incident_address, resolution_description
        FROM
          sample_data.nyc.service_requests
        WHERE
          created_date >= '2022-03-27' AND
          created_date <= '2022-03-31';

        """,
        engine=con
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    **Exercise 4.02**

    Run `DESCRIBE` on the `sample_data.who.ambient_air_quality` table to inspect the column names. Write a query that gets the average concentrations of PM1.0 and PM2.5 particles for the `'United States of America'`, for the last 10 years, grouped and ordered by year.
    """
    )
    return


if __name__ == "__main__":
    app.run()
