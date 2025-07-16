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


@app.cell
def _(ambient_air_quality, con, mo):
    _df = mo.sql(
        f"""
        describe sample_data.who.ambient_air_quality
        """,
        engine=con
    )
    return


@app.cell
def _(ambient_air_quality, con, mo, sample_data):
    _df = mo.sql(
        f"""
        SELECT 
            year,
            country_name,
            AVG(pm10_concentration),
            AVG(pm25_concentration)
        FROM sample_data.who.ambient_air_quality
        WHERE year >= (YEAR(current_date) - 10) and
            country_name = 'Latvia'
        GROUP BY year, country_name
        ORDER BY year, country_name

        """,
        engine=con
    )
    return


@app.cell
def _(con, mo):
    _df = mo.sql(
        f"""
        SELECT * FROM read_parquet('hf://datasets/datonic/threatened_animal_species/data/threatened_animal_species.parquet');
        """,
        engine=con
    )
    return


@app.cell
def _(con, mo):
    _df = mo.sql(
        f"""
        USE my_db;
        """,
        engine=con
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    **Exercise 4.03**

    Create a new table called `animals` in your MotherDuck database `md:my_db` based on the `datonic/threatened_animal_species` dataset.
    """
    )
    return


@app.cell
def _(con, mo):
    _df = mo.sql(
        f"""
        CREATE TABLE IF NOT EXISTS animals AS (SELECT * FROM read_parquet('hf://datasets/datonic/threatened_animal_species/data/threatened_animal_species.parquet'))
        """,
        engine=con
    )
    return (animals,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    **Exercise 4.04**

    DuckDB releases are each named after a duck! Let's load [this data](https://duckdb.org/data/duckdb-releases.csv) into a new table called `duckdb_ducks`. You can use `read_csv` to load the data directly from the HTTP URL: `https://duckdb.org/data/duckdb-releases.csv`.
    """
    )
    return


@app.cell
def _(con, mo):
    _df = mo.sql(
        f"""
        CREATE TABLE IF NOT EXISTS duckdb_ducks AS (
            SELECT *
            FROM read_csv('https://duckdb.org/data/duckdb-releases.csv')
        )
        """,
        engine=con
    )
    return (duckdb_ducks,)


@app.cell
def _(animals, con, mo):
    _df = mo.sql(
        f"""
        DESCRIBE animals
        """,
        engine=con
    )
    return


@app.cell
def _(con, duckdb_ducks, mo):
    _df = mo.sql(
        f"""
        DESCRIBE duckdb_ducks;
        """,
        engine=con
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    **Exercise 4.05**

    Create a new table called `duckdb_species` that joins the `duckdb_ducks` and `animals` tables on the scientific name.
    """
    )
    return


@app.cell
def _(animals, con, duckdb_ducks, mo):
    _df = mo.sql(
        f"""
        CREATE TABLE IF NOT EXISTS duckdb_species AS
        (SELECT duck.*,
            animals.category
        FROM duckdb_ducks AS duck
        INNER JOIN animals 
        ON duck.duck_species_primary = animals.scientific_name)
        """,
        engine=con
    )
    return


@app.cell
def _(con, mo):
    _df = mo.sql(
        f"""
        ATTACH 'md:_share/mosaic_examples/b01cfda8-239e-4148-a228-054b94cdc3b4';
        """,
        engine=con
    )
    return


@app.cell
def _(con, mo):
    _df = mo.sql(
        f"""
        USE mosaic_examples;
        SHOW TABLES;
        """,
        engine=con
    )
    return


@app.cell
def _(con, mo, seattle_weather):
    _df = mo.sql(
        f"""
        SELECT * FROM seattle_weather
        """,
        engine=con
    )
    return


@app.cell
def _(con, mo):
    _df = mo.sql(
        f"""
        USE my_db;
        DETACH mosaic_examples;
        """,
        engine=con
    )
    return


@app.cell
def _(con, mo):
    _df = mo.sql(
        f"""
        DROP SHARE duck_share;
        """,
        engine=con
    )
    return


if __name__ == "__main__":
    app.run()
