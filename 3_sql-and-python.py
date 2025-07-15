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

    duckdb.execute("SELECT 42 as hello_world").fetchall()
    return (duckdb,)


@app.cell
def _(duckdb):
    duckdb.sql("SELECT 42 as hello_world").fetchall()
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        SELECT 42
        """
    )
    return


@app.cell
def _(duckdb):
    duckdb.sql("SELECT 42 as hello_world").df()
    return


@app.cell
def _(duckdb):
    import pandas as pd
    ducks_pandas = pd.read_csv('ducks.csv')

    duckdb.sql("SELECT * FROM ducks_pandas").df()
    return (ducks_pandas,)


@app.cell
def _():
    import polars as pl
    import pyarrow as pa
    import pyarrow.csv as pa_csv
    return pa_csv, pl


@app.cell
def _(duckdb, pl):
    ducks_polars = pl.read_csv('ducks.csv')
    duckdb.sql("""SELECT * FROM ducks_polars""").pl()
    return (ducks_polars,)


@app.cell
def _(duckdb, pa_csv):
    ducks_arrow = pa_csv.read_csv('ducks.csv')
    duckdb.sql("""SELECT * FROM ducks_arrow""").arrow()
    return (ducks_arrow,)


@app.cell
def _(pa_csv):
    birds_arrow = pa_csv.read_csv('birds.csv')
    return (birds_arrow,)


@app.cell
def _(birds_arrow, duckdb):
    duckdb.sql("""
    DESCRIBE birds_arrow""").arrow()
    return


@app.cell
def _(duckdb, ducks_arrow):
    duckdb.sql("""
    DESCRIBE ducks_arrow""").arrow()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    **Exercise 3.01**

    Read in the birds.csv file using Apache Arrow, then use the DuckDB Python library to execute a SQL statement on that Apache Arrow table to find the maximum `wing_length` in the dataset. Output that result as an Apache Arrow table.
    """
    )
    return


@app.cell
def _(birds_arrow, duckdb):
    duckdb.sql("""
    SELECT 
        MAX(birds_arrow.wing_length)
    FROM birds_arrow
    """).arrow()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    **Exercise 3.02**


    Use the DuckDB Python client to return these results as a Polars dataframe.
    ```sql
    SELECT
        Species_Common_Name,
        AVG(Beak_Width) AS Avg_Beak_Width,
        AVG(Beak_Depth) AS Avg_Beak_Depth,
        AVG(Beak_Length_Culmen) AS Avg_Beak_Length_Culmen
    FROM 'birds.csv'
    GROUP BY Species_Common_Name
    ```
    """
    )
    return


@app.cell
def _(duckdb, pl):
    birds_polars = pl.read_csv('birds.csv')
    duckdb.sql("""
    SELECT
        Species_Common_Name,
        AVG(Beak_Width) AS Avg_Beak_Width,
        AVG(Beak_Depth) AS Avg_Beak_Depth,
        AVG(Beak_Length_Culmen) AS Avg_Beak_Length_Culmen
    FROM birds_polars
    GROUP BY Species_Common_Name
    """).pl()
    return (birds_polars,)


@app.cell
def _(duckdb):
    duck_legends = (duckdb
      .read_csv("ducks.csv")
      .filter("extinct = 0")
      .aggregate("author, count(name) as count_name, min(year) as min_year", "author")
      .order("count_name desc")
    )
    duck_legends
    return (duck_legends,)


@app.cell
def _(duck_legends):
    print(duck_legends.sql_query())
    return


@app.cell
def _(duck_legends, duckdb):
    duckdb.sql("select * from duck_legends limit 5")
    return


@app.cell
def _(duck_legends, duckdb):
    duckdb.sql("select * from duck_legends limit 5").limit(1)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    **Exercise 3.03**

    As an exercise, use SQL to initially pull the CSV file, but then chain together the remaining Relational operators from the `duck_legends` example above to return the same result.
    """
    )
    return


@app.cell
def _(duckdb, ducks_arrow):
    duckdb.sql("""
    SELECT 
        author,
        count(name) as count_name,
        min(year) as min_year
    FROM ducks_arrow
    WHERE extinct = 0
    GROUP BY author
    ORDER BY count_name DESC
    """).arrow()
    return


@app.cell
def _():
    import ibis
    from ibis import _
    ibis.options.interactive = True

    con = ibis.duckdb.connect(database='whats_quackalackin.duckdb')
    return con, ibis


@app.cell
def _(ibis):
    ducks_ibis = ibis.read_csv('ducks.csv')
    ducks_ibis
    return (ducks_ibis,)


@app.cell
def _(con, ducks_ibis):
    persistent_ducks = con.create_table('persistent_ducks', obj=ducks_ibis.to_pyarrow(), overwrite=True)
    persistent_ducks
    return (persistent_ducks,)


@app.cell
def _(ibis, persistent_ducks):
    duck_legend = (persistent_ducks
      .filter(persistent_ducks.extinct == 0)
      .select("name", "author", "year")
      .group_by("author")
      .aggregate([persistent_ducks.name.count(), persistent_ducks.year.min()])
      .order_by([ibis.desc("Count(name)")])
    )
    duck_legend
    return (duck_legend,)


@app.cell
def _(duck_legend, ibis):
    ibis.to_sql(duck_legend)
    return


@app.cell
def _(ibis, persistent_ducks):
    new_duck_legends = (persistent_ducks
      .sql("""SELECT name, author, year FROM persistent_ducks WHERE extinct = 0""")
      .group_by("author")
      .aggregate([_.name.count(), _.year.min()]) # Use _ instead of persistent_ducks
      .order_by([ibis.desc("Count(name)")])
    )
    new_duck_legends
    return


if __name__ == "__main__":
    app.run()
