import click
import geopandas as gpd
import pandas as pd
from loguru import logger

from . import DATA_DIR
from .aws import upload_to_s3
from .data import load_asbestos_data
from .data.asbestos import geocode, transform
from .scrape import DatabaseScraper, update_permit_urls

# School columns
SCHOOL_COLUMNS = [
    "school_name",
    "school_level",
    "year_closed",
    "school_website",
    "school_address",
    "year_opened",
]


@click.group()
def main():
    """Run the asbestos dashboard data analysis."""
    pass


@main.command()
def update(ndays=30):
    """Run the daily update."""

    # Load the old processed data
    old_data = gpd.read_file(DATA_DIR / "processed" / "asbestos-data.geojson")

    # Initialize the database scraper
    logger.info(f"Downloading raw data from past {ndays} days")
    scraper = DatabaseScraper(ndays=ndays)
    new_data = scraper.run()

    # Update any old data
    old_sel = new_data["permit_number"].isin(old_data["permit_number"])
    new_sel = ~old_sel

    # Update info for overlap
    overlap_columns = new_data.columns.intersection(old_data.columns)
    for permit_number in new_data.loc[old_sel, "permit_number"]:
        old_data.loc[
            old_data["permit_number"] == permit_number, overlap_columns
        ] = new_data.loc[
            new_data["permit_number"] == permit_number, overlap_columns
        ].values

    # Get the new permits
    if new_sel.sum():

        logger.info(f"New data includes {new_sel.sum()} entries")
        new_data = new_data.loc[new_sel]
        new_data.to_excel(DATA_DIR / "interim" / "new_data.xlsx", index=False)

        logger.info("Cleaning new data...")
        new_data = new_data.pipe(geocode).pipe(transform)
        logger.info("  ...done")

        # Combine
        data = pd.concat([old_data, new_data])

    else:
        logger.info("No new data found")
        data = old_data.copy()

    # Make sure there are no duplicates
    data = data.drop_duplicates()

    # Get the permit numbers too
    out = update_permit_urls(data)

    assert len(out) == len(data)
    assert out.duplicated().sum() == 0

    # Run the etl
    _run_etl(out)


def _run_etl(df):

    # Save asbestos to AWS
    asbestos = pd.DataFrame(df.drop(labels=["geometry"] + SCHOOL_COLUMNS[3:], axis=1))
    upload_to_s3(asbestos.to_json(orient="records"), "asbestos-data.json")

    # And save locally
    df.to_file(DATA_DIR / "processed" / "asbestos-data.geojson", driver="GeoJSON")

    # Create the schools database
    schools = df[SCHOOL_COLUMNS + ["geometry"]].drop_duplicates()

    # Remove duplicates that have only different geometries
    duplicated_schools = schools.drop(columns=["geometry"]).duplicated()
    schools = schools.loc[~duplicated_schools]

    # Save to AWS
    upload_to_s3(schools.to_json(), "schools.json")

    # And save locally
    schools.to_file(DATA_DIR / "processed" / "schools.geojson", driver="GeoJSON")


@main.command()
def etl():
    """Run the ETL pipeline."""

    # Load the asbestos data
    df = load_asbestos_data()
    assert df.duplicated().sum() == 0
    _run_etl(df)
