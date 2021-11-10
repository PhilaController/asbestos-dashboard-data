import geopandas as gpd
import numpy as np
import pandas as pd

from .. import DATA_DIR


def load_schools_database():
    """Load the schools database."""

    # Columns to keep
    columns = [
        "ULCS Code",
        "Publication Name",
        "School Level",
        "GPS Location",
        "Street Address",
        "Website",
        "School Year",
        "Abbreviated Name",
        "Year Opened",
        "Year Closed",
    ]

    # Loop over the data files
    data = []
    files = (DATA_DIR / "raw" / "schools").glob("2*")
    for f in sorted(files, reverse=True):

        if f.suffix == ".csv":
            data.append(pd.read_csv(f))
        else:
            data.append(pd.read_excel(f, sheet_name=1))

        data[-1]["School Year"] = f.name.split()[0]

    # Add the historical data file too
    path = DATA_DIR / "raw/schools/Longitudinal School List (20171128).xlsx"
    data.append(
        pd.read_excel(path, sheet_name=1).rename(
            columns={"Current Year Address": "Street Address"}
        )
    )

    data = (
        pd.concat(data)[columns]
        .drop_duplicates(keep="first", subset=["ULCS Code", "Publication Name"])
        .rename(
            columns={
                "ULCS Code": "ulcs_code",
                "Publication Name": "school_name",
                "School Level": "school_level",
                "GPS Location": "gps_location",
                "Street Address": "school_address",
                "Website": "school_website",
                "School Year": "school_year",
                "Abbreviated Name": "school_abbreviation",
                "Year Opened": "year_opened",
                "Year Closed": "year_closed",
            }
        )
        .assign(
            school_level=lambda df: df.school_level.str.lower(),
            year_closed=lambda df: df.year_closed.replace({"open": np.nan}),
            lat=lambda df: df["gps_location"]
            .str.split(",")
            .apply(lambda x: float(x[0]) if not np.isscalar(x) else x),
            lng=lambda df: df["gps_location"]
            .str.split(",")
            .apply(lambda x: float(x[1]) if not np.isscalar(x) else x),
        )
    )

    # Drop the EOP schools
    sel = data["school_name"].apply(
        lambda s: any([word == "EOP" for word in s.split()])
    )
    data = data.loc[~sel]

    return (
        gpd.GeoDataFrame(
            data,
            geometry=gpd.points_from_xy(data["lng"], data["lat"]),
            crs="EPSG:4326",
        )
        .assign(school_abbreviation=lambda df: df.school_abbreviation.str.lower())
        .dropna(subset=["school_name", "school_address"])
        .drop(labels=["lat", "lng", "gps_location", "school_year"], axis=1)
        .reset_index(drop=True)
    )
