import os
from pathlib import Path

import ais2gpd
import geopandas as gpd
import numpy as np
import pandas as pd
import schuylkill as skool
from dotenv import find_dotenv, load_dotenv
from loguru import logger

from .. import DATA_DIR
from .schools import load_schools_database


def trim_to_school_district(data):
    """
    Trim the input data to rows associated with the School District.
    This will check the owner name and owner address.
    """

    def test_owner(x):
        if isinstance(x, str):
            x = x.lower()
            return ("school" in x and "phila" in x and "dis" in x) or x == "sdp"
        else:
            return False

    def test_address(x):
        if isinstance(x, str):
            x = x.lower()
            return "440" in x and "broad" in x
        else:
            return False

    sel1 = data["facility_owner"].apply(test_owner)
    sel2 = data["facility_owner_address"].apply(test_address)
    return data.loc[sel1 | sel2]


def extract_asbestos_data(filename=None):
    """
    Extract the data from the raw .xlsx file.

    This will:
        - Rename the input columns
        - Trim to Jan 1 2016 onwards
        - Trim to School District only
    """
    # Use the last modified file
    if filename is None:
        raw_data_files = sorted(
            Path(DATA_DIR / "raw").glob("Citizen*.xlsx"),
            key=lambda f: os.path.getmtime(f),
        )
        filename = raw_data_files[-1]

    # Load the raw data
    data = pd.read_excel(filename, sheet_name=0)

    # Parse dates
    date_columns = [
        "Application Date",
        "Approval Date",
        "Issue Date",
        "Expiration Date",
        "Work Start",
        "Complete Date",
    ]
    for col in date_columns:
        data[col] = pd.to_datetime(data[col]).dt.strftime("%m-%d-%Y")

    # Rename the columns
    COLUMNS = {
        "Permit #": "permit_number",
        "Work Description": "work_description",
        "Subtype": "subtype",
        "Applicant": "applicant",
        "Status": "status",
        "Site Address": "facility_address",
        "Application Date": "application_date",
        "Approval Date": "approval_date",
        "Issue Date": "issue_date",
        "Expiration Date": "expiration_date",
        "Work Start": "work_start",
        "Complete Date": "complete_date",
        "Notification Type": "notification_type",
        "Asbestos Inspector": "asbestos_inspector",
        "Project Type": "project_type",
        "Type of Operation": "operation_type",
        "Facility Name": "facility_name",
        "Facility Owner": "facility_owner",
        "Facility Owner Address": "facility_owner_address",
        "Abatement Contractor": "abatement_contractor",
        "Demo Contractor": "demo_contractor",
        "Asbestos Investigator": "asbestos_investigator",
        "Asbestos Present": "asbestos_present",
        "Linear Ft of Friable Material": "linear_ft_friable",
        "Square Ft of Friable Material": "square_ft_friable",
        "Cubic Ft of Friable Material": "cubic_ft_friable",
        "Linear Ft of Non-Fraibale Material": "linear_ft_nonfriable",
        "Square Ft of Non-Friable Material": "square_ft_nonfriable",
        "Cubic Ft of Non-Friable Material": "cubic_ft_nonfriable",
        "Asbestos Material": "friable_acm",
    }

    # Rename and trim
    data = data.rename(columns=COLUMNS)[list(COLUMNS.values())]

    # Trim to school district
    school_district = trim_to_school_district(data)

    # Trim to 2016 onwards
    sel = pd.to_datetime(school_district["application_date"]) >= "01-01-2016"
    school_district = school_district.loc[sel]

    # Log
    logger.info(f"Size of original database: {len(data)}")
    logger.info(f"Size of school district database: {len(school_district)}")

    return school_district.sort_values("application_date", ignore_index=True)


def geocode(df, address_column="facility_address", ignore_failure=False):
    """Geocode the input data."""

    # Load the environment variables
    load_dotenv(find_dotenv())

    # Make sure we have the API key
    api_key = os.getenv("AIS_API_KEY")
    if api_key is None:
        raise ValueError(
            "Please define the `AIS_API_KEY` variable in your .env file to use AIS to geocode"
        )

    # Merge existing geocoded episodes
    path = DATA_DIR / "interim" / "geocoded_addresses.xlsx"
    geocoded_addresses = None
    if path.exists():
        geocoded_addresses = pd.read_excel(path)

    # Manual geocodes
    path = DATA_DIR / "interim" / "manual_geocoded_addresses.xlsx"
    if path.exists():
        manual_addresses = pd.read_excel(path)
        if geocoded_addresses is not None:
            geocoded_addresses = pd.concat([geocoded_addresses, manual_addresses])
        else:
            geocoded_addresses = manual_addresses

    # Merge these into the original data
    if geocoded_addresses is not None:
        df = df.merge(
            geocoded_addresses.dropna(subset=["lat", "lng"]).drop_duplicates(
                subset=[address_column]
            ),
            on=address_column,
            how="left",
        )
    else:
        df["lat"] = np.nan
        df["lng"] = np.nan

    # Get the unique addresses
    missing_sel = df["lat"].isnull() | df["lng"].isnull()
    unique_addresses = df.loc[missing_sel][address_column].drop_duplicates()

    # No addresses to geocode so return
    if len(unique_addresses) == 0:
        # Log
        logger.info(f"No new addresses to geocode...")
        return df

    # Log
    logger.info(f"Geocoding {len(unique_addresses)} unique addresses...")

    result = ais2gpd.get(unique_addresses, api_key)
    logger.info("   ...done")

    # Trim the result
    result = result[
        [
            "street_address",
            "zip_code",
            "opa_account_num",
            "opa_address",
            "elementary_school",
            "middle_school",
            "high_school",
            "geometry",
        ]
    ].assign(lat=lambda df: df.geometry.y, lng=lambda df: df.geometry.x)

    # Combine with the original addresses
    X = result.set_index(unique_addresses.index).join(unique_addresses)

    # Log missing
    missing_sel = X.lat.isnull() | X.lng.isnull()
    missing = X.loc[missing_sel]
    logger.info(f"Found coordinates for {len(result)-len(missing)} addresses")

    # Merge back into the original data frame
    out = df.merge(X, on=address_column, how="left", suffixes=("", "_y"))

    # Merge in the coordinates
    if "lat_y" in out.columns:
        out["lat"] = out["lat"].fillna(out["lat_y"])
        out = out.drop(labels=["lat_y"], axis=1)
    if "lng_y" in out.columns:
        out["lng"] = out["lng"].fillna(out["lng_y"])
        out = out.drop(labels=["lng_y"], axis=1)

    # Split into mot missing and missing
    sel = out.lat.isnull() | out.lng.isnull()
    missing = out.loc[sel]
    not_missing = out.loc[~sel]

    # The successful geocodes
    columns = [address_column, "lat", "lng"]
    path = DATA_DIR / "interim" / "geocoded_addresses.xlsx"
    if path.exists():
        not_missing = pd.concat([not_missing[columns], pd.read_excel(path)])
    not_missing = not_missing.drop_duplicates(subset=[address_column])

    # The missing geocodes
    missing.drop_duplicates(subset=[address_column])[columns].to_excel(
        DATA_DIR / "interim" / "missing_geocoded_addresses.xlsx", index=False
    )

    if len(missing) and not ignore_failure:
        raise ValueError(
            "Missing lat/lng coordinates: see 'data/interim/missing_geocoded_addresses.xlsx'"
        )

    return out


def transform(data):
    """Transform the input data."""
    from ..scrape import update_permit_urls

    # Add project length
    data["project_length"] = (
        (
            pd.to_datetime(data["complete_date"]) - pd.to_datetime(data["work_start"])
        ).dt.total_seconds()
        / 60
        / 60
        / 24
    )

    # Convert to a geodataframe
    gdf = gpd.GeoDataFrame(
        data, geometry=gpd.points_from_xy(data["lng"], data["lat"]), crs="EPSG:4326"
    ).to_crs(epsg=2272)

    # Fill street_address with Site Address
    gdf["street_address"] = gdf["street_address"].fillna(gdf["facility_address"])

    # Return
    gdf = gdf.drop(labels=["facility_address"], axis=1).rename(
        columns={"street_address": "facility_address"}
    )

    # Match!
    schools = load_schools_database()
    data = match_datasets(gdf, schools)

    # Trim
    data = data[
        [
            "permit_number",
            "work_description",
            "applicant",
            "status",
            "application_date",
            "work_start",
            "complete_date",
            "project_type",
            "operation_type",
            "facility_name",
            "linear_ft_friable",
            "square_ft_friable",
            "cubic_ft_friable",
            "linear_ft_nonfriable",
            "square_ft_nonfriable",
            "cubic_ft_nonfriable",
            "friable_acm",
            "facility_address",
            "project_length",
            "school_name",
            "school_level",
            "school_address",
            "school_website",
            "year_opened",
            "year_closed",
            "lat",
            "lng",
        ]
    ]

    # Drop duplicate coords
    geo_coords = data[["school_name", "lat", "lng"]].drop_duplicates(
        subset=["school_name"]
    )
    data = data.drop(labels=["lat", "lng"], axis=1).merge(
        geo_coords, on="school_name", how="left"
    )

    # Append closed to school name
    closed = data["year_closed"].notnull()
    data.loc[closed, "school_name"] += " (Closed)"

    # Merge in the existing permit urls
    if "permit_url" not in data.columns:

        urls = pd.read_csv(DATA_DIR / "interim" / "permit-number-urls.csv")
        data = data.merge(urls, on="permit_number", how="left")

    # Update any missing urls
    data = update_permit_urls(data)

    # Fix school level
    data["school_level"] = data["school_level"].replace(
        {"elementarymiddle": "elementary-middle"}
    )

    # Return
    return gpd.GeoDataFrame(
        data, geometry=gpd.points_from_xy(data["lng"], data["lat"]), crs="EPSG:4326"
    ).drop(labels=["lat", "lng"], axis=1)


def load_asbestos_data(filename=None, ignore_failure=False, processed=False):
    """Load the data from the raw .xlsx file."""

    if processed:
        return gpd.read_file(DATA_DIR / "processed" / "asbestos-data.geojson")

    # Extract the data
    return (
        extract_asbestos_data(filename=filename)
        .pipe(geocode, ignore_failure=ignore_failure)
        .pipe(transform)
    )


def _clean_columns(df, cols):
    """Clean the columns."""

    # Copy the data
    df = df.copy()

    # Copy the columns
    for col in cols:
        df[f"{col}_clean"] = df[col]

    # Clean the columns
    df = skool.clean_strings(
        df,
        [f"{col}_clean" for col in cols],
    )

    # Replace
    replace = {
        "es": "elementary school",
        "ms": "middle school",
        "hs": "high school",
        "sch": "school",
        "elem": "elementary",
    }
    for col in cols:
        df[f"{col}_clean"] = df[f"{col}_clean"].replace(replace)

    return df


def _test_merge(data, schools, crosswalk, known_missing):
    """Do a test merge."""

    # Do a test merge
    test_merge = data.merge(crosswalk, on=["facility_name"], how="left")

    # Did we match everything?
    missing_matches0 = test_merge["school_name"].isnull()

    # Are they all known?
    missing_matches = ~test_merge.loc[missing_matches0, "facility_name"].isin(
        known_missing["facility_name"]
    )

    if missing_matches.sum():
        logger.info(
            f"Found {missing_matches.sum()} missing matches; proceeding to match to school list"
        )
    else:
        logger.info(
            "All facilities match existing crosswalk; no additional matching necessary"
        )

        # Merge in the full school data
        out = test_merge.loc[~missing_matches0].merge(
            schools.drop_duplicates(subset=["school_name"]),
            on=["school_name"],
            how="left",
        )
        return True, out

    # Data to match
    data2 = test_merge.loc[missing_matches0].drop(labels=["school_name"], axis=1)

    return False, data2


def match_datasets(data, schools):
    """Match the asbestos data set to the schools database."""

    # Load the known missing and crosswalk
    known_missing = pd.read_excel(DATA_DIR / "interim" / "known_missing_matches.xlsx")
    crosswalk = pd.read_excel(DATA_DIR / "interim" / "crosswalk.xlsx")

    # This will return if successful
    success, data2 = _test_merge(data, schools, crosswalk, known_missing)
    if success:
        return data2

    # Clean the datasets
    data2 = _clean_columns(data2, ["facility_name", "facility_address"])
    schools = _clean_columns(schools, ["school_name", "school_address"])

    # Get the left/right datasets
    left0 = data2.set_index("permit_number")
    right0 = schools.drop_duplicates(
        subset=["school_name_clean", "school_address_clean"]
    )

    # Columns to exact merge on
    merge_columns = [
        ("facility_name_clean", "school_name_clean"),
        ("facility_name_clean", "school_abbreviation"),
        (
            "facility_address_clean",
            "school_address_clean",
        ),
    ]

    # Exact merge
    left = left0.copy()
    right = right0.copy()
    exact = None
    for (left_on, right_on) in merge_columns:

        exact_matches = (
            left.reset_index()
            .merge(
                right.drop_duplicates(subset=[right_on]),
                left_on=left_on,
                right_on=right_on,
            )
            .set_index("permit_number")
        )

        left = left.loc[left.index.difference(exact_matches.index)]

        if exact is None:
            exact = exact_matches
        else:
            exact = pd.concat([exact, exact_matches])

    # Remove known missing
    left = left.loc[~left.facility_name.isin(known_missing.facility_name)]

    if len(left):
        logger.info("New entries without exact matches; proceeding to fuzzy matches")
    else:
        logger.info("All entries have exact matches; no additional matching necessary")

        # New crosswalk
        new_crosswalk = (
            exact[["facility_name", "school_name"]]
            .drop_duplicates()
            .reset_index(drop=True)
        )
        crosswalk = pd.concat([crosswalk, new_crosswalk]).drop_duplicates()

        # This will return if successful
        success, data2 = _test_merge(data, schools, crosswalk, known_missing)
        if success:
            return data2
        else:
            raise ValueError("This should not happen!")

    # Do the fuzzy merge
    out = []
    out.append(
        skool.fuzzy_merge(
            left,
            right,
            left_on="facility_name_clean",
            right_on="school_name_clean",
            score_cutoff=60,
            max_matches=1,
        ).assign(match="name")
    )
    out.append(
        skool.fuzzy_merge(
            left,
            right.dropna(subset=["school_abbreviation"]),
            left_on="facility_name_clean",
            right_on="school_abbreviation",
            score_cutoff=60,
            max_matches=1,
        ).assign(match="abbrev")
    )

    out = pd.concat(out).sort_values(
        [
            "match_probability",
            "facility_name_clean",
        ],
        ascending=False,
    )
    out = out.loc[~out.index.duplicated()]

    # Save the fuzzy matches
    out[
        [
            "facility_name",
            "school_name",
            "facility_address",
            "school_address",
            "match_probability",
            "match",
        ]
    ].to_excel(DATA_DIR / "interim" / "fuzzy_matches.xlsx", index=False)

    raise ValueError(
        "See data/interim/fuzzy_matches.xlsx for manual review of fuzzy matches"
    )
