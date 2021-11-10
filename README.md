# asbestos-dashboard-data

Python toolkit for preprocessing data for the Philadelphia City Controller's [School District Asbestos Dashboard](https://controller.phila.gov/philadelphia-audits/interactive-asbestos-dashboard/#/).

The main command for pulling the latest data from the [Air Management Services online portal](https://www.citizenserve.com/philagov) and updating the dashboard is:

```bash
asbestos-dashboard-data update
```

This will update the processed asbestos data (`data/processed/asbestos-data.geojson`) and school data `data/processed/schools.geojson`. The latest data is also automatically uploaded to AWS s3.

### Steps

1. Downloads the latest raw data from the online portal. 
1. Determines if there are any new entries. 
1. Geocode new addresses. The code attempts to geocode these addresses by first looking in `data/interim/geocoded_addresses.xlsx` to see if a match already exists. If not, it will use AIS to geocode the addresses. 
    - If this step fails, the update code will raise an error
    - In this case, you will need to add a new entry into the `data/interim/manual_geocoded_addresses.xlsx` with your manual geocode. You can find the missing addresses in `data/interim/missing_geocoded_addresses.xlsx`.
1. Match new entries to schools. The steps are:
    - Crossmatch to the existing crosswalk (`data/interim/crosswalk.xslx`)
    - If any didn't match, do an exact match to the full list of schools. 
    - If any didn't match, do a fuzzy match on school name. At this point, the code will raise an error and a file with the fuzzy matches will be saved (`data/interim/fuzzy_matches.xlsx`).
    - Review the fuzzy matches and copy the correct matches to the main crosswalk file, and re-run the code. 
1. Upload the cleaned data file to s3 and save it to the `data/processed` folder.

## Local Development

Clone the repository and create a `.env` file (copying from the example) with your API keys. Then install the 
dependencies with:

```
poetry install
```

And then run the main update command:

```bash
poetry run asbestos-dashboard-data update
```