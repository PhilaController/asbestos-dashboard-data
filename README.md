# asbestos-dashboard-data

Python toolkit for preprocessing data for the Philadelphia City Controller's School District Asbestos Dashboard.

The main command for pulling the latest data from the [Air Management Services online portal](https://www.citizenserve.com/philagov) and updating the dashboard is:

```bash
asbestos-dashboard-data update
```

This will update the processed asbestos data (`data/processed/asbestos.geojson`) and school data `data/processed/schools.geojson`. The latest data is also automatically uploaded to AWS s3.


### Local Development

Clone the repository and create a `.env` file (copying from the example) with your API keys. Then install the 
dependencies with:

```
poetry install
```

And then run the main update command:

```bash
poetry run asbestos-dashboard-data update
```