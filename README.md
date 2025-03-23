## Installation and Setup

This setup uses Python 3.12.2. Follow these steps:
## 1. Create the Conda Environment
```
$ conda create --name my_env python=3.12.2 -y
```
## 2. Activate the Environment
```
$ conda activate my_env
```
## 3. Install Dependencies
Install all required dependencies from `requirements.txt`
```
$ pip install -r requirements.txt
```
## 4. Set Up PostgreSQL with Docker

Uuse `docker-compose.yml` to download the PostgreSQL image and run the container:
```
$ docker-compose up -d
```
## Running
To run the python CLI app, use the following command with your desired coin ID and date range:
```
$ python main.py --coin-id <coin-id> --start-date <YYYY-MM-DD> --end-date <YYYY-MM-DD> --store
```
where:
- `<coin-id>` is the identifier for the cryptocurrency (e.g., `bitcoin`, `ethereum`).
- `<YYYY-MM-DD>` is the date in the format `Year-Month-Day` (e.g., `2025-01-01`).
- `--store` is an optional argument that indicates whether the downloaded data should be stored in the PostgreSQL database. If this argument is not provided, the data will only be saved locally in the `data` directory.
- Additionally, the date range specified by ``--start-date`` and `--end-date` can be replaced by a specific date using the `--date` argument (eg. `python main.py --date 2025-03-01 --coin-id bitcoin`).
- In case no argument is specified, the script will only download Bitcoin data for the previous day locally.
### Example:
To fetch data for Bitcoin, Ethereum and Cardano from January 1st to March 20th, 2025, use:
```
$ python main.py --start-date 2025-01-01 --end-date 2025-03-20 --coin-id bitcoin,ethereum,cardano --store
```
### Output:

- The data will be downloaded in **JSON** format in the `data` folder and stored in the PostgreSQL database.
- A log file will be created in the `logs` folder.
- A `analysis.txt` file will be created in the `outputs` folder with the following information:
    - Average price per coin and month (in USD)
    - Average price recovery after 3 days of consecutive price drops within an `n`-days window (in USD) (default `n=2`)
- Plots showing price trends for each currency will be saved in the `plots` folder.
- A `models_MAE_metric.json` file in the `outputs` folder will contain the MAE (Mean Absolute Error) metric and the predicted price for the next day for both the linear regression model and the XGBoost model for each coin.

## 2. Configure periodic download with CRON

To configure periodic download with CRON, run the following command:
```
$ python src/setup_cron.py
```
By default, the script will download data from Bitcoin, Ethereum, and Cardano every day at 3 a.m. But it can be configured in `settings.py`

Note: Verify that the CRON service is active:
```
$ sudo systemctl status cron
```
On the other hand, you can check if there are any scheduled tasks with the command:
```
$ crontab -l
```

and remove all of them with:
```
$ crontab -r
```
## Implemented Features
### Task 1:
1.  The CLI application receives a date and a coin identifier, then downloads the data from the API. The data is stored locally in a JSON file in `data` folder.
2.  A log file is generated for each run, and the app can be configured to download data every day. By default,  Bitcoin, Ethereum, and Cardano data will be download daily at 3 AM. This can be set up from th `settings.py` file.
3.  The application supports downloading and processing data for a date range. The progress is monitored in the log file.

Note: The process runs concurrently with a configurable limit set in the `settings.py` file.
### Task 2:
1.  Two tables were created with the required specifications, and the corresponding SQL queries were written in `.sql` files (see `01_create_raw_data_table.sql` and `02_create_aggregated_data_table.sql` in the `sql` folder). Additionally, the `volume_usd` field was added to the database since it will be used for training machine learning models in **Task 4**.

Note: Database manager follows Singleton Pattern.

2.  The application includes an optional `--store` argument that allows storing data in the PostgreSQL database. It also updates the monthly maximum and minimum values for each coin. This can be verified by checking the database and observing how these values change when new data is added.

Note: `SQLAlchemy` was used following best practices for database management in Python.
### Task 3:
1.  The average values per coin per month were calculated (see the `output` folder).
2.  The price increase was calculated after a three-day consecutive drop within a two-day window, which is configurable in settings.py. The market cap was also included.

Note: The results will be shown in a `.txt` file (see `analysis.txt` at `outputs` folder). The queries were written in an `.sql` file (see `analysis_queries.sql` at `sql` folder).
### Task 4:
1.  The graphs displaying the prices of Bitcoin, Ethereum, and Cardano for the last 30 days can be found in the `saved_30days_plots` folder.
2.  The requested features have been added.
3.  The data has been reprocessed as requested, including holidays from China and the US.
4.  Two models were trained: Linear Regression and XGBoost, both aimed at predicting the price for the next day (T1).
