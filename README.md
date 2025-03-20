# Installation and Setup

We will use Python 3.12.2 for this setup. 

First, prepare the Conda environment:

## 1. Create the Conda Environment
```
$ conda create --name my_env python=3.12.2 -y
```
## 2. Activate the Environment
```
$ conda activate my_env
```
## 3. Install Dependencies from `requirements.txt`
```
$ pip install -r requirements.txt
```
Finally, download the PostgreSQL image and run the container:
## 4. Download the PostgreSQL 17.4 Docker Image

To download the latest PostgreSQL 17.4 image from Docker Hub, run the following command:
```
$ docker pull postgres:17.4
```
## 5. Running PostgreSQL with Docker Compose

We use `docker-compose.yml` to set up a PostgreSQL container:
```
$ docker-compose up -d
```
# Task 1: Getting crypto token data

## 1. Download of a particular currency on a specific day

To run the script, use the following command with your desired coin ID and date:
```
$ python src/fetcher.py --coin-id <coin-id> --date <YYYY-MM-DD>
```
where:
- `<coin-id>` is the identifier for the cryptocurrency (e.g., `bitcoin`, `ethereum`).
- `<YYYY-MM-DD>` is the date in the format `Year-Month-Day` (e.g., `2025-01-01`).

### Example:

To fetch data for Bitcoin on January 1st, 2025, use:
```
$ python src/fetcher.py --coin-id bitcoin --date 2025-01-01
```

### Output:

- The data will be downloaded in **JSON format** and stored in: `data/downloads/single/`

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

## 3. Bulk-processing for a time range

To fetch historical data for a range of dates for specific coins, you can run the script with the --start-date and --end-date arguments. You can also specify the coins you want to fetch data for using the --coin-id argument (optional).

### Example: Fetch data for multiple coins (bitcoin, ethereum) from March 1st, 2025 to March 3rd, 2025
```
$ python src/fetcher.py --start-date 2025-03-01 --end-date 2025-03-03 --coin-id bitcoin,ethereum
```

### Example: Fetch data for the default coins (bitcoin, ethereum, cardano) from March 1st, 2025 to March 3rd, 2025
```
$ python src/fetcher.py --start-date 2025-03-01 --end-date 2025-03-03
```

### Example: Fetch data for a single coin (bitcoin) from March 1st, 2025 to March 3rd, 2025
```
$ python src/fetcher.py --start-date 2025-03-01 --end-date 2025-03-03 --coin-id bitcoin
```

### Default behavior

If no arguments are provided, the script will fetch data for today's date (`today`) for the default coins (`bitcoin, ethereum, cardano`).
```
$ python src/fetcher.py
```
### Output:

- The data will be downloaded in **JSON format** and stored in: `data/downloads/bulk/`

### Bonus

The bulk processing is running in a concurrent way using the `concurrent` library..

# Task 2: Loading data into the database

## 1. and 2. 

In order to create the two tables specified in the challenge run the following code:
```
$ python src/loader.py
```
Note 1: Note that in this case, only the specified tables and their columns have been created. However, they are empty.

If you want to create the tables with their columns and actually load them with data, we have to add the `--store` argument:
```
$ python src/loader.py --store
```
Note 2: By default, the database is loaded with information from the files in the `data/testing` folder. This can be modified from `settings.py`.

Note 3: Although it was not a requirement, we have added `volume_usd` to the `raw_crypto_data` table since we will use it to train the model in Task 4.

Note 4: The maximum and minimum values ​​for each currency per month are automatically updated when we load new data. This can be verified as follows:

### Example

First, enter the container:
```
$ docker exec -it postgres_db psql -U gcrisnejo -d crypto_db
```
Second, display the maximum and minimum values:
```
crypto_db=# SELECT coin_id, year, month, max_price, min_price FROM aggregated_crypto_data;
```
Output: 
```
coin_id  | year | month |     max_price      |     min_price      
----------+------+-------+--------------------+--------------------
 ethereum | 2025 |     2 |  3296.390634843652 | 2603.0333698049267
 cardano  | 2025 |     1 |  1.084480992215901 | 0.8442610184323533
 ethereum | 2025 |     1 | 3447.0340476747956 | 3076.4937692857234
 bitcoin  | 2025 |     1 | 104835.19253555956 |  92376.27578346101
 bitcoin  | 2025 |     2 |  97836.18856127483 |  83900.11496524839
 cardano  | 2025 |     2 | 0.8102560488773543 | 0.6446208040699569
(6 rows)
```
Third, copy the files in `data/added_files`to `data/testing`
```
$ cp data/added_files/* data/testing/
```

Fourth load them into the database:
```
$ python src/loader.py --store
```
Finally, display again the maximum and minimum values in the database and compare:
```
crypto_db=# SELECT coin_id, year, month, max_price, min_price FROM aggregated_crypto_data;
```
Output: 
```
coin_id  | year | month |     max_price      |     min_price      
----------+------+-------+--------------------+--------------------
 ethereum | 2025 |     2 |  3296.390634843652 | 2305.3229378029837
 ethereum | 2025 |     1 | 3687.1447140588307 | 3076.4937692857234
 cardano  | 2025 |     1 | 1.1369537229272808 | 0.8442610184323533
 bitcoin  | 2025 |     1 |  106182.2368201815 |  92376.27578346101
 bitcoin  | 2025 |     2 | 102382.39409722165 |  83900.11496524839
 cardano  | 2025 |     2 | 0.9425307818897108 | 0.6446208040699569
(6 rows)
```
You can check that the maximum and minimum were updated.

Note 5: No previous records are lost, only updated if needed. This means that if there is already a record for the same `coin_id` and `date`, the new data will not be inserted.

# Task 3: Analysing coin data with SQL

Run the following command:
```
$ python src/analyzer.py
```

# Task 4: Finance meets Data Science
Run the following command:
```
$ python src/builder.py
```
