from pathlib import Path
from datetime import datetime

now = datetime.now()
today_date = now.strftime("%Y-%m-%d")
run_time = now.strftime("%Y-%m-%d_%H-%M-%S")


# Paths
PROJ_ROOT = Path.cwd()

#logs dir
LOGS_DIR = PROJ_ROOT/"logs"/today_date
LOGS_DIR.mkdir(parents=True, exist_ok=True)
log_file_name = LOGS_DIR / f"{run_time}.log"

#data directories
DATA_DIR = PROJ_ROOT / "data"
BRONZE_DIR = DATA_DIR/ "bronze"
SILVER_DIR = DATA_DIR/ "silver"
GOLD_DIR = DATA_DIR/ "gold"

#model artifacts
MODELS_DIR = PROJ_ROOT / "models"

#reports and figures dir
REPORTS_DIR = PROJ_ROOT / "reports"
FIGURES_DIR = REPORTS_DIR / "figures"

data_paths = {

    "bronze" : {

        "raw_paths" : {

            "sales" : f"{BRONZE_DIR}/ raw/ sales.csv",
            "calendar" : f"{BRONZE_DIR}/ raw/ calendar.csv",
            "price" : f"{BRONZE_DIR}/ raw/ sell_prices.csv",
        },

        "processed_data" : {

            "sales_long" : f"{BRONZE_DIR}/processed_data/sales_long",
            "joined_data" : f"{BRONZE_DIR}/processed_data/joined_data",


        },

    },

    "silver" : {}

        
    }


