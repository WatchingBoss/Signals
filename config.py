import os
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

INVEST_TOKEN = os.getenv("INVEST_TOKEN")
DATA_DIR_NAME = os.getenv("DATA_DIR")

data_path = Path(os.path.join(os.path.curdir, DATA_DIR_NAME))
