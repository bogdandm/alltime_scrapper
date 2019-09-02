import shutil
from pathlib import Path

DOMAIN = "https://www.alltime.ru"
CATALOG_PAGE = DOMAIN + "/watch/man/"
CATALOG_PAGES = 150
ITEMS_PER_PAGE = 100
HTML_ENCODING = 'windows-1251'
PARALLEL_CONNECTIONS = 10  # per process
RETRY_AFTER = 0.5  # sec

PATH: Path = Path(__file__).parent.parent
DB_PATH = PATH / 'db.sqlite3'
TERMINAL_WIDTH = shutil.get_terminal_size((100, 20)).columns
