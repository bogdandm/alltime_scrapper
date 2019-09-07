import shutil
from pathlib import Path

PATH: Path = Path(__file__).parent.parent
DB_PATH = PATH / 'db.sqlite3'
TERMINAL_WIDTH = shutil.get_terminal_size((100, 20)).columns
