import os
import sys
from dotenv import load_dotenv

from APIs.google_spreadsheets import GoogleAPI


def cleanup_spreadsheet(env_key: str) -> None:
    """
    Delete all data rows (but keep header row and its formatting)
    from every worksheet in the spreadsheet referenced by ENV[env_key].
    """
    load_dotenv("CONFIG.env")
    file_key = os.environ.get(env_key)
    if not file_key:
        raise SystemExit(f"Environment variable {env_key!r} is not set.")

    api = GoogleAPI()
    spreadsheet = api.client.open_by_key(file_key)

    for ws in spreadsheet.worksheets():
        api.clear_worksheet_data(file_key, ws.title)
        print(f"Cleared data rows in sheet '{ws.title}' (kept header).")


def main(argv: list[str]) -> None:
    env_key = argv[1]
    if ',' in env_key:
        for key in env_key.split(','):
            print(f">>> Cleaning up spreadsheet {key}...")
            cleanup_spreadsheet(key)
    else:
        print(f">>> Cleaning up spreadsheet {env_key}...")
        cleanup_spreadsheet(env_key)

if __name__ == "__main__":
    main(sys.argv)
