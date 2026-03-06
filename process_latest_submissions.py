from dotenv import load_dotenv
import argparse
import datetime
import pandas as pd
from zoneinfo import ZoneInfo
import os

from APIs.google_spreadsheets import GoogleAPI
from APIs.owncloud import OwnCloudAPI
from processing.utils import get_last_data_timestamp, get_last_config_timestamp, \
                            load_config_versions, save_last_config_timestamp, \
                            save_last_data_timestamp
from processing.xml import FormXMLParser
from processing.process import process_site


def main():
    load_dotenv('CONFIG.env')

    google_api = GoogleAPI()
    owncloud_api = OwnCloudAPI()

    now = datetime.datetime.now(ZoneInfo("Europe/Paris"))
    last_run_timestamp = get_last_data_timestamp()
    last_config_timestamp = get_last_config_timestamp()

    raw_sheet_id = os.environ.get('RAW_SHEET_ID')
    raw_sheet_backup_id = os.environ.get('RAW_SHEET_BACKUP_ID')

    print(f'>>> {now}')

    # download new submitted sites
    subfolders = owncloud_api.get_new_folders(last_run_timestamp)

    if len(subfolders) > 0:
        # download configs
        downloaded = owncloud_api.get_new_config_files("logsheets", "downloaded_configs", last_config_timestamp)
        if downloaded:
            save_last_config_timestamp(now)
        
        # load configs
        configs = load_config_versions("downloaded_configs")

        data = dict()

        logsheet_names = dict()
        
        # process the new sites
        for subfolder in subfolders:
            print(f'>>> Downloading site {subfolder}...')
            files = owncloud_api.get_remote_files(subfolder)
            for filename, content in files:
                if filename != "site_metadata.xml":
                    xml = FormXMLParser()
                    xml.parse_string(content)
                    config = configs[xml.form_id][xml.logsheet_version]

                    # store the logsheet name for Google spreadsheet names
                    logsheet_names[xml.form_id] = config['name']

                    output = process_site(xml, config)
                    output["Site ID"] = xml.site_id
                    output["Submission date"] = xml.submitted_at
                    
                    data[xml.form_id] = data.get(xml.form_id, []) + [output]

        for form_id, submissions in data.items():
            print(f'>>> Processing form {form_id} with {len(submissions)} submissions...')

            processed_df = pd.DataFrame(submissions)

            # store to Google sheet
            print('\tStoring submissions in Google sheets...')
            row_dicts = processed_df.to_dict(orient="records")

            google_api.add_rows(raw_sheet_id, logsheet_names[form_id], row_dicts)
            google_api.add_rows(raw_sheet_backup_id, logsheet_names[form_id], row_dicts)

        # and update the last run timestamp
        print(f'>>> Updating last run timestamp...')
        save_last_data_timestamp(now)
    else:
        print(f'>>> No new submissions found.')

if __name__ == '__main__':
    args_parser = argparse.ArgumentParser(description='Process new logsheet submissions')

    args_parser._action_groups.pop()
    optional = args_parser.add_argument_group('optional arguments')
    
    args = args_parser.parse_args()
    main()
