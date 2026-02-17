
import sys
from accelerators.databricks.services.databricks_service import DatabricksService

sys.path.append('.')
from common.scripts import (direct_data_to_object_storage, download_and_unzip_direct_data_files,
                            extract_doc_content, load_data, retrieve_doc_text)
from common.services.azure_blob_service import AzureBlobService
from common.services.vault_service import VaultService
from common.utilities import log_message, read_json_file
from datetime import date




def main():
    config_filepath: str = "accelerators/databricks/resources/connector_config.json"
    vapil_settings_filepath: str = "accelerators/databricks/resources/vapil_settings.json"
    vault_service: VaultService = VaultService(vapil_settings_filepath)

    config_params: dict = read_json_file(config_filepath)
    direct_data_params: dict = config_params['direct_data']
    # adding dynamic stop time to ensure we pull the most recent file available in the vault
    direct_data_params['stop_time'] = f"{date.today().strftime('%Y-%m-%dT%H:%MZ')}"

    # using the stop time, getting the file name for the most recent direct data extract available in the vault that matches the extract type and start/stop time criteria
    # Will need to add logic for multiple file parts
    log_message(log_level='Info',
                message=f'before get file name call')
    file_name: str = direct_data_to_object_storage.get_file_name(vault_service=vault_service, direct_data_params=direct_data_params)
    
    # Creating the full object path and extract folder for where the file will be stored in object storage by combining the folder path and file name
    blob_params: dict = config_params['blob']
    blob_params["archive_filepath"] = f"direct-data/{file_name}"
    blob_params["extract_folder"] = file_name.split('.tar')[0]
    log_message(log_level='Info',
                message=f'Blob name information: {blob_params["archive_filepath"]}')
    databricks_params: dict = config_params['databricks']

    extract_document_content: bool = config_params.get('extract_document_content')
    retrieve_document_text: bool = config_params.get('retrieve_document_text')

    object_storage_root: str = f'abfs://{blob_params["account_url"]}{blob_params["container"]}'

    blob_params['convert_to_parquet'] = config_params['convert_to_parquet']
    databricks_params['convert_to_parquet'] = config_params['convert_to_parquet']
    databricks_params['object_storage_root'] = object_storage_root

    blob_service: AzureBlobService = AzureBlobService(blob_params)
    databricks_service: DatabricksService = DatabricksService(databricks_params)
    #vault_service: VaultService = VaultService(vapil_settings_filepath)
    
    direct_data_to_object_storage.run(vault_service=vault_service,
                                      object_storage_service=blob_service,
                                      direct_data_params=direct_data_params)

    download_and_unzip_direct_data_files.run(object_storage_service=blob_service)

    load_data.run(object_storage_service=blob_service,
                  database_service=databricks_service,
                  direct_data_params=direct_data_params)

    if extract_document_content:
        extract_doc_content.run(object_storage_service=blob_service,
                                vault_service=vault_service)

    if retrieve_document_text:
        retrieve_doc_text.run(object_storage_service=blob_service,
                              vault_service=vault_service)


if __name__ == "__main__":
    main()
