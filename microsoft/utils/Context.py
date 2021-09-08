import typing
from . import (
    Configuration,
    StorageBlobValidationEntry,
    AzCliStorageUtil,
    AzureBlobStorageUtils,
    AzureTableStoreUtil
)

class Context:
    def __init__(self, config: Configuration):
        self.configuration = config

        self.validation_storage_account = AzCliStorageUtil.get_storage_account(
            self.configuration.historyStorage["account"],
            self.configuration.historyStorage["subscription"])

        self.validation_table_store = AzureTableStoreUtil(
            self.validation_storage_account.name, 
            self.validation_storage_account.keys[0]
        )

    def search_table_store(self, industry:str) -> typing.List[StorageBlobValidationEntry]:
        return self.validation_table_store.search_industry(
            self.configuration.historyStorage["table"], 
            industry
            )

    def add_table_record(self, entry: StorageBlobValidationEntry):
        table = entry.table_name

        if not table:
            table = self.configuration.historyStorage["table"]
            
        self.validation_table_store.add_record(
                table,
                entry.get_entity()
            )

    def get_current_hash(self, existing_entry: StorageBlobValidationEntry) -> str:
        return self.get_blob_hash(
            existing_entry.account,
            existing_entry.subscription,
            existing_entry.blob
        )

    def get_blob_hash(self, account: str, subscription: str, blob: str):
        blob_storage = AzCliStorageUtil.get_storage_account(
            account,
            subscription) 
                 
        stgutil = AzureBlobStorageUtils(blob_storage.name, blob_storage.keys[0])

        return stgutil.get_blob_hash(blob)