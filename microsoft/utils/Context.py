import datetime
import typing
from . import (
    Configuration,
    StorageBlobValidationEntry,
    AzCliStorageUtil,
    AzureBlobStorageUtils,
    AzureTableStoreUtil
)

class BlobValidationResult:
    def __init__(self, entry:StorageBlobValidationEntry):
        self.validation_entry = entry
        self.current_hash = None
        self.validated = False

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

    def get_industry_validation_result(self, industry: str) -> typing.List[BlobValidationResult]:
        return_value = []
        results = self.search_table_store(industry)

        if results and len(results) > 0:
            print("Found",len(results), "results for", industry)
            for res in results:
                validation_result = BlobValidationResult(res)
                validation_result.current_hash = self.get_current_hash(res)
                validation_result.validated = validation_result.current_hash == res.md5

                return_value.append(validation_result)
            else:
                print("Found 0 results for", industry)


        return return_value

    def search_table_store(self, industry:str) -> typing.List[StorageBlobValidationEntry]:
        return self.validation_table_store.search_industry(
            self.configuration.historyStorage["table"], 
            industry
            )

    def get_history_entry(self, activity: str, actor:str):
        return {
            "timestamp" : datetime.datetime.utcnow().isoformat(),
            "activity" : activity,
            "actor" : actor
        }      


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