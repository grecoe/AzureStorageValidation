import datetime

class ProcessEntry:
    def __init__(self, table_name:str, partition_key:str):
        self.table_name = table_name
        self.RowKey = datetime.datetime.utcnow().isoformat()
        self.PartitionKey = partition_key.replace("/","_")

    def get_entity(self):
        """
        Entity is everyting in self.__dict__ EXCEPT the 
        table name.
        """
        entity = {}
        for prop in self.__dict__:
            if prop != 'table_name':
                entity[prop] = self.__dict__[prop]

        return entity

class StorageBlobValidationEntry(ProcessEntry):
    def __init__(self, table_name, blob_name):
        super().__init__(table_name, blob_name)
        self.industry = None
        self.md5 = None
        self.account = None
        self.subscription = None
        self.blob = blob_name
        self.actor = None

    @staticmethod
    def create_from_record(table:str, settings:dict) -> object:
        return_val = StorageBlobValidationEntry(table, settings["blob"])
        return_val.RowKey = settings["RowKey"]
        return_val.industry = settings["industry"]
        return_val.md5 = settings["md5"]
        return_val.account = settings["account"]
        return_val.subscription = settings["subscription"]
        return_val.blob = settings["blob"]
        return_val.actor = settings["actor"]

        return return_val
