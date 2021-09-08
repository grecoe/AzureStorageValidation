import typing
import datetime
from .AzureTableValidationEntry import StorageBlobValidationEntry
from azure.data.tables import TableServiceClient, TableClient
from azure.data.tables._entity import EntityProperty
from azure.data.tables._deserialize import TablesEntityDatetime
from azure.core.exceptions import ResourceExistsError

class AzureTableStoreUtil:
    CONN_STR = "DefaultEndpointsProtocol=https;AccountName={};AccountKey={};EndpointSuffix=core.windows.net"

    def __init__(self, account_name:str, account_key:str):
        self.connection_string = AzureTableStoreUtil.CONN_STR.format(
            account_name,
            account_key
        )

    def search_industry(self, table_name:str, industry:str):
        return_records = []
        with self._get_table_client(table_name) as table_client:
            results = table_client.list_entities()
            for result in results:
                entity_record = {}
                
                for key in result:
                    value = result[key]

                    if isinstance(result[key], EntityProperty): 
                        value = result[key].value
                    if isinstance(result[key], TablesEntityDatetime):
                        value = datetime.datetime.fromisoformat(str(result[key]))

                    entity_record[key] = value

                if "industry" in entity_record and industry in entity_record["industry"]:
                    return_records.append(
                        StorageBlobValidationEntry.create_from_record(
                            table_name,
                            entity_record)
                        )
        
        return return_records


    def delete_records(self, table_name:str, records:typing.List[typing.Tuple[str,str]]) -> None:
        """
        Delete records from a table
        
        Parameters:
        table_name - name of table to remove. 
        records - List of tuples that are (RowKey,PartitionKey)
        """
        with self._get_table_client(table_name) as table_client:
            for pair in records:
                table_client.delete_entity(
                    row_key=pair[0], 
                    partition_key=pair[1]
                    )

    def add_record(self, table_name:str, entity:dict):
        """
        Add a record to a table

        Parameters:
        table_name - Name of table to add to
        entity - Dictionary of non list/dict data
        """
        with self._create_table(table_name) as log_table:
            try:
                resp = log_table.create_entity(entity=entity)
            except ResourceExistsError as ex:
                resp = log_table.update_entity(entity=entity)
            except Exception as ex:
                print("Unknown table error")
                print(str(ex))

    def _create_table(self, table_name:str) -> TableClient:
        """
        Ensure a table exists in the table storage 
        """
        with TableClient.from_connection_string(conn_str=self.connection_string, table_name=table_name) as table_client:
            try:
                table_client.create_table()
            except Exception as ex:
                pass
                
        return self._get_table_client(table_name)

    def _get_table_client(self, table_name: str) ->TableClient:
        """Searches for and returns a table client for the specified
        table in this account. If not found throws an exception."""
        return_client = None

        with TableServiceClient.from_connection_string(conn_str=self.connection_string) as table_service:
            name_filter = "TableName eq '{}'".format(table_name)
            queried_tables = table_service.query_tables(name_filter)

            found_tables = []
            for table in queried_tables:
                # Have to do this as its an Item_Paged object
                if table.name == table_name:
                    found_tables.append(table)
                    break 
        
            if found_tables and len(found_tables) == 1:
                return_client = TableClient.from_connection_string(conn_str=self.connection_string, table_name=table_name)
            else:
                return_client = None
                print("WARNING - Table {} not found".format(table_name))
                # raise Exception("Table {} not found".format(table_name))

        return return_client  