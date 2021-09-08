import base64
from azure.storage.blob import (
    generate_blob_sas, 
    BlobServiceClient, 
    ContainerClient, 
    BlobClient,
    BlobProperties,
    BlobSasPermissions  
)


class AzureBlobStorageUtils:
    """
        Helper class for Azure Storage Functionlity.
    """
    CONN_STR = "DefaultEndpointsProtocol=https;AccountName={};AccountKey={};EndpointSuffix=core.windows.net"

    def __init__(self, account: str, key: str):
        """
        Constructor for StorageUtils class

        Params:
        storage_connnection_string : Full connection string to Azure Storage account
        """
        self.account_name = account
        self.account_key = key
        self.connection_string = AzureBlobStorageUtils.CONN_STR.format(
            self.account_name,
            self.account_key
        )

    def get_blob_hash(self, blob:str, container:str = None):
        """
        If container is none then parse the blob to get the container 
        from it (first part)
        """
        blob_hash = None

        container, blob = AzureBlobStorageUtils._parse_blob_parts(blob, container)


        blob_srv_client = BlobServiceClient.from_connection_string(self.connection_string)
        container_client = blob_srv_client.get_container_client(container)

        if container_client:
            blob_client = container_client.get_blob_client(blob)
            if blob_client:
                blob_props = blob_client.get_blob_properties()
                blob_hash = base64.b64encode(blob_props.content_settings.content_md5)
                blob_hash = blob_hash.decode('ascii')
                
        return blob_hash


    @staticmethod
    def _parse_blob_parts(blob:str, container:str = None ):
        blob_path = blob.replace("\\", "/")
        if blob.startswith("/"):
            blob = blob[1:]

        if container is None:
            blob_parts = blob.split('/')
            container = blob_parts[0]
            blob = "/".join(blob_parts[1:])

        return container, blob