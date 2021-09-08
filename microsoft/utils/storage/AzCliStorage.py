
from ..cli.CmdUtils import CmdUtils

class AzStorageAccount:
    def __init__(self, name:str, keys:list, subscription:str):
        self.name = name
        self.keys = keys
        self.subscription = subscription

class AzCliStorageUtil:
    @staticmethod
    def get_storage_account(storage_account:str, subscription:str) -> AzStorageAccount:
        command = "az storage account keys list --account-name {} --subscription {}".format(
            storage_account,
            subscription
        )
        keys = CmdUtils.get_command_output(command.split(" "))
        key_vals = []
        for key in keys:
            key_vals.append(key["value"])

        return AzStorageAccount(storage_account, key_vals, subscription)

