
"""
Main application for storage validation. 

Ingest files to table storage:
python app.py -ingest -settings "./exampleinput.json"

Validate files in table storage:
python app.py -validate -industry INDUSTRY_OR_FILTER_IN_CONFIG_JSON

Rebase files in table storage
python app.py -rebase -industry INDUSTRY_OR_FILTER_IN_CONFIG_JSON

"""
import sys
from microsoft.utils import (
    AzLoginUtils, 
    AzCliStorageUtil,
    AzureTableStoreUtil,
    AzureBlobStorageUtils,
    Configuration,
    ProgramArguments,
    StorageBlobValidationEntry
)

def get_current_hash(existing_entry: StorageBlobValidationEntry) -> str:
    return get_blob_hash(
        existing_entry.account,
        existing_entry.subscription,
        existing_entry.blob
    )

def get_blob_hash(account: str, subscription: str, blob: str):
    blob_storage = AzCliStorageUtil.get_storage_account(
        account,
        subscription) 
                 
    stgutil = AzureBlobStorageUtils(blob_storage.name, blob_storage.keys[0])

    return stgutil.get_blob_hash(blob)

# Load configuration and validate that we have a login
credentials_file = ".\\credentials.json"
configuration_file = ".\\configuration.json"

configuration = Configuration(configuration_file)
script_actor = AzLoginUtils.validate_login(credentials_file)

# Collect the arguments and validate them
app_arguments = ProgramArguments(sys.argv[1:])
app_arguments.validate_args()
if app_arguments.industry:
    if app_arguments.industry not in configuration.industries:
        print("The only acceptable industries are:")
        print(configuration.industries)
        quit()

# Regardless what we are going to do, we will need the validation
# storage table to read or update
validation_storage_account = AzCliStorageUtil.get_storage_account(
    configuration.historyStorage["account"],
    configuration.historyStorage["subscription"])

validation_table_store = AzureTableStoreUtil(
    validation_storage_account.name, 
    validation_storage_account.keys[0]
)


# Now figure out what it is we are doing.
if app_arguments.validate:
    """
    Get all records for a given industry and compare the current hash
    to the last hash that was recorded in the table. 

    Only prints out if good or bad for each one. 
    """
    print("\nValidating current hashes for industry", app_arguments.industry)

    results = validation_table_store.search_industry(
        configuration.historyStorage["table"], 
        app_arguments.industry
        )
    
    print("Found", len(results), "records for", app_arguments.industry)

    if results and len(results) > 0:
        for res in results:
            current_hash = get_current_hash(res)
            valid = current_hash == res.md5
            print("Validation for", res.blob,"=", valid)

if app_arguments.rebase:
    """
    Get all records for a given industry and update the hash if the stored
    one is different from the the current one. 
    """
    print("\nRebasing hashes for industry", app_arguments.industry)

    results = validation_table_store.search_industry(
        configuration.historyStorage["table"], 
        app_arguments.industry
        )
    
    print("Found", len(results), "records for", app_arguments.industry)

    if results and len(results) > 0:
        for res in results:
            current_hash = get_current_hash(res)
            
            if current_hash != res.md5:
                print("Updating hash for", res.blob)
                res.md5 = current_hash
                validation_table_store.add_record(res.table_name, res.get_entity())
            else:
                print("Unchanged hash for", res.blob)

if app_arguments.ingest:
    """
    Load up all of the blobs/hashes to tracking table
    """
    print("\nIngesting", app_arguments.settings)

    ingest_settings = Configuration(app_arguments.settings)

    # Make sure we have what we need
    if not ingest_settings.industry or not ingest_settings.account or not ingest_settings.subscription or len(ingest_settings.blobs) == 0:
        print("Settings are incorrect")
        quit()

    # Industry has to be in our list
    if ingest_settings.industry not in configuration.industries:
        print("The only acceptable industries are:")
        print(configuration.industries)

    # Get existing ones so we don't create duplicates
    results = validation_table_store.search_industry(
        configuration.historyStorage["table"], 
        ingest_settings.industry
        )

    print("Ingesting blobs for ", ingest_settings.industry, "in", ingest_settings.account )

    for blob in ingest_settings.blobs:

        existing_entry = [x for x in results if x.account == ingest_settings.account and x.blob == blob]
        if existing_entry and len(existing_entry) > 0:
            existing_entry = existing_entry[0]
        else:
            existing_entry = None

        print("\nEntry for", blob,"in", ingest_settings.account, "exists:" , existing_entry != None)

        blob_hash = get_blob_hash(
            ingest_settings.account,
            ingest_settings.subscription,
            blob
        )

        if existing_entry:
            if existing_entry.md5 != blob_hash: 
                print("Updating hash for", blob)
                existing_entry.md5 = blob_hash
                validation_table_store.add_record(existing_entry.table_name, existing_entry.get_entity())
            else:
                print("Hash for", blob, "in", ingest_settings.account, "unchanged.")
        else:
            print("Creating entry for", blob,"in", ingest_settings.account, "exists:" , existing_entry != None)

            blob_entry = StorageBlobValidationEntry(configuration.historyStorage["table"], blob)
            blob_entry.account = ingest_settings.account
            blob_entry.subscription = ingest_settings.subscription
            blob_entry.industry = ingest_settings.industry
            blob_entry.md5 = blob_hash
            blob_entry.actor = script_actor

            print("Adding entry for", blob, "in", ingest_settings.account)
            validation_table_store.add_record(
                configuration.historyStorage["table"],
                blob_entry.get_entity()
            )


print("Tasks complete!")
