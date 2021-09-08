
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
    Configuration,
    ProgramArguments,
    StorageBlobValidationEntry,
    Context
)


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

application_context = Context(configuration)


# Now figure out what it is we are doing.
if app_arguments.validate:
    """
    Get all records for a given industry and compare the current hash
    to the last hash that was recorded in the table. 

    Only prints out if good or bad for each one. 
    """
    print("\nValidating current hashes for industry", app_arguments.industry)

    results = application_context.search_table_store(app_arguments.industry)

    print("Found", len(results), "records for", app_arguments.industry)

    if results and len(results) > 0:
        for res in results:
            current_hash = application_context.get_current_hash(res)
            valid = current_hash == res.md5
            print("Validation for", res.blob,"=", valid)

if app_arguments.rebase:
    """
    Get all records for a given industry and update the hash if the stored
    one is different from the the current one. 
    """
    print("\nRebasing hashes for industry", app_arguments.industry)

    results = application_context.search_table_store(app_arguments.industry)

    print("Found", len(results), "records for", app_arguments.industry)

    if results and len(results) > 0:
        for res in results:
            current_hash = application_context.get_current_hash(res)
            
            if current_hash != res.md5:
                print("Updating hash for", res.blob)
                res.md5 = current_hash
                application_context.add_table_record(res)
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
    results = application_context.search_table_store(ingest_settings.industry)

    print("Ingesting blobs for ", ingest_settings.industry, "in", ingest_settings.account )

    for blob in ingest_settings.blobs:

        existing_entry = [x for x in results if x.account == ingest_settings.account and x.blob == blob]
        if existing_entry and len(existing_entry) > 0:
            existing_entry = existing_entry[0]
        else:
            existing_entry = None

        print("\nEntry for", blob,"in", ingest_settings.account, "exists:" , existing_entry != None)

        blob_hash = application_context.get_blob_hash(
            ingest_settings.account,
            ingest_settings.subscription,
            blob
        )

        if existing_entry:
            if existing_entry.md5 != blob_hash: 
                print("Updating hash for", blob)
                existing_entry.md5 = blob_hash
                application_context.add_table_record(existing_entry)
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
            application_context.add_table_record(blob_entry)

print("Tasks complete!")
