from pymongo import MongoClient
client = MongoClient('mongodb://localhost:27017/')
db = client['EUTL-1118']
transaction = db['transaction']
operators_yearly_activity_daily = db['operators_yearly_activity_daily']
operators_daily = db['operators_daily']
accounts_daily = db['accounts_daily']
Transaction_L0 = db['Transaction_L0']
Transaction_L1 = db['Transaction_L1']
Transaction_L2 = db['Transaction_L2']
Compliance_L0 = db['Compliance_L0']
Compliance_L1 = db['Compliance_L1']
Compliance_L2 = db['Compliance_L2']
Accounts_L0 = db['Accounts_L0']
Accounts_L1 = db['Accounts_L1']
Accounts_L2 = db['Accounts_L2']
Installation_L0 = db['Installation_L0']
Installation_L1 = db['Installation_L1']
Installation_L2 = db['Installation_L2']
def construct_l0(
        source_collection,
        target_collection,
        fields,
):
    projection = {field: 1 for field in fields}
    documents = list(source_collection.find({}, projection))
    target_collection = db[target_collection]
    result = target_collection.insert_many(documents)
    inserted_count = len(result.inserted_ids)
    print(f" inserted {inserted_count} records.")


def construct_ids_l0(
        collection_field_pairs,
        target_collection,
        field_name
):
    all_ids = set()
    for collection, field in collection_field_pairs:
        ids = collection.distinct(field)
        all_ids.update(ids)

    documents = [{field_name: value} for value in all_ids if value is not None]
    target_collection = db[target_collection]
    if documents:
        result = target_collection.insert_many(documents)
        print(f"inserted {len(result.inserted_ids)} records to {target_collection.name}")
        return result
    else:
        print("No results")
        return None
