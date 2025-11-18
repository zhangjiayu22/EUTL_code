from pymongo import UpdateOne
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


def update_installation_code(collection, target_code, batch_size=1000):
    bulk_updates = []
    count = 0

    for record in collection.find({}, {"installation_id": 1, target_code: 1}):
        acc_id = record.get("installation_id")
        acc_type = record.get(target_code, "")

        if acc_id:
            bulk_updates.append(
                UpdateOne(
                    {"installation_id": acc_id},
                    {"$set": {target_code: acc_type}},
                    upsert=False
                )
            )

        if len(bulk_updates) >= batch_size:
            Installation_L2.bulk_write(bulk_updates, ordered=False)
            count += len(bulk_updates)
            bulk_updates = []
            print(f"update {count} records")

    if bulk_updates:
        Installation_L2.bulk_write(bulk_updates, ordered=False)
        count += len(bulk_updates)

    print(f"done writing {target_code}")

    result = Installation_L2.update_many(
        {target_code: {"$exists": False}},
        {"$set": {target_code: ""}}
    )
    print(f"update {target_code} as Null,  {result.modified_count} in total.")
