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

def update_main_activity(batch_size=2000):
    tr_map = {}
    ac_map = {}


    cursor = transaction.find({}, {
        "tr_installation_id": 1,
        "ac_installation_id": 1,
        "TRANSFERRING_INSTALLATION_MAIN_ACTIVITY": 1,
        "ACQUIRING_INSTALLATION_MAIN_ACTIVITY": 1,
    })


    for t in cursor:
        tr_id = t.get("tr_installation_id")
        ac_id = t.get("ac_installation_id")

        if tr_id:
            tr_map.setdefault(tr_id, []).append((
                t.get("TRANSFERRING_INSTALLATION_MAIN_ACTIVITY")
            ))
        if ac_id:
            ac_map.setdefault(ac_id, []).append((
                t.get("ACQUIRING_INSTALLATION_MAIN_ACTIVITY")
            ))


    bulk_updates = []
    count = 0
    total = Installation_L2.count_documents({})
    cursor = Installation_L2.find({}, {"installation_id": 1})

    for record in cursor:
        acc_id = record.get("installation_id")
        type_set = set()

        for tp in tr_map.get(acc_id, []):
            if tp:
                type_set.add(tp)

        for tp in ac_map.get(acc_id, []):
            if tp:
                type_set.add(tp)

        final_type = next(iter(type_set)) if len(type_set) == 1 else ""

        bulk_updates.append(UpdateOne(
            {"installation_id": acc_id},
            {"$set": {
                "INSTALLATION_MAIN_ACTIVITY": final_type,
            }}
        ))

        if len(bulk_updates) >= batch_size:
            Installation_L2.bulk_write(bulk_updates, ordered=False)
            count += len(bulk_updates)
            print(f"update {count} records")
            bulk_updates = []

    if bulk_updates:
        Installation_L2.bulk_write(bulk_updates, ordered=False)
        count += len(bulk_updates)

    print(f"all done")

    Installation_L2.update_many(
        {"INSTALLATION_MAIN_ACTIVITY": {"$exists": False}},
        {"$set": {"INSTALLATION_MAIN_ACTIVITY": ""}}
    )
    print("update missing fields as Null")
