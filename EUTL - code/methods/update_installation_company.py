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

def update_installation_company(batch_size=2000):
    tr_map = {}
    ac_map = {}

    cursor = transaction.find({}, {
        "tr_installation_id": 1,
        "ac_installation_id": 1,
        "TRANSFERRING_INSTALLATION_PARENT_COMPANY": 1,
        "TRANSFERRING_INSTALLATION_SUBSIDIARY_COMPANY": 1,
        "ACQUIRING_INSTALLATION_PARENT_COMPANY": 1,
        "ACQUIRING_INSTALLATION_SUBSIDIARY_COMPANY": 1
    })

    for t in cursor:
        tr_id = t.get("tr_installation_id")
        ac_id = t.get("ac_installation_id")

        if tr_id:
            tr_map.setdefault(tr_id, []).append((
                t.get("TRANSFERRING_INSTALLATION_PARENT_COMPANY"),
                t.get("TRANSFERRING_INSTALLATION_SUBSIDIARY_COMPANY")
            ))
        if ac_id:
            ac_map.setdefault(ac_id, []).append((
                t.get("ACQUIRING_INSTALLATION_PARENT_COMPANY"),
                t.get("ACQUIRING_INSTALLATION_SUBSIDIARY_COMPANY")
            ))

    bulk_updates = []
    count = 0
    total = Installation_L2.count_documents({})
    cursor = Installation_L2.find({}, {"installation_id": 1})

    for record in cursor:
        acc_id = record.get("installation_id")
        type_set, type2_set = set(), set()

        for tp, tp2 in tr_map.get(acc_id, []):
            if tp:
                type_set.add(tp)
            if tp2:
                type2_set.add(tp2)

        for tp, tp2 in ac_map.get(acc_id, []):
            if tp:
                type_set.add(tp)
            if tp2:
                type2_set.add(tp2)

        final_type = next(iter(type_set)) if len(type_set) == 1 else ""
        final_type2 = next(iter(type2_set)) if len(type2_set) == 1 else ""

        bulk_updates.append(UpdateOne(
            {"installation_id": acc_id},
            {"$set": {
                "INSTALLATION_PARENT_COMPANY": final_type,
                "INSTALLATION_SUBSIDIARY_COMPANY": final_type2
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

    Installation_L2.update_many(
        {"INSTALLATION_PARENT_COMPANY": {"$exists": False}},
        {"$set": {"INSTALLATION_PARENT_COMPANY": ""}}
    )
    Installation_L2.update_many(
        {"INSTALLATION_SUBSIDIARY_COMPANY": {"$exists": False}},
        {"$set": {"INSTALLATION_SUBSIDIARY_COMPANY": ""}}
    )
    print("all done")
