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


def update_account_L1(collection, target_code, target_code_tr, target_code_ac, new_code):
    operator_map = {}
    for doc in collection.find(
            {"account_id": {"$ne": None}},
            {"account_id": 1, target_code: 1, "_id": 0}
    ):
        if doc.get(target_code):
            operator_map[doc["account_id"]] = doc[target_code]

    print(f"done loading {collection}")

    tr_map = {}
    ac_map = {}

    for doc in transaction.find(
            {},
            {
                "tr_account_id": 1,
                "ac_account_id": 1,
                target_code_tr: 1,
                target_code_ac: 1,
                "_id": 0
            }
    ):
        tr_id = doc.get("tr_account_id")
        ac_id = doc.get("ac_account_id")

        if tr_id and doc.get(target_code_tr):
            tr_map[tr_id] = doc[target_code_tr]

        if ac_id and doc.get(target_code_ac):
            ac_map[ac_id] = doc[target_code_ac]

    print(f"done loading transaction")

    account_cursor = Accounts_L1.find({}, {"account_id": 1, "_id": 0})
    bulk_updates = []
    count = 0
    for acc in account_cursor:
        account_id = acc.get("account_id")
        operator_val = operator_map.get(account_id)
        tr_val = tr_map.get(account_id)
        ac_val = ac_map.get(account_id)

        vals = [v for v in [operator_val, tr_val, ac_val] if v is not None]

        if not vals:
            continue

        update_fields = {}

        unique_vals = list(set(vals))

        if len(unique_vals) == 1:
            update_fields[new_code] = unique_vals[0]
        else:
            if operator_val is not None:
                update_fields[f"{new_code} 1"] = operator_val
            if tr_val is not None:
                update_fields[f"{new_code} 2"] = tr_val
            if ac_val is not None:
                update_fields[f"{new_code} 3"] = ac_val

        bulk_updates.append(
            UpdateOne(
                {"account_id": account_id},
                {"$set": update_fields}
            )
        )

        count += 1

        if count % 2000 == 0:
            Accounts_L1.bulk_write(bulk_updates, ordered=False)
            bulk_updates = []
            print(f"update {count} records")

    if bulk_updates:
        Accounts_L1.bulk_write(bulk_updates, ordered=False)


def update_account_type_L1(collection, target_code, batch_size=1000):
    bulk_updates = []
    count = 0
    for record in collection.find({}, {"account_id": 1, target_code: 1}):
        acc_id = record.get("account_id")
        acc_type = record.get(target_code, "")

        if acc_id:
            bulk_updates.append(
                UpdateOne(
                    {"account_id": acc_id},
                    {"$set": {target_code: acc_type}},
                    upsert=False
                )
            )

        if len(bulk_updates) >= batch_size:
            Accounts_L1.bulk_write(bulk_updates, ordered=False)
            count += len(bulk_updates)
            bulk_updates = []
            print(f"update {count} {target_code}")
    if bulk_updates:
        Accounts_L1.bulk_write(bulk_updates, ordered=False)
        count += len(bulk_updates)

    print(f"done writing {target_code} ,{count} records in total")

    result = Accounts_L1.update_many(
        {target_code: {"$exists": False}},
        {"$set": {target_code: ""}}
    )
    print(f"update {target_code} as Null.")

def update_transaction_account_types_L1(batch_size=2000):
    tr_map = {}
    ac_map = {}

    cursor = transaction.find({}, {
        "tr_account_id": 1,
        "ac_account_id": 1,
        "TRANSFERRING_ACCOUNT_TYPE": 1,
        "TRANSFERRING_ACCOUNT_TYPE2": 1,
        "ACQUIRING_ACCOUNT_TYPE": 1,
        "ACQUIRING_ACCOUNT_ACCOUNT_TYPE2": 1
    })

    for t in cursor:
        tr_id = t.get("tr_account_id")
        ac_id = t.get("ac_account_id")

        if tr_id:
            tr_map.setdefault(tr_id, []).append((
                t.get("TRANSFERRING_ACCOUNT_TYPE"),
                t.get("TRANSFERRING_ACCOUNT_TYPE2")
            ))
        if ac_id:
            ac_map.setdefault(ac_id, []).append((
                t.get("ACQUIRING_ACCOUNT_TYPE"),
                t.get("ACQUIRING_ACCOUNT_ACCOUNT_TYPE2")
            ))

    bulk_updates = []
    count = 0
    total = Accounts_L1.count_documents({})
    cursor = Accounts_L1.find({}, {"account_id": 1})

    for record in cursor:
        acc_id = record.get("account_id")
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
            {"account_id": acc_id},
            {"$set": {
                "TRANSACTION_ACCOUNT_TYPE": final_type,
                "TRANSACTION_ACCOUNT_TYPE2": final_type2
            }}
        ))

        if len(bulk_updates) >= batch_size:
            Accounts_L1.bulk_write(bulk_updates, ordered=False)
            count += len(bulk_updates)
            print(f"update {count} records")
            bulk_updates = []

    if bulk_updates:
        Accounts_L1.bulk_write(bulk_updates, ordered=False)
        count += len(bulk_updates)

    print("all done")

    Accounts_L1.update_many(
        {"TRANSACTION_ACCOUNT_TYPE": {"$exists": False}},
        {"$set": {"TRANSACTION_ACCOUNT_TYPE": ""}}
    )
    Accounts_L1.update_many(
        {"TRANSACTION_ACCOUNT_TYPE2": {"$exists": False}},
        {"$set": {"TRANSACTION_ACCOUNT_TYPE2": ""}}
    )
    print("all done")