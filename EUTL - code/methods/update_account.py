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


def update_account(collection, target_code, target_code_tr, target_code_ac, new_code):
    operator_map = {}
    for doc in collection.find(
            {"account_id": {"$ne": None}},
            {"account_id": 1, target_code: 1, "_id": 0}
    ):
        if doc.get(target_code):
            operator_map[doc["account_id"]] = doc[target_code]

    print(f"{collection} done loading")

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

    print(f"transaction done loading")

    account_cursor = Accounts_L2.find({}, {"account_id": 1, "_id": 0})
    bulk_updates = []
    count = 0

    for acc in account_cursor:
        account_id = acc.get("account_id")
        operator_val = operator_map.get(account_id)
        tr_val = tr_map.get(account_id)
        ac_val = ac_map.get(account_id)
        if operator_val:
            final_val = operator_val
        elif tr_val and ac_val:
            final_val = tr_val if tr_val == ac_val else tr_val
        elif tr_val:
            final_val = tr_val
        elif ac_val:
            final_val = ac_val
        else:
            final_val = None

        if final_val is not None:
            bulk_updates.append(
                UpdateOne(
                    {"account_id": account_id},
                    {"$set": {new_code: final_val}}
                )
            )

        if len(bulk_updates) >= 1000:
            Accounts_L2.bulk_write(bulk_updates, ordered=False)
            bulk_updates = []
            count += 1000
            print(f"updated {count} records")

    if bulk_updates:
        Accounts_L2.bulk_write(bulk_updates, ordered=False)
        count += len(bulk_updates)

    print(f"all {new_code} done writing")
    result = Accounts_L2.update_many(
        {new_code: {"$exists": False}},
        {"$set": {new_code: ""}}
    )
    print(f"update {new_code} as Null.")
