from pymongo import UpdateOne
import pandas as pd
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


def update_transaction_registry_codes():
    accounts_df = pd.read_csv("accounts_iso2_code.csv")
    operators_df = pd.read_csv("operators_iso2_code.csv")
    accounts_map = dict(zip(accounts_df["REGISTRY_NAME"], accounts_df["REGISTRY_CODE"]))
    operators_map = dict(zip(operators_df["REGISTRY_NAME"], operators_df["REGISTRY_CODE"]))
    cursor = transaction.find(
        {},
        {
            "_id": 1,
            "TRANSFERRING_REGISTRY_NAME": 1,
            "ACQUIRING_REGISTRY_NAME": 1
        }
    )

    bulk_updates = []
    count = 0

    for doc in cursor:
        trans_name = (doc.get("TRANSFERRING_REGISTRY_NAME", "")).strip()
        acqu_name = (doc.get("ACQUIRING_REGISTRY_NAME", "")).strip()

        trans_acc_code = accounts_map.get(trans_name, "")
        trans_op_code = operators_map.get(trans_name, "")
        acqu_acc_code = accounts_map.get(acqu_name, "")
        acqu_op_code = operators_map.get(acqu_name, "")
        update_fields = {}
        if trans_acc_code:
            update_fields["TRANSFERRING_REGISTRY_CODE"] = trans_acc_code
        if trans_op_code:
            update_fields["TRANSFERRING_INSTALLATION_REGISTRY_CODE"] = trans_op_code
        if acqu_acc_code:
            update_fields["ACQUIRING_REGISTRY_CODE"] = acqu_acc_code
        if acqu_op_code:
            update_fields["ACQUIRING_INSTALLATION_REGISTRY_CODE"] = acqu_op_code

        if update_fields:
            bulk_updates.append(UpdateOne({"_id": doc["_id"]}, {"$set": update_fields}))
            count += 1

        if len(bulk_updates) >= 1000:
            transaction.bulk_write(bulk_updates)
            bulk_updates = []

    if bulk_updates:
        transaction.bulk_write(bulk_updates)

    print(f"update {count} to transaction")
