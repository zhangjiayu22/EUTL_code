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


def transaction_combine_id():
    cursor = transaction.find(
        {},
        {
            "_id": 1,
            "TRANSFERRING_REGISTRY_CODE": 1,
            "TRANSFERRING_ACCOUNT_IDENTIFIER": 1,
            "ACQUIRING_REGISTRY_CODE": 1,
            "ACQUIRING_ACCOUNT_IDENTIFIER": 1,
            "TRANSFERRING_INSTALLATION_REGISTRY_CODE": 1,
            "TRANSFERRING_INSTALLATION_INSTALLATION_IDENTIFIER": 1,
            "ACQUIRING_INSTALLATION_REGISTRY_CODE": 1,
            "ACQUIRING_INSTALLATION_INSTALLATION_IDENTIFIER": 1,
        },
    )

    bulk_updates = []
    count = 0

    for doc in cursor:
        update_fields = {}

        tr_reg = doc.get("TRANSFERRING_REGISTRY_CODE", "")
        tr_acc = doc.get("TRANSFERRING_ACCOUNT_IDENTIFIER", "")
        ac_reg = doc.get("ACQUIRING_REGISTRY_CODE", "")
        ac_acc = doc.get("ACQUIRING_ACCOUNT_IDENTIFIER", "")
        update_fields["tr_account_id"] = f"{tr_reg}_{tr_acc}"
        update_fields["ac_account_id"] = f"{ac_reg}_{ac_acc}"

        tr_inst_reg = doc.get("TRANSFERRING_INSTALLATION_REGISTRY_CODE", "")
        tr_inst_id = doc.get("TRANSFERRING_INSTALLATION_INSTALLATION_IDENTIFIER", "")
        ac_inst_reg = doc.get("ACQUIRING_INSTALLATION_REGISTRY_CODE", "")
        ac_inst_id = doc.get("ACQUIRING_INSTALLATION_INSTALLATION_IDENTIFIER", "")

        if tr_inst_id != "":
            update_fields["tr_installation_id"] = f"{tr_inst_reg}_{tr_inst_id}"
        if ac_inst_id != "":
            update_fields["ac_installation_id"] = f"{ac_inst_reg}_{ac_inst_id}"

        if update_fields:
            bulk_updates.append(UpdateOne({"_id": doc["_id"]}, {"$set": update_fields}))
            count += 1

        if len(bulk_updates) >= 1000:
            transaction.bulk_write(bulk_updates)
            bulk_updates = []

    if bulk_updates:
        transaction.bulk_write(bulk_updates)

    print(f"update {count} records, done combining IDs in transaction")
