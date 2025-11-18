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
def ac_ins_mapping():
    mapping = {}

    cursor_tr = transaction.find(
        {},
        {
            "tr_account_id": 1,
            "tr_installation_id": 1,
            "ac_account_id": 1,
            "ac_installation_id": 1,
            "_id": 0
        }
    )

    for doc in cursor_tr:
        tr_acc = doc.get("tr_account_id")
        tr_inst = doc.get("tr_installation_id")

        ac_acc = doc.get("ac_account_id")
        ac_inst = doc.get("ac_installation_id")

        if tr_acc:
            mapping.setdefault(tr_acc, set())
            if tr_inst:
                mapping[tr_acc].add(str(tr_inst))

        if ac_acc:
            mapping.setdefault(ac_acc, set())
            if ac_inst:
                mapping[ac_acc].add(str(ac_inst))

    cursor_ops = operators_daily.find(
        {},
        {"account_id": 1, "installation_id": 1, "_id": 0}
    )

    for doc in cursor_ops:
        acc = doc["account_id"]
        inst = doc.get("installation_id")

        mapping.setdefault(acc, set())
        if inst:
            mapping[acc].add(str(inst))
    print("account_id → installation_id done")

    cursor_accounts = Accounts_L0.find({}, {"account_id": 1})

    for doc in cursor_accounts:
        acc_id = doc["account_id"]

        inst_set = mapping.get(acc_id, set())

        if len(inst_set) == 1:
            final_inst = list(inst_set)[0]

        elif len(inst_set) > 1:
            print(f"conflicts: account_id={acc_id}, installation_id={inst_set}")
            continue

        else:
            final_inst = ""

        Accounts_L0.update_one(
            {"account_id": acc_id},
            {"$set": {"installation_id": final_inst}}
        )

    print("all done")
