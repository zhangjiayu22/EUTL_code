import math

from pymongo import InsertOne

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


def aggregate_transaction():
    unit_type_map = {
        "AAU - Assigned Amount Unit Allowance issued for the 2008-2012 period and subsequent 5-year periods and is converted from an AAU 1": "AAU2",
        "AAU - Assigned Amount Unit  1": "AAU2",
        "AAU - Assigned Amount Unit No supplementary unit type 1": "AAU3",
        "AAU - Assigned Amount Unit No supplementary unit type 2": "AAU3",
        "CER - Certified Emission Reduction Unit converted from an AAU No supplementary unit type 1": "CER",
        "CER - Certified Emission Reduction Unit converted from an AAU  1": "CER",
        "CER - Certified Emission Reduction Unit converted from an AAU  2": "CER",
        "CER - Certified Emission Reduction Unit converted from an AAU No supplementary unit type 2": "CER",
        "ERU - Converted from an RMU No supplementary unit type 1": "ERUR",
        "ERU - Converted from an RMU  1": "ERUR",
        "ERU - Emission Reduction Unit  1": "ERU",
        "ERU - Emission Reduction Unit No supplementary unit type 1": "ERU",
        "Non-Kyoto Unit Allowance issued for the 2005-2007 period and not converted from an AAU or other Kyoto unit 0": "NKU1",
        "Non-Kyoto Unit Allowance issued for the 2008 to 2012 and subsequent five-year periods by a Member State that does not have AAUs 1": "NKU2",
        "Non-Kyoto Unit Annual Emission Allocation Unit 2": "NKU3",
        "Non-Kyoto Unit EU Aviation Allowances (EUAA) 2": "EUAA3",
        "Non-Kyoto Unit EU Aviation Allowances (EUAA) 1": "EUAA2",
        "Non-Kyoto Unit EU Aviation Allowances (EUAA) 3": "EUAA4",
        "Non-Kyoto Unit EU General Allowances (EUA) 1": "EUA2",
        "Non-Kyoto Unit EU General Allowances (EUA) 2": "EUA3",
        "Non-Kyoto Unit EU General Allowances (EUA) 3": "EUA4",
        "Non-Kyoto Unit Swiss Aviation Allowances (CHUA) 3": "CHUA",
        "Non-Kyoto Unit Swiss Aviation Allowances (CHUA) 2": "CHUA",
        "Non-Kyoto Unit Swiss General Allowances (CHU) 2": "CHU",
        "tCER - Temporary CER  2": "TCER",
        "tCER - Temporary CER  1": "TCER",
        "tCER - Temporary CER No supplementary unit type 1": "TCER",
        "RMU - Removal Unit  1": "RMU",
        "RMU - Removal Unit No supplementary unit type 1": "RMU"
    }

    keep_fields = [
        'tr_account_id',
        'ac_account_id',
        'TRANSACTION_ID',
        'TRANSACTION_TYPE',
        'TRANSACTION_DATE',
    ]

    output_collection = db["Transaction_L2"]
    output_collection.delete_many({})  # 清空旧数据

    pipeline = [
        {"$group": {
            "_id": "$TRANSACTION_ID",
            "records": {"$push": "$$ROOT"},
            "total_amount": {"$sum": "$AMOUNT"}
        }}
    ]

    cursor = transaction.aggregate(pipeline, allowDiskUse=True)

    requests = []
    conflict_fields = set()
    all_unit_types = sorted(set(unit_type_map.values()) | {"NONE"})

    for doc in cursor:
        tid = doc["_id"]
        records = doc["records"]
        result = {"TRANSACTION_ID": tid}
        for f in keep_fields:
            vals = {r.get(f) for r in records if r.get(f) is not None}
            if len(vals) > 1:
                conflict_fields.add(f)
            result[f] = vals.pop() if vals else None
        for ut in all_unit_types:
            result[ut] = ''
        unit_sums = {}
        for r in records:
            desc = f"{r.get('UNIT_TYPE_DESCRIPTION', '')} {r.get('SUPP_UNIT_TYPE_DESCRIPTION', '')} {r.get('ORIGINAL_PERIOD_CODE', '')}".strip()
            unit_code = unit_type_map.get(desc, "NONE")

            amount = r.get("AMOUNT", 0)
            if not isinstance(amount, (int, float)) or math.isnan(amount):
                amount = 0

            unit_sums[unit_code] = unit_sums.get(unit_code, 0) + amount
        for ut in all_unit_types:
            amt = unit_sums.get(ut, 0)
            result[ut] = amt if amt != 0 else 0

        result["total_amount"] = doc["total_amount"]

        requests.append(InsertOne(result))

    if requests:
        output_collection.bulk_write(requests)
        print(f"wrote {len(requests)} to Transaction_L2")
    else:
        print("no results")

    if conflict_fields:
        print(f"conflicts：{list(conflict_fields)}")
    else:
        print("No conflicts")
