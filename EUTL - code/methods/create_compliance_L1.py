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

def create_compliance_L1():
    keep_fields = [
        'PERIOD_YEAR',
        'VERIFIED_EMISSIONS',
        'CH_VERIFIED_EMISSIONS',
        'ALLOCATION',
        'ALLOCATION_RES',
        'ALLOCATION_TRA',
        'CH_ALLOCATION',
        'EXCLUDED',
        'CH_EXCLUDED',
        'SURR_EUA',
        'SURR_EUAA',
        'SURR_CHU',
        'SURR_CHUA',
        'SURR_ERU_FROM_AAU',
        'SURR_FORMER_EUA',
        'SURR_CER',
        'SURR_ALLOWANCE_CP0',
        'SURR_ALL',
        'installation_id'
    ]

    output_collection = db["Compliance_L1"]
    output_collection.delete_many({})

    projection = {field: 1 for field in keep_fields}

    cursor = operators_yearly_activity_daily.find({}, projection)

    docs = list(cursor)
    if docs:
        output_collection.insert_many(docs)
        print(f"wrote {len(docs)} to Compliance_L2")
    else:
        print("No results")
