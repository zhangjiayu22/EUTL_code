from methods.csv_to_mongodb import csv_to_mongodb
from methods.export_registry_dict import export_registry_dict
from connection import operators_yearly_activity_daily, operators_daily, accounts_daily, transaction, db
from methods.update_transaction_registry_codes import update_transaction_registry_codes
from methods.transaction_combine_id import transaction_combine_id
from methods.combine_id import combine_id

tasks = [
    ("transactions_EUTL_PUBLIC_ESD_20251031.csv", "transaction"),
    ("transactions_EUTL_PUBLIC_NOTESD_20251031.csv", "transaction"),
    ("accounts_daily.csv", "accounts_daily"),
    ("operators_daily.csv", "operators_daily"),
    ("operators_yearly_activity_daily.csv", "operators_yearly_activity_daily"),
]

for csv_file, collection in tasks:
    print(f"from {csv_file} export to {collection}")
    csv_to_mongodb(csv_path=csv_file, db_name="EUTL-1118", collection_name=collection)
print('完成数据导入')

export_registry_dict(accounts_daily, "accounts_iso2_code.csv", "REGISTRY_NAME", "REGISTRY_CODE")
export_registry_dict(operators_daily, "operators_iso2_code.csv", "REGISTRY_NAME", "REGISTRY_CODE")

update_transaction_registry_codes()

result = transaction.update_many(
    {"TRANSFERRING_REGISTRY_CODE": {"$exists": False}},
    {"$set": {"TRANSFERRING_REGISTRY_CODE": 99}}
)

result2 = transaction.update_many(
    {"TRANSFERRING_REGISTRY_NAME": 'CDM'},
    {"$set": {"TRANSFERRING_REGISTRY_CODE": 'CDM'}}
)

transaction_combine_id()

combine_id(operators_yearly_activity_daily, 'REGISTRY_CODE', 'INSTALLATION_IDENTIFIER', 'installation_id')
combine_id(operators_daily, 'REGISTRY_CODE', 'INSTALLATION_IDENTIFIER', 'installation_id')
combine_id(operators_daily, 'ACCOUNT_REGISTRY_CODE', 'ACCOUNT_IDENTIFIER', 'account_id')
combine_id(accounts_daily, 'REGISTRY_CODE', 'ACCOUNT_IDENTIFIER', 'account_id')
