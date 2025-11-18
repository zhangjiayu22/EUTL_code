from methods.construct_L0 import construct_l0, construct_ids_l0
from connection import transaction, operators_daily, operators_yearly_activity_daily, accounts_daily
from methods.ac_ins_mapping import ac_ins_mapping

construct_l0(
    source_collection=transaction,
    target_collection='Transaction_L0',
    fields=['TRANSACTION_ID', 'tr_account_id', 'ac_account_id']
)

construct_l0(
    source_collection=operators_yearly_activity_daily,
    target_collection='Compliance_L0',
    fields=['installation_id']
)

account_field_pairs = [
    (operators_daily, 'account_id'),
    (accounts_daily, 'account_id'),
    (transaction, 'tr_account_id'),
    (transaction, 'ac_account_id')
]
construct_ids_l0(account_field_pairs, 'Accounts_L0', 'account_id')
ac_ins_mapping()

installation_field_pairs = [
    (operators_daily, 'installation_id'),
    (transaction, 'tr_installation_id'),
    (transaction, 'ac_installation_id')
]
construct_ids_l0(
    installation_field_pairs,
    'Installation_L0',
    'installation_id'
)
