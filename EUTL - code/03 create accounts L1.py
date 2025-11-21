from methods.construct_L0 import construct_ids_l0
from connection import transaction, operators_daily, accounts_daily
from methods.ac_ins_mapping import ac_ins_mapping
from methods.update_account_L1 import update_account_L1, update_account_type_L1, update_transaction_account_types_L1

account_field_pairs = [
    (operators_daily, 'account_id'),
    (accounts_daily, 'account_id'),
    (transaction, 'tr_account_id'),
    (transaction, 'ac_account_id')
]
construct_ids_l0(account_field_pairs, 'Accounts_L1', 'account_id')
ac_ins_mapping()

update_account_L1(operators_daily, 'installation_id', 'tr_installation_id', 'ac_installation_id',
                  'installation_id')
update_account_L1(accounts_daily, 'ACCOUNT_NAME', 'TRANSFERRING_ACCOUNT_NAME', 'ACQUIRING_ACCOUNT_NAME',
                  'ACCOUNT_NAME')
update_account_L1(operators_daily, 'ACCOUNT_HOLDER_NAME', 'TRANSFERRING_ACCOUNT_HOLDER', 'ACQUIRING_ACCOUNT_HOLDER',
                  'ACCOUNT_HOLDER')
update_account_L1(operators_daily, 'ACCOUNT_HOLDER_CITY', 'TRANSFERRING_ACCOUNT_HOLDER_CITY',
                  'ACQUIRING_ACCOUNT_HOLDER_CITY',
                  'ACCOUNT_HOLDER_CITY')
update_account_L1(operators_daily, 'ACCOUNT_HOLDER_ADDRESS1', 'TRANSFERRING_ACCOUNT_HOLDER_ADDRESS1',
                  'ACQUIRING_ACCOUNT_HOLDER_ADDRESS1',
                  'ACCOUNT_HOLDER_ADDRESS1')

update_account_L1(operators_daily, 'ACCOUNT_HOLDER_ADDRESS2', 'TRANSFERRING_ACCOUNT_HOLDER_ADDRESS2',
                  'ACQUIRING_ACCOUNT_HOLDER_ADDRESS2',
                  'ACCOUNT_HOLDER_ADDRESS2')

update_account_L1(accounts_daily, 'OPEN_DATE', 'TRANSFERRING_ACCOUNT_OPEN_DT',
                  'ACQUIRING_ACCOUNT_OPEN_DT',
                  'OPEN_DATE')
update_account_L1(accounts_daily, 'END_OF_VALIDITY_DATE', 'TRANSFERRING_ACCOUNT_END_OF_VALIDITY',
                  'ACQUIRING_ACCOUNT_END_OF_VALIDITY',
                  'END_OF_VALIDITY_DATE')
update_account_L1(operators_daily, 'ACCOUNT_HOLDER_COUNTRY_CODE', 'TRANSFERRING_ACCOUNT_HOLDER_COUNTRY_CODE',
                  'ACQUIRING_ACCOUNT_HOLDER_COUNTRY_CODE',
                  'ACCOUNT_HOLDER_COUNTRY_CODE')
update_account_L1(operators_daily, 'ACCOUNT_HOLDER_LEI', 'TRANSFERRING_ACCOUNT_HOLDER_LEI',
                  'ACQUIRING_ACCOUNT_HOLDER_LEI',
                  'ACCOUNT_HOLDER_LEI')
update_account_L1(operators_daily, 'ACCOUNT_HOLDER_COMPANY_REGISTRATION_NUMBER',
                  'TRANSFERRING_ACCOUNT_HOLDER_COMPANY_REGISTRATION_NUMBER',
                  'ACQUIRING_ACCOUNT_HOLDER_COMPANY_REGISTRATION_NUMBER',
                  'ACCOUNT_HOLDER_COMPANY_REGISTRATION_NUMBER')
update_account_L1(operators_daily, 'ACCOUNT_HOLDER_COMPANY_REGISTRATION_NUMBER',
                  'TRANSFERRING_ACCOUNT_HOLDER_COMPANY_REGISTRATION_NUMBER',
                  'ACQUIRING_ACCOUNT_HOLDER_COMPANY_REGISTRATION_NUMBER',
                  'ACCOUNT_HOLDER_COMPANY_REGISTRATION_NUMBER')

update_list = ['ACCOUNT_TYPE',
               'ETS_ACCOUNT_TYPE',
               'FULL_TYPE',
               'ACCOUNT_IDENTIFIER_IN_REG']
for i in update_list:
    update_account_type_L1(accounts_daily, i)

update_transaction_account_types_L1()
