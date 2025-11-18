from methods.construct_L0 import construct_ids_l0
from connection import transaction, operators_daily, Installation_L1
from methods.update_installation_L1 import update_installation_L1, update_installation_code_L1, \
    update_installation_company_L1, update_main_activity_L1

installation_field_pairs = [
    (operators_daily, 'installation_id'),
    (transaction, 'tr_installation_id'),
    (transaction, 'ac_installation_id')
]
construct_ids_l0(
    installation_field_pairs,
    'Installation_L1',
    'installation_id'
)

update_installation_L1(operators_daily, 'INSTALLATION_NAME', 'TRANSFERRING_INSTALLATION_NAME',
                       'ACQUIRING_INSTALLATION_NAME',
                       'INSTALLATION_NAME')
update_installation_L1(operators_daily, 'CITY', 'TRANSFERRING_INSTALLATION_CITY', 'ACQUIRING_INSTALLATION_CITY',
                       'CITY')
update_installation_L1(operators_daily, 'POSTAL_CODE', 'TRANSFERRING_INSTALLATION_POSTAL_CODE',
                       'ACQUIRING_INSTALLATION_POSTAL_CODE',
                       'POSTAL_CODE')
update_installation_L1(operators_daily, 'ADDRESS1', 'TRANSFERRING_INSTALLATION_ADDRESS1',
                       'ACQUIRING_INSTALLATION_ADDRESS1',
                       'ADDRESS1')
update_installation_L1(operators_daily, 'ADDRESS2', 'TRANSFERRING_INSTALLATION_ADDRESS2',
                       'ACQUIRING_INSTALLATION_ADDRESS2',
                       'ADDRESS2')
update_installation_L1(operators_daily, 'PERMIT_IDENTIFIER', 'TRANSFERRING_INSTALLATION_PERMIT_IDENTIFIER',
                       'ACQUIRING_INSTALLATION_PERMIT_IDENTIFIER',
                       'PERMIT_IDENTIFIER')
update_installation_L1(operators_daily, 'EPER_IDENTIFICATION', 'TRANSFERRING_INSTALLATION_EPER_IDENTIFICATION',
                       'ACQUIRING_INSTALLATION_EPER_IDENTIFICATION',
                       'EPER_IDENTIFICATION')

update_list = [
    'ACTIVITY_TYPE_CODE',
    'ACTIVITY_TYPE',
    'YEAR_OF_FIRST_EMISSIONS',
    'YEAR_OF_LAST_EMISSIONS'
]
for i in update_list:
    update_installation_code_L1(operators_daily, i)

update_installation_company_L1()

update_main_activity_L1()
