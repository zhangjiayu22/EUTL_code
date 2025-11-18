from connection import transaction
from methods.create_transaction_L1 import create_transaction_L1

create_transaction_L1(
    source_collection=transaction,
    target_collection='Transaction_L1',
    fields=[
        "TRANSACTION_ID",
        "TRANSACTION_TYPE",
        "TRANSACTION_DATE",
        "TRANSACTION_STATUS",
        "ORIGINATING_REGISTRY",
        "UNIT_TYPE_DESCRIPTION",
        "SUPP_UNIT_TYPE_DESCRIPTION",
        "ORIGINAL_PERIOD_CODE",
        "LULUCF_CODE_DESCRIPTION",
        "PROJECT_IDENTIFIER",
        "TRACK",
        "EXPIRY_DATE",
        "AMOUNT",
        "tr_account_id",
        "ac_account_id"
    ]
)
