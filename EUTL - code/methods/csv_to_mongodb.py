import pandas as pd
from pymongo import MongoClient


def csv_to_mongodb(csv_path: str, db_name: str, collection_name: str, mongo_uri: str = "mongodb://localhost:27017/"):
    df = pd.read_csv(csv_path, dtype=str)
    df = df.fillna('')

    for col in df.columns:
        non_empty_values = df[col][df[col] != '']
        if len(non_empty_values) > 0:
            try:
                df[col] = df[col].apply(lambda x: int(float(x)) if x != '' else '')
            except ValueError:
                df[col] = df[col].astype(str)
        else:
            df[col] = df[col].astype(str)

    records = df.to_dict('records')
    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[collection_name]
    if len(records) > 0:
        collection.insert_many(records)
        print(f"wrote {len(records)} to {db_name}.{collection_name}")
    else:
        print("No records")