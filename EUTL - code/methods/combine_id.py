from pymongo import UpdateOne


def combine_id(collection, code, name, output):
    cursor = collection.find({},
                             {code: 1, name: 1})
    bulk_updates = []
    count = 0
    for doc in cursor:
        update_fields = {}
        code_id = doc.get(code, '')
        name_id = doc.get(name, '')
        update_fields[output] = f"{code_id}_{name_id}"
        if update_fields:
            bulk_updates.append(UpdateOne({"_id": doc["_id"]}, {"$set": update_fields}))
            count += 1

        if len(bulk_updates) >= 1000:
            collection.bulk_write(bulk_updates)
            bulk_updates = []
    if bulk_updates:
        collection.bulk_write(bulk_updates)

    print(f"update {count} records.")