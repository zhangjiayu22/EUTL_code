def insert_ids(collection_field_pairs, target_collection, field_name):
    all_ids = set()

    for collection, field in collection_field_pairs:
        ids = collection.distinct(field)
        all_ids.update(ids)

    documents = [{field_name: value} for value in all_ids if value is not None]

    if documents:
        result = target_collection.insert_many(documents)
        print(f"insert {len(result.inserted_ids)} to {target_collection.name}")
        return result
    else:
        print("No results")
        return None
