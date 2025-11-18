import csv


def export_registry_dict(collection, output_csv: str, name, code):
    cursor = collection.find({}, {name: 1, code: 1, "_id": 0})

    registry_dict = {}
    for doc in cursor:
        name = doc.get("REGISTRY_NAME")
        code = doc.get("REGISTRY_CODE")
        if name and code and name not in registry_dict:
            registry_dict[name] = code

    with open(output_csv, "w", newline='', encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["REGISTRY_NAME", "REGISTRY_CODE"])
        for name, code in registry_dict.items():
            writer.writerow([name, code])

    print(f"wrote {len(registry_dict)} to {output_csv}")