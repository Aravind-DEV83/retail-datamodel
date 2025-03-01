import json, gzip
import csv
from datetime import datetime


def flatten_json(item):

    final = {}

    for key, value in item.items():
        if isinstance(value, dict):
            if "$oid" in value:
                final[key] = value["$oid"]
            elif "$ref" in value:
                final[key + "_ref"] = value["$ref"]
                final[key + "_id"] = value.get("$id", {}).get("$oid") 
            else:
                for nested_key, nested_value in value.items():
                    final[f"{key}_{nested_key}"] = nested_value
        else:
            final[key] = value
    return final

def processData():

    input_directory = '/Users/aravind_jarpala/Downloads/Data Engineering/analytics/retail-datamodel/data'

    with gzip.open(input_directory + '/' + 'brands.json.gz', 'rt') as brands_file:
        brandItems = [json.loads(line) for line in brands_file]


    with open(input_directory + '/output/' + 'brands.csv', 'w') as output_file:
        csv_writer = csv.writer(output_file)
        # count = 0
        flattened_data = [flatten_json(item) for item in brandItems]
        # print(brandItems)
        all_keys = set()
        for dictionary in brandItems:
            all_keys.update(dictionary.keys())
        header = sorted(list(all_keys))
        csv_writer.writerow(header)


        for item in flattened_data:
            row = [item.get(key, None) for key in header]
            csv_writer.writerow(row)

processData()