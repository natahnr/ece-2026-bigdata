import csv
import json

class CsvBuilder:
    def __init__(self):
        self.data = []

    def write(self, row):
        self.data.append(row)

def serialize(dataset, output):
    if output == "":
        return dataset
    elif output == "json":
        dataset = json.dumps(dataset, default=str)
        print(dataset)
    elif output == "jsonline":
        for record in dataset:
            print(json.dumps(record, default=str))
    elif output == "csv":
        builder = CsvBuilder()
        writer = csv.writer(builder)
        writer.writerow(dataset[0].keys())
        for record in dataset:
            writer.writerow(record.values())
        print("".join(builder.data))
    else:
        raise ValueError("Unsupported output format.")