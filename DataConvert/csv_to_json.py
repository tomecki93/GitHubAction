import csv, json, os

csvFilePath = 'D:\\DataProject\\favorita-grocery-sales-forecasting\\stores_csv\\stores.csv'
jsonFilePath = 'stores.json'
cwd = os.getcwd()

# read csv file and add to set data
# data = []
# with open(csvFilePath) as csvFile:
#     csvReader = csv.DictReader(csvFile)
#     with open(jsonFilePath, 'w') as jsonFile:
#         for rows in csvReader:
#             data = rows
#             jsonFile.write(json.dumps(data, indent=4))

# Function to convert a CSV to JSON
# Takes the file paths as arguments
def make_json(csvFilePath, jsonFilePath, primaryKey):
    # create a dictionary
    data = {}

    # Open a csv reader called DictReader
    with open(csvFilePath, encoding='utf-8') as csvf:
        csvReader = csv.DictReader(csvf)

        # Convert each row into a dictionary
        # and add it to data
        for rows in csvReader:
            # Assuming a column named 'No' to
            # be the primary key
            key = rows[primaryKey]
            data[key] = rows

    # Open a json writer, and use the json.dumps()
    # function to dump data
    with open(jsonFilePath, 'w', encoding='utf-8') as jsonf:
        jsonf.write(json.dumps(data, indent=4))


# Driver Code

# Decide the two file paths according to your
# computer system
# csvFilePath = r'Names.csv'
# jsonFilePath = r'Names.json'

# Call the make_json function
make_json(csvFilePath, jsonFilePath, 'store_nbr')

