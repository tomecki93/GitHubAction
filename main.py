# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import uuid

def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')
    sf_params_dwh_store_stage = {
        "sfURL": "sqa68179.snowflakecomputing.com",
        "sfUser": "TOMECKI1993",
        "sfPassword": "Abis1993",
        "sfDatabase": "STORE_DB",
        "sfSchema": "STAGE",
        "sfWarehouse": "DWH_STORES"
    }
    # print(sf_params_dwh_store_stage['sfURL'].index('.'))
    # print(sf_params_dwh_store_stage['sfURL'][:sf_params_dwh_store_stage['sfURL'].index('.')])
    print(uuid.uuid4())

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
