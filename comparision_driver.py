from source_to_dataframe import SrcToDataframe as sd
from user_input import UserInput as uip
from data_comparision import DataComparision as dc
import sys
import time

'''Source 1 details'''
print("Select the Source 1 type")
source_1_type = uip.source_type()
source_1_file_path, db_type_1 = uip.source_location(source_1_type)
print("User given source path is: ", source_1_file_path)

'''Loading the source as dataframe'''
if source_1_type == 1:
    print("Started reading source csv file.,")
    source = sd.load_csv(source_1_file_path)
elif source_1_type == 2:
    if db_type_1 == 1:
        source = sd.load_sqlserver_data(source_1_file_path)
    elif db_type_1 == 2:
        source = sd.load_mysql_data(source_1_file_path)
    elif db_type_1 == 3:
        source = sd.load_msaccess_data(source_1_file_path)
    elif db_type_1 == 4:
        source = sd.load_netezza_data(source_1_file_path)
    elif db_type_1 == 5:
        source = sd.load_oracle_data(source_1_file_path)
elif source_1_type == 3:
    json_type = int(input("Press 1 for non-nested json, 2 if json is nested: "))
    if json_type == 1:
        source = sd.load_json(source_1_file_path)
    elif json_type == 2:
        source = sd.load_json_normalize(source_1_file_path)
elif source_1_type == 4:
    xml_type = int(input("Press 1 for non-nested xml, 2 if xml is nested: "))
    if xml_type == 1:
        print("Please customize the code in xml parsing file and call the function")
        time.sleep(30)
        sys.exit()
    elif xml_type == 2:
        print("Please customize the code in xml parsing file and call the function")
        time.sleep(30)
        sys.exit()
else:
    print("Incorrect option")
    time.sleep(30)
    sys.exit()

print(source.head())

'''Source 2 details'''
print("Select the Source 2 type")
source_2_type = uip.source_type()
source_2_file_path, db_type_2 = uip.source_location(source_1_type)
print("User given source path is: ", source_2_file_path)

'''Loading the target as dataframe'''
if source_2_type == 1:
    print("Started reading source csv file.,")
    target = sd.load_csv(source_2_file_path)
elif source_2_type == 2:
    if db_type_2 == 1:
        target = sd.load_sqlserver_data(source_2_file_path)
    elif db_type_2 == 2:
        target = sd.load_mysql_data(source_2_file_path)
    elif db_type_2 == 3:
        target = sd.load_msaccess_data(source_2_file_path)
    elif db_type_2 == 4:
        target = sd.load_netezza_data(source_2_file_path)
    elif db_type_2 == 5:
        target = sd.load_oracle_data(source_2_file_path)
elif source_2_type == 3:
    json_type = int(input("Press 1 for non-nested json, 2 if json is nested: "))
    if json_type == 1:
        target = sd.load_json(source_2_file_path)
    elif json_type == 2:
        target = sd.load_json_normalize(source_2_file_path)
elif source_2_type == 4:
    xml_type = int(input("Press 1 for non-nested xml, 2 if xml is nested: "))
    if xml_type == 1:
        print("Please customize the code in xml parsing file and call the function")
        time.sleep(30)
        sys.exit()
    elif xml_type == 2:
        print("Please customize the code in xml parsing file and call the function")
        time.sleep(30)
        sys.exit()
else:
    print("Incorrect option")
    sys.exit()

print(target.head())

"""Data comparision"""
print("Datatype check in progress.,")
dc.datatype_check(source, target)
print("Datatype check completed.,")
print("Number of columns check in progress")
dc.length_check(source, target)
print("Compared number of columns")
primary_key = uip.set_primary_key()
output_path = uip.create_output_path()
dc.comparision_by_index(source, target, primary_key, output_path)

#'F:\xxxxxx\xxxxx\Tool development\\ExtInv.csv'
#'F:\xxxxxx\xxxxx\Tool development\\InttInv.csv'

"""If you would like to do some transformations before comparing, use below functions for reference"""
#source = source.round({'col1': 1})
#source.dropna(axis=0, how='any', thresh=1, subset=['col1', 'col2'], inplace=True)
#source.drop(axis=1, columns=['col1', 'col2'], inplace=True)
#source.drop_duplicates(subset=None, keep="first", inplace=True)
#source.fillna(value = {'col1':0,'col2':1}, inplace=True)
#source = source.infer_objects() #to assign the datatype based on the value
#source['col1'] = source['col1'].astype(int) #converting as integer int can be replaced with str, float16, complex etc
#source = source.groupby(['col1', 'col2']).agg({'col6': 'sum', 'col5': 'mean'}).reset_index()