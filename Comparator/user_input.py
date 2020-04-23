import os
import time
from source_to_dataframe import SrcToDataframe as sd

class UserInput:
    """This class contains the functions which prompts user to enter the necessary inputs, and returns the same
    to the driver program"""

    def source_type():
        """ This function show the users the list of input types supported for comparision.
        Returns the input type as an integer"""
        src_file_type = int(input("Select the source type \n 1. csv file \n 2. Database \n 3. Json \n 4. Xml \n 5. Others \n"))
        return src_file_type

    def source_location(src_file_type):
        """This function receives the code for source file type as input, and gets the source file path
        from the User.
        Returns the source file absolute path as a string."""
        if src_file_type == 1:
            sourcefile_1_path = input("Enter the Absolute path of the csv file: ")
            return sourcefile_1_path, 0
        elif src_file_type == 2:
            database_type = int(input("Select the DB type \n 1. SQL Server \n 2. mySQL \n 3. Access \n 4. Netezza \n 5. Oracle \n"))
            source_1_path = input("Enter the Absolute path of the SQL file: ")
            return source_1_path, database_type
        elif src_file_type == 3:
            sourcefile_1_path = input("Enter the Absolute path of the json file: ")
            return sourcefile_1_path, 0
        elif src_file_type == 4:
            sourcefile_1_path = input("Enter the Absolute path of the xml file: ")
            return sourcefile_1_path, 0
        else:
            print("Selected file type not supported")

    def set_primary_key():
        """This function asks the user to enter the primary from the user.
        Returns the user entered primary key as a list of keys"""
        print("Enter the primary key from source_1 in the following format pk_column1,pk_column2,pk_column_n \n")
        print("Note: Primary keys to be comma separated, kinldy don't include unnecessary space between the keys \n")
        primary_key = input()
        primary_key = primary_key.split(',')
        return primary_key

    def create_output_path():
        """This function prompts the user to enter the output path. creates a directory with the received
        folder name appended with current timestamp.
        Returns the new output path where results will be placed"""
        root_path = input("Enter the Absolute path where you want test results: ex: D:\python\\newfolder\: ")
        time_str = time.strftime("%Y%m%d_%H%M%S")
        new_folder = root_path+time_str
        new_dir = os.path.join(root_path, new_folder)
        os.mkdir(new_dir)
        print(new_dir)
        return new_dir