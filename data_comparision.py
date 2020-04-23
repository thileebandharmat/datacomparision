# Copyright (C) 2020 Thileeban Dharma <thileebandharma@gmail.com>
import sys
import os
import time
import pandas as pd

class DataComparision:
    """This class has functions for length check, datatype_check and Data comparision"""

    def length_check(source, target):
        """This functions receives two dataframes as input, compares the number of columns
        between the dataframes.
        Return : If number of column matches then the control passed to next step,
        else program will terminate. User should correct this to proceed.
        """
        print("Number of columns in source1: ", len(source.columns))
        print("Number of columns in source1: ", len(target.columns))
        if len(source.columns) == len(target.columns):
            print("Number of columns matches between source1 and source 2")
        else:
            print("Number of columns in source1 and source1 are not matching, Kindly get this corrected")
            time.sleep(30)
            sys.exit("Length not matches:")

    def datatype_check(source, target):
        """This functions receives two dataframes as an input, compares the datatype between both
        the datatypes.
        Return: If matches control passed to next statement, else program will terminate. User should correct
        this to Proceed.
        """
        print("Source 1 Datatypes: ")
        print(source.dtypes)
        print("Source 2 Datatypes: ")
        print(target.dtypes)
        if (source.dtypes == target.dtypes).all():
            print("Datatype matching between Source and Target.,")
        else:
            print("Datatype not matching between source and target.,")
            time.sleep(30)
            sys.exit("Datatype not matching")

    def comparision_by_index(source, target, primary_key, output_path):
        """This function receives two dataframes as an input.
        compares both the dataframes based on the index.
        Identifies extra records available in both the dataframes and write it as csv files.
        Identifies mismatch values for each matching index and writes it as csv file.
        Creates a summary report and write it as a csv file.
        """
        target.columns = source.columns
        # setting the new index based on the unique key
        source = source.sort_values(primary_key).set_index(primary_key)
        target = target.sort_values(primary_key).set_index(primary_key)
        # finding the extra keys available in source and target
        print("Started identifying extra records in source file 1")
        source_id_unidentified = source[~source.index.isin(target.index)].reset_index().to_csv(os.path.join(output_path, "Data_Sourcekey_notavailable_inTarget.csv"), index=False)
        print("Saved the extra records in source file 1")
        print("Started identifying extra records in source file 2")
        target_id_unidentified = target[~target.index.isin(source.index)].reset_index().to_csv(os.path.join(output_path, "Data_Targetkey_notavailable_inSource.csv"), index=False)
        print("Saved the extra records in source file 1")
        # finding matching records
        source_match = source[source.index.isin(target.index)].reset_index()
        target_match = target[target.index.isin(source.index)].reset_index()

        def mismatch(source, target):
            source_bool = (source != target).stack()
            mismatch = pd.concat([source.stack()[source_bool], target.stack()[source_bool]], axis=1)
            mismatch.columns = ['Data_source1_mismatch','Data_source_2_mismatch']
            return mismatch

        def match(source, target):
            source_bool = (source == target).stack()
            match = pd.concat([source.stack()[source_bool], target.stack()[source_bool]], axis=1)
            match.columns = ['Data_source1_match', 'Data_source_2_match']
            return match

        print("Started loading report with Primary index match and data mismatch")
        mismatch = mismatch(source[source.index.isin(target.index)], target[target.index.isin(source.index)]).reset_index()
        mismatch.to_csv(os.path.join(output_path, "mismatch.csv"), index=False)
        print("Completed loading report with Primary index match and data mismatch")
        print("Started loading report with both Primary index and data match")
        match = match(source[source.index.isin(target.index)], target[target.index.isin(source.index)]).reset_index()
        match.to_csv(os.path.join(output_path, "match.csv"), index=False)
        print("Completed loading report with both Primary index and data match")
        # summary report and column wise report creation
        print("Started preparing Summary report")
        level = 'level_' + str(len(primary_key))
        match_count = match[level].value_counts().reset_index(name="matched_count")
        mismatch_count = mismatch[level].value_counts().reset_index(name="mismatch_count")
        summary = match_count.merge(mismatch_count, how='outer')
        summary.to_csv(os.path.join(output_path, "SUMMARY.csv"), index=False)
        print("Completed loading Summary report")
        # split files by grouping and writing to csv
        print("Started loading column wise mismatch report")
        for i, x in mismatch.groupby(level):
            p = os.path.join(output_path + '/'"{}_mismatched.csv".format(i.upper()))
            x.to_csv(p, index=False)
        print("Completed loading column wise mismatch report")
        # split files by grouping and writing to csv
        print("Started loading column wise match report")
        for i, x in match.groupby(level):
            p = os.path.join(output_path + '/'"{}_matched.csv".format(i.upper()))
            x.to_csv(p, index=False)
        print("Completed loading column wise match report")
        print("Done")


