import pandas as pd
import os
import numpy as np


def csv_files():
    csv_files=[]
    for file in os.listdir(os.getcwd()):
        if file.endswith(".csv"):
            csv_files.append(file)
    return csv_files


def configure_dataset_directory(csv_files, dataset_dir):
    # Make directory
    mkdir = f"mkdir {dataset_dir}"
    os.system(mkdir)

    # Move csv files to directory
    for csv in csv_files:
        mv_file = f"move {csv} {dataset_dir}"
        os.system(mv_file)
        print(mv_file)

def create_df(csv_files,dataset_dir):
    # Path to csv files
    data_path=os.getcwd()+"\\"+dataset_dir
    # loop throw the csv files and create dataframe
    df={}
    for file in csv_files:
        try:
            df[file]=pd.read_csv(os.path.join(data_path,file))
        except UnicodeDecodeError:
            df[file]=pd.read_csv(os.path.join(data_path,file),encoding="ISO-8859-1")
    return df

def clean_table_name(filename):
    clean_table_name=filename.lower().replace(" ","_").replace("?","") \
        .replace("-","_").replace( "/" ,"_").replace("\\","_") \
        .replace("%","").replace(")","").replace("(","").replace("$","")
    table_name="{}".format(clean_table_name.split(".")[0])
    return table_name


def clean_colname(dataframe):
    dataframe.columns = [x.lower().replace(" ", "_").replace("?", "") \
                             .replace("-", "_").replace("/", "_").replace("\\", "_") \
                             .replace("%", "").replace(")", "").replace("(", "").replace("$", "")
                         for x in dataframe.columns
                         ]
    # replacment dictionary that maps pandas dtypes to sql dtypes
    replacments = {
        "object": "varchar",
        "float64": "float",
        "int64": "int"
    }
    col_str = ", ".join(["{} {}".format(c, d) for (c, d) in zip(dataframe.columns, \
                                                                dataframe.dtypes.replace(replacments))])

    return col_str, dataframe.columns

def drop_duplicates(df,subset=None):
    if subset is None:
        subset=df.columns
    df.drop_duplicates(subset=subset,keep= "first",inplace=True)

def drop_na_rows(df):
    df.dropna(inplace=True)



