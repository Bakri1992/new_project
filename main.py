from database import *
from clean import *

db_config = {
        'dbname': 'mydb',
        'user': 'postgres',
        'password': 'admin',
        'host': 'localhost',
        'port':5432
    }

# db_config=config_of_database()
# conn,cur=connect_to_db(db_config)
# create_tables(cur,"koko","id int, fname varchar(50), lname varchar(50)")
# commit_to_db(conn)
# close_db_connection(conn,cur)
def main():
    dataset_dir = "datasets"
    # db_config = config_of_database()
    my_files=csv_files()
    configure_dataset_directory(my_files,dataset_dir)
    df=create_df(my_files, dataset_dir)

    for k in my_files:
        # call the df
        dataframe=df[k]
        dataframe.dropna(inplace=True)

        # call table name if necessary
        tbl_name = clean_table_name(k)

        schema, dataframe.columns=clean_colname(dataframe)
        upload_to_db(db_config,
                     tbl_name,
                     schema,
                     file=k,
                     dataframe=dataframe,
                     dataframe_columns=dataframe.columns)



if __name__ == "__main__":
    main()