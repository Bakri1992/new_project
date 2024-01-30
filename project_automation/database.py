import psycopg2
def config_of_database():
    db_config = {
        "host": input("Please enter the host of the database ? "),
        "database": input("Please enter the name of the database ? "),
        "user": input("Username ? "),
        "password": input("Password? "),
        "port": input("port number? ")
    }
    return db_config

def connect_to_db(db_config):
    conn=None
    cur=None
    try:
        conn =psycopg2.connect(**db_config)
        cur=conn.cursor()
        print("Connected to database successfully")
    except Exception as e:
        print(f"An error occured while connecting:{e} ")
    return conn ,cur

def create_tables(cur,table_name,shema):
    cur.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({shema})")
    print(f"{table_name} was created successfully")

def close_db_connection(conn,cur):
    cur.close()
    conn.close()


def copy_csv_to_db(cur,table_name,file):
    SQL_STATEMENT="""
    COPY %s FROM STDIN WITH
    CSV HEADER DELIMITER AS ','
    """

    cur.copy_expert(sql=SQL_STATEMENT % table_name,file=file)
    print(f"{file} copied to {table_name}")

def commit_to_db(conn):
    conn.commit()

def upload_to_db(db_config,table_name,schema,file,dataframe,dataframe_columns):
    conn,cur=connect_to_db(db_config)
    create_tables(cur,table_name,schema)
    # save the dataframe to csv
    dataframe.to_csv(file,header=dataframe_columns,index=False,encoding="utf-8")
    with open(file) as myfile:
        copy_csv_to_db(cur,table_name,myfile)
        commit_to_db(conn)
        print(f"{file} uploaded to {table_name} successfully")

    close_db_connection(conn,cur)

