import pandas as pd
import numpy as np
from glob import glob
import psycopg2
import os
import time
import getpass




# declaring variable...
__host = "database1.csioilzjaaf3.ap-south-1.rds.amazonaws.com"
__dbname = "database1"
__user = "admin"
__port = 5432
__password = str()
__All_Files = str()


Files_location = str(input("Enter folder location : ")).replace('\\','\\\\')

# Login detail Function...
def login_detail():
    print(f"Host     : {__host}")
    time.sleep(0.7)
    print(f"Database : {__dbname}")
    time.sleep(0.2)
    print(f"Port     : {__port}")
    time.sleep(0.2)
    print(f"User     : {__user}")
    time.sleep(0.2)
    __password_temp = getpass.getpass(prompt='Password : ', stream=None) 
    return __password_temp



def import_data():

    if len(os.listdir(Files_location) ) == 0:
        print('File not present on given location either folder is empty...')
        df = pd.DataFrame()
        return df
    else:
        stock_files = sorted(glob(Files_location+'\\\\*.csv'))
        df = pd.concat((pd.read_csv(file).assign(filename = file) for file in stock_files), ignore_index = True)
        return df


def creating_outlets(df):
    df = df
    df = df.copy()
    
    # create a list of our conditions
    conditions = [
        (df['Restaurant ID'] == 21305),
        (df['Restaurant ID'] == 21125),
        (df['Restaurant ID'] == 23268),
        (df['Restaurant ID'] == 23501),
        (df['Restaurant ID'] == 23729),
        (df['Restaurant ID'] == 24382),
        (df['Restaurant ID'] == 34633),
        (df['Restaurant ID'] == 46303),
        (df['Restaurant ID'] == 46304),
        (df['Restaurant ID'] == 52354),
        (df['Restaurant ID'] == 54615),
        (df['Restaurant ID'] == 59264)
        ]

    # create a list of the values we want to assign for each condition
    values = ['Kasarvadavli','Brahmand','Vrindavan','charai','Tulsidham','Airoli','Powai','Vashi','Kandivali','Andheri West','Chembur','Mira/Bhayandar']

    # create a new column and use np.select to assign values to it using our lists as arguments
    df['Outlet'] = np.select(conditions, values)

    return df




def time_range(df):
    df = df

    # create a list of our conditions
    conditions = [
        (df['Time Diff'] <= 0.1),
        (df['Time Diff'] >0.10) & (df['Time Diff'] <=0.20),
        (df['Time Diff'] > 0.20) & (df['Time Diff'] <=0.30),
        (df['Time Diff'] > 0.30) & (df['Time Diff'] <=0.40),
        (df['Time Diff'] > 0.40) & (df['Time Diff'] <=0.50),
        (df['Time Diff'] > 0.50) & (df['Time Diff'] <=1.00),
        (df['Time Diff'] > 1.00) & (df['Time Diff'] <=1.10),
        (df['Time Diff'] > 1.10) & (df['Time Diff'] <=1.20),
        (df['Time Diff'] > 1.20) & (df['Time Diff'] <=1.30),
        (df['Time Diff'] > 1.30) & (df['Time Diff'] <=1.40),
        (df['Time Diff'] > 1.40) & (df['Time Diff'] <=1.50),
        (df['Time Diff'] > 1.50) & (df['Time Diff'] <=2.00),
        (df['Time Diff'] > 2.00) & (df['Time Diff'] <=2.10),
        (df['Time Diff'] > 2.10) & (df['Time Diff'] <=2.20),
        (df['Time Diff'] > 2.20) & (df['Time Diff'] <=2.30),
        (df['Time Diff'] > 2.30) & (df['Time Diff'] <=2.40),
        (df['Time Diff'] > 2.40) & (df['Time Diff'] <=2.50),
        (df['Time Diff'] > 2.50) & (df['Time Diff'] <=3.00),
        (df['Time Diff'] > 3.00)
        ]

    # create a list of the values we want to assign for each condition
    values = ["10 min","20 min","30 min","40 min","50 min","60 min","1 hr 10 min","1 hr 20 min","1 hr 30 min","1 hr 40 min","1 hr 50 min","2 hr","2 hr 10 min","2 hr 20 min","2 hr 30 min","2 hr 40 min","2 hr 50 min","3 hr","3 hr 10 min"]

    # create a new column and use np.select to assign values to it using our lists as arguments
    df['Time Range'] = np.select(conditions, values)

    return df


def cleaning_data(dfi):

    df = dfi
    df = df.drop_duplicates(subset = ['Order ID'], keep = 'first')


    # Getting only selected columns.....
    df = df[['Order ID','Restaurant ID','Accepted Time','Mark Ready Time']]


    # Removing Nan from Accepted Time & Mark Ready Time...
    df.dropna(subset = ["Accepted Time"], inplace=True)
    df.dropna(subset = ["Mark Ready Time"], inplace=True)


    # Creating Date, Months and Year form Accepted Time.....
    temp_df_01 = df["Accepted Time"].str.split(" ", n = 3, expand = True)
    df['Date'] = temp_df_01[0]
    df['Months'] = temp_df_01[1]
    df['Year'] = temp_df_01[2]
    df['temp'] = temp_df_01[3]


    # Getting time difference form Mark Ready Time & Accepted Time...
    df['Time_temp'] = (pd.to_datetime(df['Mark Ready Time']) - pd.to_datetime(df['Accepted Time'])).astype(str)


    # Creating date form Time_temp in proper formate....
    temp_df_02 = df["Time_temp"].str.split(" ", n = 3, expand = True)
    df["unwanted_01"]= temp_df_02[0]
    df["unwanted_02"]= temp_df_02[1]
    df['Time_temp_02'] = temp_df_02[2]

    temp_df_03 = df["Time_temp_02"].str.split(":", n = 3, expand = True)
    df["Hr"]= temp_df_03[0]
    df["Min"]= temp_df_03[1]
    df['Sec'] = temp_df_03[2]

    df['Time Diff'] = df['Hr']+"."+df['Min']


    # Function that create outlets data from Reastaurent ID...
    df = creating_outlets(df)


    # Droping useless columns....
    df = df.drop(columns=['Time_temp','unwanted_01','unwanted_02','Time_temp_02','Hr','Min','Sec'])


    # Set a proper data types for columns...
    df[['Date','Year']] = df[['Date','Year']].astype(int)
    df['Time Diff'] = df['Time Diff'].astype(float)


    # Function that create Time Range data...
    df = time_range(df)


    # Getting usefull data...
    df = df[['Order ID','Restaurant ID','Accepted Time','Mark Ready Time','Date','Year','Months','Time Diff','Outlet','Time Range']]

    return df



def load_postgreSQL(df):

    df = df
    conn = None
    cur = None


    try:
        conn = psycopg2.connect(
            host = __host,
            dbname = __dbname,
            user = __user,
            password = __password,
            port = __port
            )

        cur = conn.cursor()


        insert_script = '''INSERT INTO packing_report VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''


        record_number = 1
        for record in df.values.tolist():
            cur.execute(insert_script,record)

            print(record_number,record)
            record_number = record_number + 1

        conn.commit()
        remove_columns()
        info(df)
        time.sleep(20)

    except Exception as error:
        print(error)
        time.sleep(20)
    except NameError as error:
        print("Error : Entered month not present in DataBase, please enter a correct month.")
        time.sleep(20)
    except psycopg2.OperationalError as error_pass:
        print('\nError : Password incorrect.')
        time.sleep(20)

    finally:
        if cur is not None:
            cur.close()

        if conn is not None:
            conn.close()


def remove_columns():
    stock_files = sorted(glob(Files_location+'\\\\*.csv'))
    for i in stock_files:
        os.remove(i)


def info(df):
    
    df = df

    print("Which date of data recorded")
    for i in list(df['Date'].unique()):
        print(i)
    print()

    outlets_count = 1
    print("Which outlets of data recorded")
    for i in list(df['Outlet'].unique()):
        lenght = len(df[df['Outlet'] == i] )
        print(f"{outlets_count} : {i} with count : {lenght} ")
        outlets_count = outlets_count + 1

    print()

    print(f"Total number of data recorded  :  {len(df)}.")
    print("Thank You")

if __name__ == "__main__":

    __password = login_detail()
    dfi = import_data()
    if len(dfi) == 0:
        print("Application terminating...")
        time.sleep(7)
    else:
        df = cleaning_data(dfi)
        load_postgreSQL(df)