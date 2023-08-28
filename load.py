import mysql.connector
from extract import all_df
from sql import queries, queries1, queries2, queries3, queries4
import streamlit as st

# files paths
agg_usr_path = r'C:/Users/AR KING/desktop/phonepay/pulse-master/data/aggregated/user/country/india/state'
agg_tran_path = r'C:/Users/AR KING/desktop/phonepay/pulse-master/data/aggregated/transaction/country/india/state'
map_tran_path = r'C:/Users/AR KING/desktop/phonepay/pulse-master/data/map/transaction/hover/country/india/state'
map_usr_path = r'C:/Users/AR KING/desktop/phonepay/pulse-master/data/map/user/hover/country/india/state'
usr_pin_path = r'C:/Users/AR KING/desktop/phonepay/pulse-master/data/top/user/country/india/state'

aggregateds_transaction, aggregateds_users, maps_transaction, maps_users, pincode_values = all_df(agg_usr_path,
                                                                                                  agg_tran_path,
                                                                                                  usr_pin_path,
                                                                                                  map_tran_path,
                                                                                                  map_usr_path)

conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password=""
)
cursor = conn.cursor()


def create_tables(conn, cursor):
    cursor.execute("CREATE DATABASE IF NOT EXISTS PhonePhe")
    cursor.execute("USE PhonePhe")
    # queries from Sql_quries

    for qur in queries:
        cursor.execute(qur)
        conn.commit()


def create_tables1(conn, cursor):
    cursor.execute("CREATE DATABASE IF NOT EXISTS PhonePhe")
    cursor.execute("USE PhonePhe")
    # queries from Sql_quries

    for qur in queries1:
        cursor.execute(qur)
        conn.commit()


def create_tables2(conn, cursor):
    cursor.execute("CREATE DATABASE IF NOT EXISTS PhonePhe")
    cursor.execute("USE PhonePhe")
    # queries from Sql_quries

    for qur in queries2:
        cursor.execute(qur)
        conn.commit()


def create_tables3(conn, cursor):
    cursor.execute("CREATE DATABASE IF NOT EXISTS PhonePhe")
    cursor.execute("USE PhonePhe")
    # queries from Sql_quries
    for qur in queries3:
        cursor.execute(qur)
        conn.commit()


def create_tables4(conn, cursor):
    cursor.execute("CREATE DATABASE IF NOT EXISTS PhonePhe")
    cursor.execute("USE PhonePhe")
    # queries from Sql_quries
    for qur in queries4:
        cursor.execute(qur)
        conn.commit()


def Trans_Methods_insert(conn, cursor):
    state_list = aggregateds_transaction['state'].unique().tolist()
    for state_name in state_list:
        cursor.execute(f"INSERT IGNORE INTO State (StateName) VALUES ('{state_name}')")
    conn.commit()

    # insert unique years into year table
    year_list = aggregateds_transaction['year'].unique().tolist()
    for year_value in year_list:
        cursor.execute(f"INSERT IGNORE INTO Year (Year) VALUES ({year_value})")
    conn.commit()

    # map state and year values to their corresponding primary keys
    for index, row in aggregateds_transaction.iterrows():
        # Get state_id
        state_name = row['state']
        cursor.execute(f"SELECT StateID FROM State WHERE StateName='{state_name}'")
        state_id = cursor.fetchone()[0]

        # Get year_id
        year_value = row['year']
        cursor.execute(f"SELECT YearID FROM Year WHERE Year={year_value}")
        year_id = cursor.fetchone()[0]

        # Insert transaction data
        cursor.execute(
            f"INSERT INTO TransactionMethods (TransactionMethod, TransactionCounts, TransactionAmounts,Quarter, StateID, YearID) VALUES ('{row['Transaction_method']}', {row['Transaction_Counts']}, {row['Transaction_amounts']},{row['quarter']}, {state_id}, {year_id})")

        conn.commit()


def Trans_Location_insert(conn, cursor):
    state_list = [state for state in maps_transaction['state'].unique()]  # changed
    for state_name in state_list:
        cursor.execute(f"INSERT IGNORE INTO State (StateName) VALUES ('{state_name}')")
    conn.commit()

    # insert unique years into year table
    year_list = maps_transaction['year'].unique().tolist()
    for year_value in year_list:
        cursor.execute(f"INSERT IGNORE INTO Year (Year) VALUES ({year_value})")
    conn.commit()

    # map state and year values to their corresponding primary keys
    for index, row in maps_transaction.iterrows():
        # Get state_id
        state_name = row['state']
        cursor.execute(f"SELECT StateID FROM State WHERE StateName='{state_name}'")
        state_id = cursor.fetchone()[0]

        # Get year_id
        year_value = row['year']
        cursor.execute(f"SELECT YearID FROM Year WHERE Year={year_value}")
        year_id = cursor.fetchone()[
            0]  # ['Distric_Name', 'Total_Transaction_count', 'Total_Transaction_amount','state', 'year']

        # Insert transaction data
        cursor.execute(
            f"INSERT INTO TransactionLocation (DistrictName, TotalTransactionCount, TotalTransactionAmount,Quarter,StateID, YearID) VALUES ('{row['Distric_Name']}', {row['Total_Transaction_count']}, {row['Total_Transaction_amount']},{row['quarter']},{state_id}, {year_id})")
        conn.commit()


def Users_Device_insert(conn, cursor):
    state_list = [state for state in aggregateds_users['state'].unique()]

    for state_name in state_list:
        cursor.execute(f"INSERT IGNORE INTO State (StateName) VALUES ('{state_name}')")
    conn.commit()

    # insert unique years into year table
    year_list = aggregateds_users['year'].unique().tolist()
    for year_value in year_list:
        cursor.execute(f"INSERT IGNORE INTO Year (Year) VALUES ({year_value})")
    conn.commit()

    # map state and year values to their corresponding primary keys
    for index, row in aggregateds_users.iterrows():
        # Get state_id
        state_name = row['state']
        cursor.execute(f"SELECT StateID FROM State WHERE StateName='{state_name}'")
        state_id = cursor.fetchone()[0]

        # Get year_id
        year_value = row['year']
        cursor.execute(f"SELECT YearID FROM Year WHERE Year={year_value}")
        year_id = cursor.fetchone()[0]

        # Insert transaction data
        cursor.execute(
            f"INSERT INTO UsersDevice (DeviceBrandName, BrandCount, Percentage, NoOfUsers, AppOpening,Quarter, StateID, YearID) VALUES ('{row['Device_Brand']}', {row['Brand_count']}, {row['percentage']}, {row['Registered_users']}, {row['App_opening']},{row['quarter']}, {state_id}, {year_id})")
        conn.commit()


def Users_Location_insert(conn, cursor):
    state_list = [state for state in maps_users['state'].unique()]
    for state_name in state_list:
        cursor.execute(f"INSERT IGNORE INTO State (StateName) VALUES ('{state_name}')")
    conn.commit()

    # insert unique years into year table
    year_list = maps_users['year'].unique().tolist()
    for year_value in year_list:
        cursor.execute(f"INSERT IGNORE INTO Year (Year) VALUES ({year_value})")
    conn.commit()

    # map state and year values to their corresponding primary keys
    for index, row in maps_users.iterrows():
        # Get state_id
        state_name = row['state']
        cursor.execute(f"SELECT StateID FROM State WHERE StateName='{state_name}'")
        state_id = cursor.fetchone()[0]

        # Get year_id
        year_value = row['year']
        cursor.execute(f"SELECT YearID FROM Year WHERE Year={year_value}")
        year_id = cursor.fetchone()[0]

        # Insert transaction data
        cursor.execute(
            f"INSERT INTO UsersLocation (DistrictName, UsersCount, AppOpening, Quarter, StateID, YearID) VALUES ('{row['Distric_Name']}', {row['Registered_users']}, {row['App_opening']},{row['quarter']},{state_id}, {year_id})")
        conn.commit()


def Pincode_insert(conn, cursor):
    state_list = [state for state in pincode_values['state'].unique()]
    for state_name in state_list:
        cursor.execute(f"INSERT IGNORE INTO State (StateName) VALUES ('{state_name}')")
    conn.commit()

    # insert unique years into year table
    year_list = pincode_values['year'].unique().tolist()
    for year_value in year_list:
        cursor.execute(f"INSERT IGNORE INTO Year (Year) VALUES ({year_value})")
    conn.commit()

    # map state and year values to their corresponding primary keys
    for index, row in pincode_values.iterrows():
        # Get state_id
        state_name = row['state']
        cursor.execute(f"SELECT StateID FROM State WHERE StateName='{state_name}'")
        state_id = cursor.fetchone()[0]

        # Get year_id
        year_value = row['year']
        cursor.execute(f"SELECT YearID FROM Year WHERE Year={year_value}")
        year_id = cursor.fetchone()[0]

        # Insert transaction data
        cursor.execute(
            f"INSERT INTO Pincodes (Pincodes,Total_Registered_users,Quarter,StateID, YearID) VALUES ('{row['Pincodes']}', {row['Total_Registered_users']}, {row['quarter']}, {state_id}, {year_id})")
        conn.commit()


def all_data():
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password=""
    )
    cursor = conn.cursor()
    create_tables(conn, cursor)
    Users_Device_insert(conn, cursor)


def all_data1():
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password=""
    )
    cursor = conn.cursor()
    create_tables1(conn, cursor)
    Users_Location_insert(conn, cursor)


def all_data2():
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password=""
    )
    cursor = conn.cursor()
    create_tables2(conn, cursor)
    Trans_Location_insert(conn, cursor)


def all_data3():
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password=""
    )
    cursor = conn.cursor()
    create_tables3(conn, cursor)
    Trans_Methods_insert(conn, cursor)


def all_data4():
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password=""
    )
    cursor = conn.cursor()
    create_tables4(conn, cursor)
    Pincode_insert(conn, cursor)
    # close database connection
    cursor.close()
    conn.close()


b1 = st.button("button1")
b2 = st.button("button2")
b3 = st.button("button3")
b4 = st.button("button4")
b5 = st.button("button5")

if b1:
    all_data()
    st.success("done")

if b2:
    all_data1()
    st.success("done")

if b3:
    all_data2()
    st.success("done")

if b4:
    all_data3()
    st.success("done")

if b5:
    all_data4()
    st.success("done")
