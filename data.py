# need full package
import pandas as pd
import streamlit as st
import PIL
from PIL import Image
from streamlit_option_menu import option_menu
import plotly.express as px
import pandas as pd
import matplotlib.pyplot as plt
import requests
import geopandas as gpd
# connect to the database
import mysql.connector

# UI -----------------------------------------

page_title = "Phonepe pulse data visualization "
page_icon = ":money_with_wings:"
layout = "wide"

st.set_page_config(page_title=page_title, page_icon=page_icon, layout=layout)
st.title(page_title + " " + page_icon)

SELECT = option_menu(
    menu_title=None,
    options=["Home", "Basic insights"],
    icons=["house", "toggles"],
    default_index=0,
    orientation="horizontal",
    styles={"container": {"padding": "0!important", "background-color": "white", "size": "cover", "width": "100%"},
            "icon": {"color": "black", "font-size": "20px"},
            "nav-link": {"font-size": "20px", "text-align": "center", "margin": "-2px", "--hover-color": "#6F36AD"},
            "nav-link-selected": {"background-color": "#6F36AD"}})

# basic insights

if SELECT == "Basic insights":
    st.title("BASIC INSIGHTS")
    st.write("----")
    st.subheader("Let's know some basic insights about the data")
    options = ["--select--",
               "Top 10 states based on year and amount of transaction",
               "List 10 states based on type and amount of transaction",
               "Top 5 Transaction_Type based on Transaction_Amount",
               "Top 10 Registered-users based on States and District",
               "Top 10 Districts based on states and Count of transaction",
               "List 10 Districts based on states and amount of transaction",
               ]

    select = st.selectbox("Select the option", options)
    # connection initiated----------

    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password=""
    )
    cursor = conn.cursor()
    cursor.execute("use phonephe")

    if select == options[1]:

        qu = "SELECT StateID, YearID, sum(TransactionAmounts) AS Total_transaction_amount  FROM transactionmethods " \
             "GROUP BY StateID, " \
             "YearID   ORDER BY Total_transaction_amount DESC LIMIT 10 "
        df = pd.read_sql(qu, conn)
        col1, col2, col3 = st.columns(3)
        with col1:
            st.title(options[1])
            st.write(df)
        with col2:
            st.title("Mapping StateName corresponds to StateID  ")
            qu = "SELECT * FROM state "
            de = pd.read_sql(qu, conn)
            st.write(de)
        with col3:
            st.title("Bar chart ")
            st.bar_chart(data=df, x="Total_transaction_amount", y="StateID")

    elif select == options[2]:

        qu = "SELECT StateID, TransactionMethod ,sum(TransactionAmounts) AS Total_transaction_amount  FROM " \
             "transactionmethods GROUP BY StateID, " \
             "TransactionMethod   ORDER BY Total_transaction_amount DESC LIMIT 10 "
        df = pd.read_sql(qu, conn)
        col1, col2, col3 = st.columns(3)
        with col1:
            st.title(options[1])
            st.write(df)
        with col2:
            st.title("To check the which stateid = state name")
            qu = "SELECT * FROM state "
            de = pd.read_sql(qu, conn)
            st.write(de)
        with col3:
            st.title("Bar chart ")
            st.bar_chart(data=df, x="Total_transaction_amount", y="TransactionMethod")

    elif select == options[3]:

        qu = "SELECT TransactionMethod, sum(TransactionAmounts) AS total_transaction_amount FROM transactionmethods " \
             "GROUP BY TransactionMethod ORDER BY total_transaction_amount DESC LIMIT 5 "
        df = pd.read_sql(qu, conn)
        col1, col2, col3 = st.columns(3)
        with col1:
            st.title(options[1])
            st.write(df)
        with col2:
            st.title("To check the which stateid = state name")
            qu = "SELECT * FROM state "
            de = pd.read_sql(qu, conn)
            st.write(de)
        with col3:
            st.title("Bar chart ")
            st.bar_chart(data=df, x="TransactionMethod", y="total_transaction_amount")

    elif select == options[4]:

        qu = "SELECT DistrictName, StateID, sum(UsersCount) AS total_user FROM userslocation GROUP BY DistrictName, " \
             "StateID ORDER BY total_user DESC LIMIT 10 "
        df = pd.read_sql(qu, conn)
        col1, col2, col3 = st.columns(3)
        with col1:
            st.title(options[1])
            st.write(df)
        with col2:
            st.title("To check the which stateid = state name")
            qu = "SELECT * FROM state "
            de = pd.read_sql(qu, conn)
            st.write(de)
        with col3:
            st.title("Bar chart ")
            st.bar_chart(data=df, x="StateID", y="total_user")

    elif select == options[5]:
        qu = "SELECT DistrictName, StateID, sum(TotalTransactionCount) AS total_transaction_count FROM " \
             "transactionlocation GROUP BY DistrictName, StateID ORDER BY  total_transaction_count DESC LIMIT 10 "
        df = pd.read_sql(qu, conn)
        col1, col2, col3 = st.columns(3)
        with col1:
            st.title(options[1])
            st.write(df)
        with col2:
            st.title("To check the which stateid = state name")
            qu = "SELECT * FROM state "
            de = pd.read_sql(qu, conn)
            st.write(de)
        with col3:
            st.title("Bar chart ")
            st.bar_chart(data=df, x="StateID", y="total_transaction_count")

    elif select == options[6]:

        qu = "SELECT DistrictName, StateID, sum(TotalTransactionAmount) AS total_transaction_Amount FROM " \
             "transactionlocation GROUP BY DistrictName, StateID ORDER BY  total_transaction_Amount DESC LIMIT 10 "
        df = pd.read_sql(qu, conn)
        col1, col2, col3 = st.columns(3)
        with col1:
            st.title(options[1])
            st.write(df)
        with col2:
            st.title("To check the which stateid = state name")
            qu = "SELECT * FROM state "
            de = pd.read_sql(qu, conn)
            st.write(de)
        with col3:
            st.title("Bar chart ")
            st.bar_chart(data=df, x="StateID", y="total_transaction_Amount")

if SELECT == "Home":

    st.subheader(':blue[Registered Users Hotspots - States]')

    Data_Aggregated_Transaction_df = pd.read_csv(
        r'C:/Users/AR KING/desktop/phonepay/datasets/Data_Aggregated_Transaction_Table.csv')
    Data_Aggregated_User_Summary_df = pd.read_csv(
        r'C:/Users/AR KING/desktop/phonepay/datasets/Data_Aggregated_User_Summary_Table.csv')
    Data_Aggregated_User_df = pd.read_csv(r'C:/Users/AR KING/desktop/phonepay/datasets/Data_Aggregated_User_Table.csv')
    Scatter_Geo_Dataset = pd.read_csv(
        r'C:/Users/AR KING/desktop/phonepay/datasets/Data_Map_Districts_Longitude_Latitude.csv')
    Coropleth_Dataset = pd.read_csv(r'C:/Users/AR KING/desktop/phonepay/datasets/Data_Map_IndiaStates_TU.csv')
    Data_Map_Transaction_df = pd.read_csv(r'C:/Users/AR KING/desktop/phonepay/datasets/Data_Map_Transaction_Table.csv')
    Data_Map_User_Table = pd.read_csv(r'C:/Users/AR KING/desktop/phonepay/datasets/Data_Map_User_Table.csv')
    Indian_States = pd.read_csv(r'C:/Users/AR KING/desktop/phonepay/datasets/Longitude_Latitude_State_Table.csv')

    c1, c2 = st.columns(2)
    with c1:
        Year = st.selectbox(
            'Please select the Year',
            ('2018', '2019', '2020', '2021', '2022'))
    with c2:
        Quarter = st.selectbox(
            'Please select the Quarter',
            ('1', '2', '3', '4'))
    year = int(Year)
    quarter = int(Quarter)

    Transaction_scatter_districts = Data_Map_Transaction_df.loc[
        (Data_Map_Transaction_df['Year'] == year) & (Data_Map_Transaction_df['Quarter'] == quarter)].copy()
    Transaction_Coropleth_States = Transaction_scatter_districts[Transaction_scatter_districts["State"] == "india"]
    Transaction_scatter_districts.drop(
        Transaction_scatter_districts.index[(Transaction_scatter_districts["State"] == "india")], axis=0, inplace=True)
    # Dynamic Scattergeo Data Generation

    Transaction_scatter_districts = Transaction_scatter_districts.sort_values(by=['Place_Name'], ascending=False)
    Scatter_Geo_Dataset = Scatter_Geo_Dataset.sort_values(by=['District'], ascending=False)
    Total_Amount = []
    for i in Transaction_scatter_districts['Total_Amount']:
        Total_Amount.append(i)
    Scatter_Geo_Dataset['Total_Amount'] = Total_Amount
    Total_Transaction = []
    for i in Transaction_scatter_districts['Total_Transactions_count']:
        Total_Transaction.append(i)
    Scatter_Geo_Dataset['Total_Transactions'] = Total_Transaction
    Scatter_Geo_Dataset['Year_Quarter'] = str(year) + '-Q' + str(quarter)
    # Dynamic Coropleth

    Coropleth_Dataset = Coropleth_Dataset.sort_values(by=['state'], ascending=False)
    Transaction_Coropleth_States = Transaction_Coropleth_States.sort_values(by=['Place_Name'], ascending=False)
    Total_Amount = []
    for i in Transaction_Coropleth_States['Total_Amount']:
        Total_Amount.append(i)
    Coropleth_Dataset['Total_Amount'] = Total_Amount
    Total_Transaction = []
    for i in Transaction_Coropleth_States['Total_Transactions_count']:
        Total_Transaction.append(i)
    Coropleth_Dataset['Total_Transactions'] = Total_Transaction

    # scatter plotting the states codes
    Indian_States = Indian_States.sort_values(by=['state'], ascending=False)
    Indian_States['Registered_Users'] = Coropleth_Dataset['Registered_Users']
    Indian_States['Total_Amount'] = Coropleth_Dataset['Total_Amount']
    Indian_States['Total_Transactions'] = Coropleth_Dataset['Total_Transactions']
    Indian_States['Year_Quarter'] = str(year) + '-Q' + str(quarter)
    fig = px.scatter_geo(Indian_States,
                         lon=Indian_States['Longitude'],
                         lat=Indian_States['Latitude'],
                         text=Indian_States['code'],  # It will display district names on map
                         hover_name="state",
                         hover_data=['Total_Amount', "Total_Transactions", "Year_Quarter"],
                         )
    fig.update_traces(marker=dict(color="white", size=0.3))
    fig.update_geos(fitbounds="locations", visible=False, )
    # scatter plotting districts
    Scatter_Geo_Dataset['col'] = Scatter_Geo_Dataset['Total_Transactions']
    fig1 = px.scatter_geo(Scatter_Geo_Dataset,
                          lon=Scatter_Geo_Dataset['Longitude'],
                          lat=Scatter_Geo_Dataset['Latitude'],
                          color=Scatter_Geo_Dataset['col'],
                          size=Scatter_Geo_Dataset['Total_Transactions'],
                          # text = Scatter_Geo_Dataset['District'], #It will display district names on map
                          hover_name="District",
                          hover_data=["State", "Total_Amount", "Total_Transactions", "Year_Quarter"],
                          title='District',
                          size_max=22)

    fig1.update_traces(marker=dict(color="rebeccapurple", line_width=1))  # rebeccapurple
    # coropleth mapping india
    fig_ch = px.choropleth(
        Coropleth_Dataset,
        geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw"
                "/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
        featureidkey='properties.ST_NM',
        locations='state',
        color="Total_Transactions",
    )
    fig_ch.update_geos(fitbounds="locations", visible=False, )
    # combining districts states and coropleth
    fig_ch.add_trace(fig.data[0])
    fig_ch.add_trace(fig1.data[0])
    st.write("### **:blue[PhonePe India Map]**")
    colT1, colT2 = st.columns([6, 4])
    with colT1:
        st.plotly_chart(fig_ch, use_container_width=True)
    with colT2:
        st.info(
            """
            Details of Map:
            - The darkness of the state color represents the total transactions
            - The Size of the Circles represents the total transactions dictrict wise
            - The bigger the Circle the higher the transactions
            - Hover data will show the details like Total transactions, Total amount
            """
        )
        st.info(
            """
            Important Observations:
            - User can observe Transactions of PhonePe in both statewide and Districtwide.
            - We can clearly see the states with highest transactions in the given year and quarter
            - We get basic idea about transactions district wide
            """
        )
    # -----------------------------------------------FIGURE2 HIDDEN
    # BARGRAPH------------------------------------------------------------------------
    Coropleth_Dataset = Coropleth_Dataset.sort_values(by=['Total_Transactions'])
    fig = px.bar(Coropleth_Dataset, x='state', y='Total_Transactions', title=str(year) + " Quarter-" + str(quarter))
    with st.expander("See Bar graph for the same data"):
        st.plotly_chart(fig, use_container_width=True)
        st.info(
            '**:blue[The above bar graph showing the increasing order of PhonePe Transactions according to the states '
            'of India, Here we can observe the top states with highest Transaction by looking at graph]**')
