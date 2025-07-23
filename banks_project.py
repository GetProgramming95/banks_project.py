# Code for ETL operations on Country-GDP data

# Importing the required libraries
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import pandas as pd
from io import StringIO  # <- für read_html Warnung
import numpy as np  # ganz oben einfügen, falls noch nicht vorhanden
import sqlite3  # falls noch nicht importiert

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open("code_log.txt", "a") as f:
        f.write(f"{timestamp} : {message}\n")

log_progress("Vorbereitungen abgeschlossen. ETL-Prozess wird eingeleitet")

def extract(url, table_attribs):
    page = requests.get(url).text
    soup = BeautifulSoup(page, "html.parser")
    tables = soup.find_all("table", {"class": "wikitable"})

    df = pd.read_html(StringIO(str(tables[0])))[0]
    print("\n==== Spaltennamen der Tabelle ====")
    print(df.columns)

    df.columns = df.columns.str.strip()
    market_cap_col = [col for col in df.columns if "Market cap" in col][0]

    # ACHTUNG: "Bank name" statt "Name"
    df = df[['Bank name', market_cap_col]]
    df.columns = table_attribs  # --> ["Name", "MC_USD_Billion"]

    df['MC_USD_Billion'] = df['MC_USD_Billion'].astype(str).str.replace('\n', '')
    df['MC_USD_Billion'] = df['MC_USD_Billion'].astype(float)

    log_progress("Datenextraktion abgeschlossen. Transformationsprozess wird eingeleitet")
    return df




def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''

    # CSV-Datei lesen
    exchange_df = pd.read_csv(csv_path)

    # In Wörterbuch umwandeln
    exchange_rate = exchange_df.set_index('Currency').to_dict()['Rate']

    # Neue Spalten mit umgerechneten Werten (auf 2 Dezimalstellen runden)
    df['MC_GBP_Billion'] = [np.round(x * exchange_rate['GBP'], 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rate['EUR'], 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rate['INR'], 2) for x in df['MC_USD_Billion']]

    # Protokolleintrag
    log_progress("Datenumwandlung abgeschlossen. Ladeprozess wird eingeleitet")
    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path, index=False)
    log_progress("Daten in CSV-Datei gespeichert")


def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)
    log_progress("Daten in Datenbank als Tabelle geladen, Abfragen werden ausgeführt")

def run_queries(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print(f"\n>>> Abfrage: {query_statement}")
    cursor = sql_connection.cursor()
    cursor.execute(query_statement)
    rows = cursor.fetchall()
    for row in rows:
        print(row)
    log_progress("Prozess abgeschlossen")


''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

# ETL-Aktionen
url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
table_attributes = ["Name", "MC_USD_Billion"]

# Extraktion und Transformation
df_extracted = extract(url, table_attributes)
df_transformed = transform(df_extracted, "exchange_rate.csv")

# Ausgabe zur Kontrolle
print(df_transformed)
print("\nMarktkapitalisierung der 5. größten Bank in EUR:")
print(df_transformed['MC_EUR_Billion'][4])

# CSV speichern
output_csv_path = "./Largest_banks_data.csv"
load_to_csv(df_transformed, output_csv_path)

# Verbindung zur SQLite-Datenbank herstellen
db_connection = sqlite3.connect("Banks.db")
log_progress("SQL-Verbindung initiiert")

# In Datenbank speichern
load_to_db(df_transformed, db_connection, "Largest_banks")

# SQL-Abfragen ausführen
run_queries("SELECT * FROM Largest_banks", db_connection)
run_queries("SELECT AVG(MC_GBP_Billion) FROM Largest_banks", db_connection)
run_queries("SELECT Name FROM Largest_banks LIMIT 5", db_connection)

# Verbindung schließen
db_connection.close()
log_progress("Serververbindung geschlossen")



