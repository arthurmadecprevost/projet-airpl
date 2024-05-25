import datetime
import os
import requests
import csv
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
import numpy as np

def download(start_date, end_date):

    # Convert the dates to datetime objects
    start_datetime = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    end_datetime = datetime.datetime.strptime(end_date, "%Y-%m-%d")

    # Iterate over each day between the start and end dates
    current_datetime = start_datetime
    while current_datetime <= end_datetime:
        # Format the current date as a string
        current_date = current_datetime.strftime("%Y-%m-%d")
        print(current_date)

        # Create the year and month folders if they don't exist
        year_folder = current_datetime.strftime("%Y")
        month_folder = current_datetime.strftime("%m")

        # Define the path to the CSV file
        csv_file = f"data/{year_folder}/{month_folder}/file_{current_date}.csv"
        # Check if the file already exists
        if not os.path.exists(csv_file):
            # Define the URL for the current day
            url = f"https://data.airpl.org/api/v1/mesure/horaire/?&date_heure_tu__range={current_date},{current_date}%2023:00:00&code_configuration_de_mesure__code_point_de_prelevement__code_station__code_commune__code_departement__in=44,49,53,72,85,&export=csv"
            # Send a GET request to the URL
            response = requests.get(url)
            # Check if the request was successful
            if response.status_code == 200:
                data = response.content
                
                year_path = f"data/{year_folder}"
                month_path = f"data/{year_folder}/{month_folder}"
                os.makedirs(year_path, exist_ok=True)
                os.makedirs(month_path, exist_ok=True)
                
                # Decode the response content as a string
                data_str = data.decode("utf-8")
                # Split the data into lines
                lines = data_str.splitlines()
                # Create a CSV writer object
                csv_writer = csv.writer(open(csv_file, "w", newline="", encoding='utf-8'))
                # Write the lines to the CSV file
                for line in lines:
                    csv_writer.writerow(line.split(","))
                print("Data has been successfully stored in the CSV file:", csv_file)
            else:
                print("Error while downloading for date", current_date, ":", response.status_code)
        else:
            print("File already exists for date", current_date, "- Skipping download")
        # Move to the next day
        current_datetime += datetime.timedelta(days=1)

def process():
    # Define the path to the result file
    result_file = "result.csv"
    
    # Create an empty DataFrame to store the result
    result_df = pd.DataFrame()
    
    # Iterate over each file in the data directory
    for root, dirs, files in os.walk("data"):
        for file in files:
            # Check if the file is a CSV file
            if file.endswith(".csv"):
                # Define the path to the current file
                file_path = os.path.join(root, file)
                
                # Read the CSV file into a DataFrame
                csv_df = pd.read_csv(file_path, sep=";")

                csv_df["datetime_debut"] = pd.to_datetime(csv_df["date_debut"])
                csv_df["datetime_fin"] = pd.to_datetime(csv_df["date_fin"])

                # Convert the date_debut and date_fin columns to datetime objects
                csv_df["date_debut"] = pd.to_datetime(csv_df["date_debut"])
                csv_df["date_fin"] = pd.to_datetime(csv_df["date_fin"])

                # Split the date_debut and date_fin into date and heure
                csv_df[['date_debut', 'heure_debut']] = csv_df['date_debut'].dt.strftime('%Y-%m-%d %H:%M:%S').str.split(' ', expand=True)
                csv_df[['date_fin', 'heure_fin']] = csv_df['date_fin'].dt.strftime('%Y-%m-%d %H:%M:%S').str.split(' ', expand=True)

                # Drop duplicate rows based on all columns
                csv_df = csv_df.drop_duplicates()

                # Filter the rows where nom_poll is equal to "NO2" or "PM10" and statut_valid is True
                filtered_df = csv_df[(csv_df["nom_poll"].isin(["NO2", "PM10"])) & (csv_df["statut_valid"] == True)]

                # Append the filtered DataFrame to the result DataFrame
                result_df = pd.concat([result_df, filtered_df])
    
    # Write the result DataFrame to the result file
    result_df.to_csv(result_file, index=False, sep=";")
    
    print("First data lines have been successfully stored in the result file:", result_file)

def main():
    # Download the data
    download("2023-01-01", "2024-03-31")

    # Read the result file into a pandas DataFrame
    result_df = pd.read_csv("result.csv", sep=";")

    process()
    
    # Generate the graphs
    #graph_date()
    st.title('Uber pickups in NYC')
    st.markdown('This is a Streamlit dashboard that shows Uber pickups in NYC.')

    # Using "with" notation
    with st.sidebar:
        add_radio = st.radio(
            "Choose a shipping method",
            ("Standard (5-15 days)", "Express (2-5 days)")
        )

    map_data = pd.read_csv("result.csv", sep=";")
    
    # Get the unique dates from the date_debut column
    available_dates = result_df["date_debut"].unique()
    # Convert the dates to datetime objects
    available_dates = pd.to_datetime(available_dates)
    # Set the minimum and maximum dates
    d = st.date_input("Date", min_value=available_dates.min().date(), max_value=available_dates.max().date(), value=available_dates.min().date())

    t = st.time_input("Heure", datetime.time(), step=3600)

    st.map(map_data, size=1000, latitude="y_wgs84", longitude="x_wgs84")

if __name__ == "__main__":
    main()