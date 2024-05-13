import datetime
import os
import requests
import csv

# Define the start and end dates
start_date = "2024-01-01"
end_date = "2024-05-13"

# Convert the dates to datetime objects
start_datetime = datetime.datetime.strptime(start_date, "%Y-%m-%d")
end_datetime = datetime.datetime.strptime(end_date, "%Y-%m-%d")

# Iterate over each day between the start and end dates
current_datetime = start_datetime
while current_datetime <= end_datetime:
    # Format the current date as a string
    current_date = current_datetime.strftime("%Y-%m-%d")
    print(current_date)

    # Define the URL for the current day
    url = f"https://data.airpl.org/api/v1/mesure/horaire/?&date_heure_tu__range={current_date},{current_date}%2023:00:00&code_configuration_de_mesure__code_point_de_prelevement__code_station__code_commune__code_departement__in=44,49,53,72,85,&export=csv"

    # Send a GET request to the URL
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        data = response.content
        
        # Create the year and month folders if they don't exist
        year_folder = current_datetime.strftime("%Y")
        month_folder = current_datetime.strftime("%m")
        year_path = f"data/{year_folder}"
        month_path = f"data/{year_folder}/{month_folder}"
        os.makedirs(year_path, exist_ok=True)
        os.makedirs(month_path, exist_ok=True)

        # Define the path to the CSV file
        csv_file = f"data/{year_folder}/{month_folder}/file_{current_date}.csv"

        # Decode the response content as a string
        data_str = data.decode("utf-8")
        # Split the data into lines
        lines = data_str.splitlines()
        # Create a CSV writer object
        csv_writer = csv.writer(open(csv_file, "w", newline=""))
        # Write the lines to the CSV file
        for line in lines:
            csv_writer.writerow(line.split(","))
        print("Data has been successfully stored in the CSV file:", csv_file)
    else:
        print("Erreur lors de la récupération des données pour la date", current_date, ":", response.status_code)

    # Move to the next day
    current_datetime += datetime.timedelta(days=1)