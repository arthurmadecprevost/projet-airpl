import datetime
import os
import requests
import csv

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
                print("Erreur lors de la récupération des données pour la date", current_date, ":", response.status_code)
        else:
            print("File already exists for date", current_date, "- Skipping download")
        # Move to the next day
        current_datetime += datetime.timedelta(days=1)

def process():
    # Define the path to the result file
    result_file = "result.csv"
    # Open the result file in write mode
    with open(result_file, "w", newline="", encoding='utf-8') as result_csv:
        # Create a CSV writer object for the result file
        result_writer = csv.writer(result_csv)
        # Iterate over each file in the data directory
        for root, dirs, files in os.walk("data"):
            for file in files:
                # Check if the file is a CSV file
                if file.endswith(".csv"):
                    # Define the path to the current file
                    file_path = os.path.join(root, file)
                    # Open the current file in read mode
                    with open(file_path, "r") as csv_file:
                        # Create a CSV reader object for the current file
                        csv_reader = csv.reader(csv_file)
                        # Skip the header line
                        next(csv_reader)
                        # Read the first data line
                        first_data_line = next(csv_reader)
                        # Write the first data line to the result file
                        result_writer.writerow(first_data_line)
    print("First data lines have been successfully stored in the result file:", result_file)

# Define the start and end dates
start_date = "2022-12-15"
end_date = "2024-01-01"
download(start_date, end_date)
process()