import datetime
import os
import requests
import csv
import pandas as pd
import matplotlib.pyplot as plt

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

                # Convert the date_debut and date_fin columns to datetime objects
                csv_df["date_debut"] = pd.to_datetime(csv_df["date_debut"])
                csv_df["date_fin"] = pd.to_datetime(csv_df["date_fin"])

                # Split the date_debut and date_fin into date and heure
                csv_df[['date_debut', 'heure_debut']] = csv_df['date_debut'].dt.strftime('%Y-%m-%d %H:%M:%S').str.split(' ', expand=True)
                csv_df[['date_fin', 'heure_fin']] = csv_df['date_fin'].dt.strftime('%Y-%m-%d %H:%M:%S').str.split(' ', expand=True)

                # Filter the rows where nom_poll is equal to "NO2" or "PM10" and statut_valid is True
                filtered_df = csv_df[(csv_df["nom_poll"].isin(["NO2", "PM10"])) & (csv_df["statut_valid"] == True)]

                # Append the filtered DataFrame to the result DataFrame
                result_df = pd.concat([result_df, filtered_df])
    
    # Write the result DataFrame to the result file
    result_df.to_csv(result_file, index=False, sep=";")
    
    print("First data lines have been successfully stored in the result file:", result_file)

def process2():
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
                        header = next(csv_reader)
                        # Write the header to the result file
                        result_writer.writerow(header)
                        # Iterate over each row in the CSV file
                        for row in csv_reader:
                            if (row[0].__contains__(";NO2;") or row[0].__contains__(";PM10;")):
                                # Write the row to the result file
                                result_writer.writerow(row)

    # Read the result file into a pandas DataFrame
    result_df = pd.read_csv("result.csv", sep=";")

    # Split the date_debut and date_fin into date and heure
    result_df[['date_debut', 'heure_debut']] = result_df['date_debut'].str.split(' ', expand=True)
    result_df[['date_fin', 'heure_fin']] = result_df['date_fin'].str.split(' ', expand=True)

    # Define the path to the result2 file
    result2_file = "result2.csv"

    # Write the modified DataFrame to the result2 file
    result_df.to_csv(result2_file, index=False, sep=";")

    print("Data has been successfully stored in the result2 file:", result2_file)
    
    print("First data lines have been successfully stored in the result file:", result_file)

def graph_date():
    # Read the result2 file into a pandas DataFrame
    result2_df = pd.read_csv("result2.csv", sep=";")
    
    # Group the data by date_debut, code_station, and nom_poll
    grouped_df = result2_df.groupby(["date_debut", "code_station (ue)", "nom_poll"])
    
    # Iterate over each group
    for group_name, group_data in grouped_df:
        # Extract the date_debut, code_station, and nom_poll from the group name
        date_debut, code_station, nom_poll = group_name
        
        # Create a new figure and axis for the graph
        fig, ax = plt.subplots()
        
        # Plot the values of pollution (field valeur) by hour
        group_data["heure_debut"] = pd.to_datetime(group_data["heure_debut"])
        group_data["valeur"] = pd.to_numeric(group_data["valeur"], errors="coerce")
        group_data.plot(x="heure_debut", y="valeur", ax=ax)
        
        # Set the title of the graph
        ax.set_title(f"Station: {code_station}, Pollutant: {nom_poll}, Date: {date_debut}")
        
        # Set the labels for the x and y axes
        ax.set_xlabel("Hour")
        ax.set_ylabel("Pollution Value")
        
        # Create the graphiques folder if it doesn't exist
        if not os.path.exists('graphiques'):
            os.makedirs('graphiques')
        
        # Save the graph to a file in the graphiques folder
        graph_file = f"graphiques/{code_station}_{nom_poll}_{date_debut}.png"
        plt.savefig(graph_file)
        
        # Close the figure to free up memory
        plt.close(fig)
        
        print("Graph has been successfully generated:", graph_file)

# Define the start and end dates
start_date = "2024-01-01"
end_date = "2024-01-15"
download(start_date, end_date)
process()
#generate_graphs()
graph_date()