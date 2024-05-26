import datetime
import os
import requests
import csv
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
import numpy as np
import time
import plotly.express as px


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

def download_sirene_data():
    # Send a GET request to download the SIRENE data
    response = requests.get("https://data.paysdelaloire.fr/api/explore/v2.1/catalog/datasets/120027016_base-sirene-v3-ss/exports/csv")
    # Check if the request was successful
    if response.status_code == 200:
        # Define the path to save the downloaded file
        sirene_file = "data/sirene_data.csv"
        # Save the response content to the file
        with open(sirene_file, "wb") as file:
            file.write(response.content)
        print("SIRENE Data downloaded successfully")
    else:
        print("Failed to download SIRENE data")

def process():
    # Define the path to the result file
    result_file = "result.csv"
    
    # Create an empty DataFrame to store the result
    result_df = pd.DataFrame()
    
    # Iterate over each file in the data directory
    for root, dirs, files in os.walk("data"):
        for file in files:
            # Check if the file is a CSV file
            if file.endswith(".csv") and file.startswith("file_"):
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

                # Remove rows where the month of datetime_debut and datetime_fin are not the same
                csv_df = csv_df[csv_df['datetime_debut'].dt.month == csv_df['datetime_fin'].dt.month]

                # Extract the year and quarter from the date_debut column
                csv_df["date_debut"] = pd.to_datetime(csv_df["date_debut"])
                csv_df["year"] = csv_df["date_debut"].dt.year
                csv_df["quarter"] = csv_df["date_debut"].dt.quarter
                # Combine the year and quarter columns into a single column
                csv_df["year_quarter"] = csv_df["year"].astype(str) + "-" + csv_df["quarter"].astype(str)

                # Filter the rows where nom_poll is equal to "NO2" or "PM10" and statut_valid is True
                filtered_df = csv_df[csv_df["nom_poll"].isin(["NO2", "PM10"])]
                filtered_df = filtered_df[filtered_df["statut_valid"] == True]
                #filtered_df = csv_df[(csv_df["nom_poll"].isin(["NO2", "PM10"])) & (csv_df["statut_valid"] == True)]

                # Append the filtered DataFrame to the result DataFrame
                result_df = pd.concat([result_df, filtered_df], ignore_index=True)
                print("Processed file:", file_path, "- Number of lines added:", len(filtered_df))
    
    # Write the result DataFrame to the result file
    result_df.to_csv(result_file, index=False, sep=";")
    
    print("First data lines have been successfully stored in the result file:", result_file)

    # Group the data by year_quarter
    grouped_df = result_df.groupby("year_quarter")

    # Iterate over each group
    for group_name, group_df in grouped_df:
        # Define the path to the output file for the current group
        os.makedirs("results/", exist_ok=True)
        output_file = f"results/result_{group_name}.csv"
        
        # Write the group DataFrame to the output file
        group_df.to_csv(output_file, index=False, sep=";")
        
        print(f"Data for {group_name} has been successfully stored in the output file:", output_file)
 
# Generates a Streamlit treemap for emission by department and city.

def process_sirene_data():
    # Read the SIRENE data file into a pandas DataFrame
    sirene_df = pd.read_csv("data/sirene_data.csv", delimiter=";")
    
    # Group the data by city and section of establishment, count the occurrences
    
    section_counts = sirene_df.groupby(["libellecommuneetablissement", "sectionetablissement"]).size().reset_index(name="count")
    
    # Pivot the table to have cities as rows and sections as columns
    pivot_table = section_counts.pivot(index="libellecommuneetablissement", columns="sectionetablissement", values="count")
    
    # Save the pivot table to a new file
    pivot_table.to_csv("results/section_distribution_by_city.csv")
    
    print("Section distribution by city file created successfully")

def plot_section_distribution_by_city():
    # Read the section distribution by city file into a pandas DataFrame
    section_df = pd.read_csv("results/section_distribution_by_city.csv")
    
    # Filter for Nantes city
    nantes_df = section_df[section_df["libellecommuneetablissement"] == "NANTES"]

    # Plot the distribution of sections by city
    st.subheader("Distribution des Sections d'Entreprises à Nantes")
    st.write("Ce graphique montre la répartition des sections d'entreprises à Nantes.")
    # Plot the distribution of establishments by city
    fig = px.bar(nantes_df, x="libellecommuneetablissement", y=nantes_df.columns[1:], title="Distribution des établissements par type à Nantes")
    st.plotly_chart(fig)
    
def treemap_emissions(df):
    df_filtered = df

    # Group by department, city, and pollutant, sum emissions
    emissions_by_dept_city_poll = (
        df_filtered.groupby(["nom_dept", "nom_com"])["valeur"]
        .sum()
        .reset_index()
        .rename(columns={"valeur": "Emission totale"})
    )

    # Create the treemap with Streamlit
    st.subheader("Treemap: Émissions par Département, Ville et Polluant (µg/m³)")
    st.write("Ce treemap montre l'émission totale pour chaque département, ville et polluant.")

    st.plotly_chart(
        figure_or_data=px.treemap(
            emissions_by_dept_city_poll, path=["nom_dept", "nom_com"], values="Emission totale", color="Emission totale"
        )
    )

# Generates a Streamlit sector chart for emission by influence.
def sector_chart_emissions(df):
    # Filter for NO2 and PM10
    df_filtered = df[(df["nom_poll"].isin(["NO2", "PM10"])) & (df["statut_valid"] == True)]

    # Count occurrences of each influence
    influence_counts = df_filtered["influence"].value_counts().reset_index(name="count")

    # Create the sector chart with Streamlit
    st.subheader("Émission par Influence")
    st.write("Ce graphique montre la répartition des émissions selon différentes influences.")

    st.plotly_chart(
        figure_or_data=px.pie(
            influence_counts, values="count", names="influence", title="Distribution des émissions par influence"
        )
    )

def plot_daily_average_emissions(df):
    # Convertir les colonnes de date et d'heure en datetime
    df['datetime_debut'] = pd.to_datetime(df['datetime_debut'])
    df['datetime_fin'] = pd.to_datetime(df['datetime_fin'])

    # Group by hour and calculate the mean emissions for each hour
    daily_avg_emissions = df.groupby(df['datetime_debut'].dt.hour)['valeur'].mean()

    # Plot the daily average emissions
    st.subheader("Courbe : Moyenne journalière d'émissions de NO2 et de PM10")
    st.write("Cette courbe montre l'évolution moyenne des émissions de NO2 et de PM10 tout au long d'une journée.")

    st.line_chart(daily_avg_emissions)

def plot_monthly_average_emissions(df):
    # Convertir les colonnes de date et d'heure en datetime
    df['datetime_debut'] = pd.to_datetime(df['datetime_debut'])
    df['datetime_fin'] = pd.to_datetime(df['datetime_fin'])

    # Group by day and calculate the mean emissions for each day
    monthly_avg_emissions = df.groupby(df['datetime_debut'].dt.day)['valeur'].mean()

    # Plot the monthly average emissions
    st.subheader("Courbe : Moyenne mensuelle d'émissions de NO2 et de PM10")
    st.write("Cette courbe montre l'évolution moyenne des émissions de NO2 et de PM10 pour chaque jour du mois.")

    st.line_chart(monthly_avg_emissions)

def main():
    st.set_page_config(
        page_title="Tableau de bord - AirPL", 
        page_icon="🔍", 
        layout="wide"
    )
    st.title("Tableau de bord Pollution de l'air en Pays de la Loire")
    st.caption('Visualisation des données de pollution en Pays de la Loire. Utilisez les filtres dans le menu de gauche pour afficher les données souhaitées.')
    st.caption('Les données sont issues de la plateforme de l\'AIRPL (https://data.airpl.org/).')
    st.divider()

    result_file = "result.csv"
    data_exists = os.path.exists(result_file)

    if not data_exists:
        if st.button("Télécharger et traiter les données"):
            with st.status("Téléchargement et traitement des données", expanded=True) as status:
                st.write("Téléchargement des données..")
                download("2023-08-01", "2024-03-31")
                st.write("Téléchargement des données SIRENE..")
                download_sirene_data()
                st.write("Traitement des données..")
                process()
                st.write("Traitement des données SIRENE..")
                process_sirene_data()
                status.update(label="Téléchargement et traitement terminé", state="complete", expanded=False)
            data_exists = os.path.exists(result_file)
    if data_exists:
        # Read the result file into a pandas DataFrame
        result_df = pd.read_csv("result.csv", sep=";")
        # Using "with" notation
        with st.sidebar:
            # Get the unique year_quarter values from the result DataFrame
            unique_year_quarters = result_df["year_quarter"].unique()
            st.title("Filtres")
            st.divider()
            # Add a radio button to select the year_quarter
            selected_year_quarter = st.selectbox(
                "Choisir un trimestre",
                unique_year_quarters
            )
            quarter_df = pd.read_csv("results/result_"+selected_year_quarter+".csv", sep=";")
            unique_department = quarter_df["nom_dept"].unique()
            selected_department = st.selectbox(
                "Choisir un département",
                unique_department,
                None
            )
            if not selected_department:
                unique_cities = quarter_df["nom_com"].unique()
            else:
                unique_cities = quarter_df[quarter_df["nom_dept"] == selected_department]["nom_com"].unique()

            # Add a selectbox to choose a city
            selected_city = st.selectbox(
                "Choisir une ville",
                unique_cities,
                None
            )
            unique_polluant = quarter_df["nom_poll"].unique()
            selected_polluant = st.selectbox(
                "Choisir un polluant",
                unique_polluant
            )

            # Filter the DataFrame based on the selected polluant
            filtered_df = quarter_df[quarter_df["year_quarter"] == selected_year_quarter]
            if selected_department:
                filtered_df = filtered_df[filtered_df["nom_dept"] == selected_department]
            if selected_city:
                filtered_df = filtered_df[filtered_df["nom_com"] == selected_city]
            filtered_df = filtered_df[filtered_df["nom_poll"] == selected_polluant]

            st.divider()

            # Display the filtered DataFrame
            st.write(filtered_df)

            # Calculate the previous quarter
            previous_quarter = selected_year_quarter.split("-")
            previous_quarter[1] = str(int(previous_quarter[1]) - 1)
            if previous_quarter[1] == "0":
                previous_quarter[0] = str(int(previous_quarter[0]) - 1)
                previous_quarter[1] = "4"
            previous_quarter = "-".join(previous_quarter)

            if previous_quarter in unique_year_quarters:
                st.write("Trimestre précédent:", previous_quarter)
            else:
                previous_quarter = None
                st.write("Trimestre précédent non disponible")

        # Calculate the average of the values in filtered_df
        average_value = filtered_df["valeur"].mean().round(2)

        # Display the average value
        col1, col2, col3 = st.columns(3)
        if previous_quarter:
            previous_quarter_df = pd.read_csv("results/result_"+previous_quarter+".csv", sep=";")
            previous_quarter_df = previous_quarter_df[previous_quarter_df["nom_poll"] == selected_polluant]
            previous_average_value = previous_quarter_df["valeur"].mean().round(2)
            txtMoyenne = f"{(average_value - previous_average_value).round(2)} {filtered_df['unite'].unique()[0]}"
            previous_min = previous_quarter_df["valeur"].min()
            txtMin = f"{(filtered_df['valeur'].min() - previous_min).round(2)} {filtered_df['unite'].unique()[0]}"
            previous_max = previous_quarter_df["valeur"].max()
            txtMax = f"{(filtered_df['valeur'].max() - previous_max).round(2)} {filtered_df['unite'].unique()[0]}"
        else:
            txtMoyenne = None
            txtMin = None
            txtMax = None
        col1.metric("Minimum", f"{filtered_df['valeur'].min()} {filtered_df['unite'].unique()[0]}", txtMin)
        col2.metric("Moyenne", f"{average_value} {filtered_df['unite'].unique()[0]}", txtMoyenne)
        col3.metric("Maximum", f"{filtered_df['valeur'].max()} {filtered_df['unite'].unique()[0]}", txtMax)

        sector_chart_emissions(quarter_df if quarter_df.empty else filtered_df)
        treemap_emissions(quarter_df if quarter_df.empty else filtered_df)

        plot_daily_average_emissions(quarter_df if quarter_df.empty else filtered_df)
        plot_monthly_average_emissions(quarter_df if quarter_df.empty else filtered_df)

        # Call the function in the main() function
        plot_section_distribution_by_city()

        left_column, right_column = st.columns(2)
        # You can use a column just like st.sidebar:
        with left_column:
            st.map(filtered_df, size=1000, latitude="y_wgs84", longitude="x_wgs84")

        # Or even better, call Streamlit functions inside a "with" block:
        with right_column:
            st.line_chart(filtered_df, x="datetime_debut", y="valeur")

if __name__ == "__main__":
    main()
    #process()