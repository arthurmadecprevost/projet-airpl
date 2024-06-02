# Projet AirPL

## Installation des dépendances nécessaires

```
pip install datetime requests pandas streamlit matplotlib numpy plotly
```

## Lancement du tableau de bord

### Cloner le projet
```
git clone git@github.com:arthurmadecprevost/projet-airpl.git
```

### Se positionner dans le dossier du projet
```
cd projet-airpl
```

### Lancez le programme avec Streamlit
```
streamlit run main.py
```

## Informations complémentaires : 

Attention, le téléchargement des données de AirPL et surtout du registre SIRENE (1,7 Go) peuvent prendre du temps. 

Si vous ne souhaitez pas attendre le téléchargement et le processing des données et seulement afficher les données déjà traitées, vous pouvez utiliser le dossier ./results_preprocessed_Q22023-Q12024 et le fichier result_preprocessed_Q22023-Q12024.csv qui contiennent des données AirPL déjà traitées du second trimestre 2023 au premier trimestre 2024. 

Pour les utiliser : 

1. Renommer le dossier "results_preprocessed_Q22023-Q12024.csv" en "results"
2. Renommer le fichier "result_preprocessed_Q22023-Q12024.csv" en "result.csv"
3. Redémarrer l'application (streamlit run main.py)

Si le téléchargement est trop long, merci de nous contacter pour récupérer les fichiers (AirPL, SIRENE) utilisés dans les traitements.