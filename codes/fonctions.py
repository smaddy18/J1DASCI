import os
import pandas as pd
import json

def concat_sensors_data(initial_path, numberOfSensors):
    """
    Concatène les données de plusieurs capteurs à partir de fichiers CSV dans les sous-dossiers.

    Args:
    - initial_path (str): Le chemin du dossier initial contenant les sous-dossiers des capteurs.
    - numberOfSensors (int): Le nombre total de capteurs à considérer.

    Returns:
    - DataFrame: Un DataFrame contenant les données concaténées de tous les capteurs.
    """
    # Initialiser une liste pour stocker les DataFrames de chaque capteur
    sensor_dataframes = []

    # Ouvrir le fichier JSON contenant les coordonnées des capteurs et le transformer en dict python
    f = open('../data/sensorCoord.json')
    sensorCoord = json.load(f)
    
    # Parcourir les sous-dossiers pour chaque capteur
    for i in range(251, numberOfSensors + 1):
        sensor_folder = os.path.join(initial_path, f'stars{i}')
        
        # Vérifier si le dossier du capteur existe
        if os.path.exists(sensor_folder):
            # Initialiser une liste pour stocker les DataFrames de chaque fichier
            sensor_files_dataframes = []
            
            # Parcourir les fichiers dans le dossier du capteur
            for file_name in os.listdir(sensor_folder):

                cols = ['UTCDataTime', 'LocalDateTime', 'EnclosureTemp', 'SkyTemp', 'Frequency', 'MSAS', 'ZP']

                if file_name.endswith('.dat'):
                    file_path = os.path.join(sensor_folder, file_name)
                    # Lire le fichier de données et stocker son DataFrame dans la liste
                    df = pd.read_csv(file_path, delimiter=';', names=cols, skiprows=35)
                    df['latitude'] = sensorCoord[f"stars{i}"][0]
                    df['longitude'] = sensorCoord[f"stars{i}"][1]
                    df['sensor'] = f"stars{i}"
                    sensor_files_dataframes.append(df)
            
            # Concaténer les DataFrames de chaque fichier et les ajouter à la liste des DataFrames de capteur
            if sensor_files_dataframes:
                sensor_dataframes.append(pd.concat(sensor_files_dataframes, ignore_index=True))
        else:
            # print(f"Le dossier du capteur stars{i} n'existe pas.")
            numberOfSensors += 1
    
    # Concaténer les DataFrames de chaque capteur en un seul DataFrame
    if sensor_dataframes:
        concatenated_data = pd.concat(sensor_dataframes)
        return concatenated_data
    else:
        print("Aucun fichier de données trouvé pour les capteurs.")
        return None

