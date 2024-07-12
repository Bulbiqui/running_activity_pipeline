# Importation des librairies nécessaires
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import boto3
from io import StringIO
import aws_credentials as creds # importation des credentials du s3

st.set_option('deprecation.showPyplotGlobalUse', False)

s3_client = boto3.client(
    's3',
    aws_access_key_id = creds.AWS_ACCESS_KEY_ID,
    aws_secret_access_key = creds.AWS_SECRET_ACCESS_KEY
)

def read_csv_from_s3(bucket_name, file_key):
    csv_obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    body = csv_obj['Body'].read().decode('utf-8')
    return pd.read_csv(StringIO(body))

# Le nom du fichier n'est jamais fixe donc récupération de son nom
def get_csv_filename_from_bucket(bucket_name, folder_name):
    response = s3_client.list_objects_v2(Bucket = bucket_name, Prefix = folder_name)
    csv_files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.csv')]

    if len(csv_files) == 1:
        return csv_files[0]
    else:
        raise ValueError("Pas un fichier seulement")


st.title('Visualisation running')

try:
    folder_name = 'transformed_activity_data_final/'
    file_key = get_csv_filename_from_bucket(creds.AWS_BUCKET_NAME, folder_name)
    st.write(f"Fichier chargé : {file_key}")

    df = read_csv_from_s3(creds.AWS_BUCKET_NAME, file_key)
    
    st.write("Aperçu des données chargées :")
    st.write(df.head())

    plt.figure(figsize=(10, 6))
    plt.plot(df['start_date'], df['average_cadence_reelle'], marker='o', linestyle='-', color='b')
    plt.xlabel('Dates')
    plt.ylabel('Cadence')
    plt.title('Cadence au fil du temps')
    plt.xticks(rotation=90)
    st.pyplot()

except Exception as e:
    st.error(f"Erreur lors du chargement du fichier : {e}")
