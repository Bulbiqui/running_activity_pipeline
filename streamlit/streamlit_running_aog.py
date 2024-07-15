# Importation des librairies nécessaires
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import boto3
from io import StringIO
import aws_credentials as creds # importation des credentials du s3

# -------------------- Récupération du fichier csv transformé -------------------------
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

folder_name = 'transformed_activity_data_final/' 
file_key = get_csv_filename_from_bucket(creds.AWS_BUCKET_NAME, folder_name)
data = read_csv_from_s3(creds.AWS_BUCKET_NAME, file_key)

data['start_date_formatted'] = pd.to_datetime(data['start_date'])

# ----------------------------------------------------------------------------------------

st.title('Application de suivi running :man-running:')

st.subheader("Aperçu des données chargées :")
st.write(data.head())

st.subheader('Filtre par dates')
col1, col2 = st.columns(2)
with col1:
    start_date = st.date_input('Date de début', data['start_date_formatted'].min().date())
with col2:   
    end_date = st.date_input('Date de fin', data['start_date_formatted'].max().date())

start_date = pd.to_datetime(start_date).tz_localize('UTC')
end_date = pd.to_datetime(end_date).tz_localize('UTC')
filtered_data = data[(data['start_date_formatted'] >= pd.to_datetime(start_date)) & (data['start_date_formatted'] <= pd.to_datetime(end_date))]

# Sélectionner une course spécifique
filtered_data['date_nom_course'] = filtered_data['start_date_formatted'].dt.strftime('%Y-%m-%d') + ' - ' + filtered_data['name']
course_names = filtered_data['date_nom_course'].unique()

selected_course = st.selectbox(
    "Sélectionnez une course :man-running: ", 
    course_names,
    index = None,
    placeholder = "Sélectionnez une course"
    )

course_data = filtered_data[filtered_data['date_nom_course'] == selected_course]