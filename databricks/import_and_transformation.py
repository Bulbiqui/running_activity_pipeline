from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, round
import re
import os

spark = SparkSession.builder \
    .appName("Import_and_transformation") \
    .getOrCreate()

# ------------------------ Accès au bucket S3 ------------------------

# Définition des crédentials
aws_access_key = "" # AccessKey à rajouter
aws_secret_key = "" # SecretKey à rajouter
encoded_secret_key = aws_secret_key.replace("/", "%2F")

# Variabilisations
s3_bucket = "running-activity-aog"
mount_name = "/mnt/running-activity-aog"

# Montage du S3 bucket pour y avoir accès (si le montage n'existe pas)
mounts = [mount.mountPoint for mount in dbutils.fs.mounts()]

if mount_name not in mounts:
    dbutils.fs.mount(
    source = f"s3a://{aws_access_key}:{encoded_secret_key}@{s3_bucket}",
    mount_point = mount_name,
    extra_configs = {"fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem"}
    )

# ------------------------ Importation du dernier fichier seulement ------------------------

# Liste des fichiers dans le S3 avec le prefix voulu
prefix_fichiers = "my_activity_data"
fichiers = [file for file in dbutils.fs.ls(mount_name) if file.name.startswith(prefix_fichiers)]

# boucle for afin de récupérer le fichier le plus récent
dernier_timestamp = None
pattern_timestamp = re.compile(r'\d{14}') #regex pour le timestamp

for file in fichiers:
    nom_fichier = file.name
    match = pattern_timestamp.search(nom_fichier)
    if match:
        fichier_timestamp = match.group()
        if dernier_timestamp is None or fichier_timestamp > dernier_timestamp:
            dernier_fichier = file.path
            dernier_timestamp = fichier_timestamp

# Lire le fichier CSV le plus récent depuis le mount point
activity_data_df = spark.read.csv(dernier_fichier, header=True, inferSchema=True)

# ------------------------ Sélection, transformation et création de données ------------------------

# Sélection des variables à garder
liste_variables = ['name','distance','moving_time','elapsed_time', \
    'total_elevation_gain','start_date','kudos_count','gear_id', \
    'start_latlng','end_latlng','average_speed', 'max_speed', 'average_cadence', \
    'average_heartrate', 'max_heartrate', 'suffer_score']

activity_data_selected_df = activity_data_df.select(liste_variables)

# Renommage des champs
activity_data_renamed_df = activity_data_selected_df.withColumnRenamed("distance", "distance_m") \
    .withColumnRenamed("moving_time", "moving_time_sec") \
    .withColumnRenamed("elapsed_time", "elapsed_time_sec") \
    .withColumnRenamed("total_elevation_gain", "total_elevation_gain_m") \
    .withColumnRenamed("average_speed", "average_speed_m_sec") \
    .withColumnRenamed("max_speed", "max_speed_m_sec")

# Création de nouvelles métriques pour certaines variables (m en km, m_sec en km_h)

activity_data_transformed_df = activity_data_renamed_df.withColumn("distance_km", round(col("distance_m") / 1000, 2)) \
    .withColumn("moving_time_min", round(col("moving_time_sec") / 60, 2)) \
    .withColumn("elapsed_time_min", round(col("elapsed_time_sec") / 60, 2)) \
    .withColumn("average_speed_km", round(col("average_speed_m_sec") * 3.6, 2)) \
    .withColumn("max_speed_km", round(col("max_speed_m_sec") * 3.6, 2)) \
    .withColumn("average_cadence_reelle", round(col("average_cadence") * 2, 0))

# Création de nouvelles variables (allure)

activity_data_created_df = activity_data_transformed_df.withColumn("allure_min_km", round(60 / col("average_speed_km"), 2))

# ------------------------ Test des tables temporaires   ------------------------
'''

# Création de la table temporaire à partir du dataframe
activity_data_created_df.createOrReplaceTempView("activity_data")

# Requete SQL sur la table temporaire
spark.sql("SELECT name, start_date, distance_km, elapsed_time_min FROM activity_data").show()


+--------------------+-------------------+-----------+----------------+
|                name|         start_date|distance_km|elapsed_time_min|
+--------------------+-------------------+-----------+----------------+
|12km - Cayeux-sur...|2024-07-04 07:19:42|       12.2|           65.18|
|8km - Cayeux-sur-...|2024-07-03 15:47:14|       8.05|            42.9|
|10km - Vallée de ...|2024-07-02 15:20:44|      10.13|           54.87|
|10km - Saleux / S...|2024-07-02 07:07:25|      10.07|           53.35|
|16km - Lille / Ci...|2024-06-30 08:33:43|       16.1|           88.03|
|10km - Saint-Maur...|2024-06-29 08:02:20|      10.09|            53.1|
|10km - Lille / Ci...|2024-06-27 16:28:50|      10.24|           60.78|
|7km - Saint-Mauri...|2024-06-25 17:16:37|       7.07|           37.67|
|7km - Saint-Mauri...|2024-06-25 04:32:55|       7.25|           38.53|
|14km - Saleux / P...|2024-06-23 07:19:01|      14.12|           75.35|
|         45' - Lille|2024-06-20 16:32:49|       8.56|           47.72|
|Semi-marathon de ...|2024-06-16 07:55:12|      21.21|            86.4|
|25' - Activation ...|2024-06-15 09:51:50|       4.84|            25.6|
|30' - Saint-Mauri...|2024-06-14 04:26:32|       5.51|           30.85|
|30' - Saint-Mauri...|2024-06-11 17:49:23|       6.07|           33.18|
|14km - Lille / Deûle|2024-06-09 06:48:02|      14.74|           74.72|
|6km - Saint-Mauri...|2024-06-08 15:50:45|       6.44|            34.7|
|40' - Saint-Mauri...|2024-06-06 16:56:20|       7.47|           45.85|
|17km - Séance tes...|2024-06-05 15:37:41|      17.06|           83.28|
|45' - Saint-Mauri...|2024-06-04 16:46:31|       8.34|           47.35|
+--------------------+-------------------+-----------+----------------+
only showing top 20 rows

'''
# ------------------------ Export du fichier dans le S3 transformed ------------------------


# ------------------------------------------------------------------------------------------