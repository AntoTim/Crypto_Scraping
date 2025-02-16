import os
import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType
import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
from kafka import KafkaProducer, KafkaConsumer
from io import StringIO

chemin_sortie = "/Users/antotim/Desktop/bdf_project/sortie_csv"
os.makedirs(chemin_sortie, exist_ok=True)  # Create the folder if it doesn't exist

chemin_checkpoint = "/Users/antotim/Desktop/bdf_project/checkpoint"
os.makedirs(chemin_checkpoint, exist_ok=True) 

# Define the installation paths for Spark and Hadoop
os.environ['SPARK_HOME'] = '/usr/local/Cellar/apache-spark/3.5.3/libexec'  # Replace with your Spark path
os.environ['HADOOP_HOME'] = '/usr/local/Cellar/hadoop/3.4.0'  # Replace with your Hadoop path
os.environ['PYSPARK_PYTHON'] = 'python3'  # Make sure this points to your Python version

findspark.init()  # Initialize Spark

# Initialize the Spark session with the Kafka package
spark = SparkSession.builder \
    .appName("CryptoPriceScraping") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
    .getOrCreate()

print("Spark version:", spark.version)

headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0.1 Safari/605.1.15',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'fr-FR,fr;q=0.9',
    'Connection': 'keep-alive',
    'Priority': 'u=0, i',
    'Sec-Fetch-Dest': 'iframe',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'cross-site',
    'Cache-Control': 'max-age=300'
}

base_url = "https://www.coingecko.com/fr"

cryptos = []

for i in range(1, 3):  # Loop through the pages
    print('Processing page {0}'.format(i))
    params = {
        'page': i
    }

    # Retrieve the HTML page
    response = requests.get(base_url, headers=headers, params=params)
    soup = BeautifulSoup(response.content, 'html.parser')

    # Extract the main table using pandas
    html_string = str(soup)  # Convert soup to HTML string
    df = pd.read_html(StringIO(html_string))[0]  # Wrap with StringIO

    # Extract the value of the "24 h" column via BeautifulSoup (data-sort attribute)
    rows = soup.find_all('tr')  # Find all rows in the table
    values_1h = []
    values_24h = []
    values_7j = []
    values_30j = []

    for row in rows[1:]:  # Ignore the first row (header)
        cells = row.find_all('td')  # Find all cells in the row
        value_1h = None  # Initialize the variable for the 1 h value

        for cell in cells:  # Loop through each cell to find the <span>
            span = cell.find('span', {'data-attr': 'price_change_percentage_1h'})
            if span:  # If a <span> with the correct data-attr is found
                # Retrieve the data-sort value from the <td> cell
                value_1h = cell['data-sort']  # Get the data-sort value
                break  # Exit the loop since the value is found

        values_1h.append(value_1h)  # Add the found value or None to the list

    for row in rows[1:]:  # Ignore the first row (header)
        cells = row.find_all('td')  # Find all cells in the row
        value_24h = None  # Initialize the variable for the 24 h value

        for cell in cells:  # Loop through each cell to find the <span>
            span = cell.find('span', {'data-attr': 'price_change_percentage_24h'})
            if span:  # If a <span> with the correct data-attr is found
                # Retrieve the data-sort value from the <td> cell
                value_24h = cell['data-sort']  # Get the data-sort value
                break  # Exit the loop since the value is found

        values_24h.append(value_24h)  # Add the found value or None to the list

    for row in rows[1:]:  # Ignore the first row (header)
        cells = row.find_all('td')  # Find all cells in the row
        value_7j = None  # Initialize the variable for the 7 j value

        for cell in cells:  # Loop through each cell to find the <span>
            span = cell.find('span', {'data-attr': 'price_change_percentage_7d'})
            if span:  # If a <span> with the correct data-attr is found
                # Retrieve the data-sort value from the <td> cell
                value_7j = cell['data-sort']  # Get the data-sort value
                break  # Exit the loop since the value is found

        values_7j.append(value_7j)  # Add the found value or None to the list

    for row in rows[1:]:  # Ignore the first row (header)
        cells = row.find_all('td')  # Find all cells in the row
        value_30j = None  # Initialize the variable for the 30 j value

        for cell in cells:  # Loop through each cell to find the <span>
            span = cell.find('span', {'data-attr': 'price_change_percentage_30d'})
            if span:  # If a <span> with the correct data-attr is found
                # Retrieve the data-sort value from the <td> cell
                value_30j = cell['data-sort']  # Get the data-sort value
                break  # Exit the loop since the value is found

        values_30j.append(value_30j)  # Add the found value or None to the list

    # Add this manually extracted "24 h" column to the dataframe
    df['1\xa0h'] = values_1h
    df['24\xa0h'] = values_24h
    df['7\xa0j'] = values_7j
    df['30\xa0j'] = values_30j

    # Add the updated DataFrame to the cryptos list
    cryptos.append(df)

# Test the function
df_data = pd.concat(cryptos, ignore_index=True)

# Drop the first row and unnecessary columns
df_data.drop(index=0, inplace=True)
df_data.drop(columns=['Unnamed: 0', 'Unnamed: 3'], inplace=True)
df_data.drop(df_data.columns[-1], axis=1, inplace=True)

# Rename multiple columns
df_data.rename(columns={
    '1\xa0h': '1 h',
    '24\xa0h': '24 h',
    '7\xa0j': '7 j',
    '30\xa0j': '30 j',
    'Volume sur 24\xa0h': 'Volume sur 24 h'
}, inplace=True)

def convert_to_float(value):
    # Check if the value is None
    if value is None:
        return None  # If it's None, return None directly
    
    if isinstance(value, str):
        # Remove the $ symbol and spaces, replace the comma with a dot
        value = value.replace('$', '').replace(' ', '').replace(',', '.')

    try:
        # Convert the value to float
        result = float(value)
        return result
    except ValueError:
        return None  # Or handle the error as you wish

# Convert the necessary columns to float
columns_to_convert = ['Cours','1 h', '24 h', '7 j', '30 j', 'Volume sur 24 h', 'Capitalisation boursière', 'FDV']
for column in columns_to_convert:
    df_data[column] = df_data[column].apply(convert_to_float)

print("Beginning of DataFrame display")
print(df_data.head())
print(df_data.info())
print("Display of columns")
print(df_data.columns.tolist())
print("Display first row")
print(df_data.iloc[1])
print("End of display")

# Configure the Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def send_to_kafka(data):
    """Sends data to Kafka after converting it to dictionaries."""
    records = data.to_dict(orient='records')
    for item in records:
        #print("Sending to Kafka: ", item)  # Add this for debugging
        producer.send('crypto_prices', value=item)
    producer.flush()
    print("Data sent to Kafka: ", len(records), " records.")

def consume_and_store_messages(max_messages):
    """Consumes messages from a Kafka topic and stores them in a DataFrame."""
    consumer = KafkaConsumer(
        'crypto_prices',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='my-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    data = []
    message_count = 0

    for message in consumer:
        print("Message received: ", message.value)  # Add this for debugging
        data.append(message.value)

        if len(data) >= max_messages:
            break

    df = pd.DataFrame(data)
    print("DataFrame created from consumed messages:")
    print(df.head())
    consumer.close()

# Send updated data to Kafka
send_to_kafka(df_data)

# Execute the function to consume messages
consume_and_store_messages(200)

# Read data from Kafka in streaming mode
df_spark = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "crypto_prices") \
    .option("startingOffsets", "earliest") \
    .load()

# Define the schema to parse the JSON
schema = StructType([
    StructField("#", FloatType(), True),
    StructField("Monnaie", StringType(), True),
    StructField("Cours", FloatType(), True),
    StructField("1 h", FloatType(), True),
    StructField("24 h", FloatType(), True),
    StructField("7 j", FloatType(), True),
    StructField("30 j", FloatType(), True),
    StructField("Volume sur 24 h", FloatType(), True),
    StructField("Capitalisation boursière", FloatType(), True),
    StructField("FDV", FloatType(), True),
    StructField("Capitalisation boursière / FDV", FloatType(), True)
])

# Parse the JSON data from the "value" column
df_json = df_spark.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Write the parsed DataFrame in streaming to a CSV file
query_csv = df_json.writeStream \
    .outputMode("append")  \
    .format("csv") \
    .option("path", chemin_sortie) \
    .option("checkpointLocation", chemin_checkpoint) \
    .start()

# Wait for the termination of the queries
query_csv.awaitTermination()
