import logging, json
from pyspark.sql import SparkSession

logging.basicConfig(filename='logs\dataBreachLoad.log', level=logging.INFO, \
    format='%(asctime)s - %(levelname)s - %(message)s')

def dbConfigLoad():
    with open('projects/configs/postgresConfig.json','r') as file:
        dbConfig = json.load(file)
    return dbConfig

def dbConnection():
    dbConfig = dbConfigLoad()
    
    properties = {
        "host" : dbConfig['database']['DB_HOST'],
        "user" : dbConfig['database']['DB_USER'],
        "port" : dbConfig['database']['DB_PORT'],
        "name" : dbConfig['database']['DB_NAME'],
        "password" : dbConfig['database']['DB_PASSWORD'],
        "driver" : "org.postgresql.Driver"
    }
    
    url = f"jdbc:postgresql://{properties['host']}:{properties['port']}/{properties['name']}"
    
    return url, properties
    

def dbWrite():
    
    pass

def createSpark():
    spark = SparkSession.builder.appName("dataBreachLoad").master("local[*]").getOrCreate()
    return spark

def readCSV():
    
    spark = createSpark()
    df = spark.read.csv('D:\Learning\GIT\workspace\datasets\dataBreaches.csv',inferSchema=True,header=True)
    df = df.withColumnRenamed('year   ','year')
    

def main():
    dbConnection()
        
        
if __name__ == '__main__':
    main()