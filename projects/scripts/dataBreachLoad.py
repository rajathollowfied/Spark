import logging, json
from pyspark.sql import SparkSession

logging.basicConfig(filename='logs\dataBreachLoad.log', level=logging.INFO, \
    format='%(asctime)s - %(levelname)s - %(message)s')

def dbConfigLoad():
    with open('D:\Learning\GIT\workspace\projects\configs\postgresConfig.json','r') as file:
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
    logging.info(f"JDBC URI - {url}")
    return url, properties
    

def dbWrite(dataframe):
    url, properties = dbConnection()
    try:
        dataframe.write.jdbc(url,"public.dataBreach",mode="overwrite",properties=properties)
        logging.info(f"Data written successfully!")
        
    except Exception as e:
        logging.info(f"Write activity failed! {e}")

def createSpark():
    spark = SparkSession.builder.appName("dataBreachLoad").\
        config("spark.jars","C:\spark\spark-3.3.2-bin-hadoop3\jars\postgresql-42.7.4.jar").master("local[*]").getOrCreate()
    return spark

def endSpark(spark):
    spark.stop()

def readCSV():
    spark = createSpark()
    df = spark.read.option("multiline",True).option("quote", "\"").option("escape", "\"").csv('D:\Learning\GIT\workspace\datasets\dataBreaches.csv',inferSchema=True,header=True)
    columnRenameMap = {
        "year   ": "yr",
        "ID": "s_id",
        "alternative name": "alternative_name",
        "records lost": "records_lost",
        "interesting story": "interesting_story",
        "data sensitivity": "data_sensitivity",
        "displayed records": "displayed_records",
        "source name": "source_name",
        "1st source link": "first_source_link",
        "2nd source link": "second_source_link"    
    }
    
    for old_name, new_name in columnRenameMap.items():
        df = df.withColumnRenamed(old_name,new_name).drop("_c11")
    
    dbWrite(df)
    endSpark(spark)
    

def main():
    readCSV()
        
if __name__ == '__main__':
    main()