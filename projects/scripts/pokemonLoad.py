import logging,json
from pyspark.sql import SparkSession

logging.basicConfig(filename='logs\pokemonLoad.log',level=logging.INFO\
    ,format='%(asctime)s - %(levelname)s - %(message)s')

def createSpark():
    spark = SparkSession.builder.config("spark.jars','C:\spark\spark-3.3.2-bin-hadoop3\jars\postgresql-42.7.4.jar")\
        .appName("pokemon").master("local[*]").getOrCreate()
    return spark
    
def readCSV(spark):
    
    df = spark.read.option("header",True).format('csv').load('D:\Learning\GIT\workspace\datasets\Pokemon.csv')
    df = df.withColumnRenamed('#','id').withColumnRenamed('Type 1','type1')\
        .withColumnRenamed('Type 2','type2').withColumnRenamed('Sp. Atk','spatk').withColumnRenamed('Sp. Def','spdef')
    return df

def writeCSV(spark):
    try:
        df = readCSV(spark)
        dbURI, config = sqlConn()
        df.write.jdbc(dbURI,"dbt.pokemon",mode="overwrite",properties=config)
        logging.info(f"Data written!")
    except Exception as e:
        logging.info(f"Write not working! {e}")

def sqlConn():
    try:
        with open('D:\Learning\GIT\workspace\projects\configs\postgresConfig.json','r') as file:
            dbConfig = json.load(file)
        config = {
            'driver': 'org.postgresql.Driver',
            'user': dbConfig['database']['DB_USER'],
            'password': dbConfig['database']['DB_PASSWORD'],
            'host': dbConfig['database']['DB_HOST'],
            'port': dbConfig['database']['DB_PORT'],
            'name': dbConfig['database']['DB_NAME']
        }
        dbURI = f"jdbc:postgresql://{config['host']}:{config['port']}/{config['name']}"
        
        logging.info(f"Connection built!")
        
    except Exception as e:
        logging.info(f"Connection not built!{e}")
    return(dbURI,config)

def main():
    spark = createSpark()
    writeCSV(spark)
    spark.stop()

if __name__=='__main__':
    main()