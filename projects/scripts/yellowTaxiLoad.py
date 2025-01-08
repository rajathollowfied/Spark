from pyspark.sql import SparkSession
from pyspark.sql.functions import when
import json
import logging

logging.basicConfig(filename='logs\yellowTaxiLoad.log',level=logging.INFO,\
    format='%(asctime)s - %(levelname)s - %(message)s')

def createSpark():
    try:
        spark = SparkSession.builder.config("spark.jars","C:\spark\spark-3.3.2-bin-hadoop3\jars\postgresql-42.7.4.jar")\
        .appName("yellowTaxi").master("local[*]").getOrCreate()
        logging.info("SUCCESS - Spark got initialized.")
        
        return spark
        
    except Exception as e:
        logging.info(f"ERROR - Spark not initialized. - {e}")
        
def sqlConnection():
    try:
        with open('D:\Learning\GIT\workspace\projects\configs\postgresConfig.json','r') as file:
            dbConfig = json.load(file)
            logging.info("SUCCESS - db config found.")
            
            properties = {
            'host' : dbConfig["database"]["DB_HOST"],
            'user' : dbConfig["database"]["DB_USER"],
            'password' : dbConfig["database"]["DB_PASSWORD"],
            'port' : dbConfig["database"]["DB_PORT"],
            'name' : dbConfig["database"]["DB_NAME"],
            "driver" : "org.postgresql.Driver"
            }
            
            URI = f"JDBC:postgres://{properties['host']}:{properties['port']}/{properties['name']}"
            logging.info("SUCCESS - URI configured!")
                
    except Exception as e:
            logging.info(f"ERROR - issue with finding dbconfig file. JDBC URI not configured. - {e}")
            
    return URI

def loadParquet(spark):
    try:
        df = spark.read.format('parquet')\
            .load('D:\Learning\GIT\workspace\datasets\yellow_tripdata_2022-01.parquet')
            
        dfFinal = df.withColumn("Vendor",\
            when(df.VendorID==1,"Creative Mobile Technologies, LLC").
            when(df.VendorID==2,"VeriFone Inc.").
            otherwise("Others")
            ).withColumn("RateCode",\
                when(df.RatecodeID==1,"Standard rate").
                when(df.RatecodeID==1,"JFK").
                when(df.RatecodeID==1,"Newark").
                when(df.RatecodeID==1,"Nassau or Westchester").
                when(df.RatecodeID==1,"Negotiated fare").
                when(df.RatecodeID==1,"Group ride").
                otherwise("Others")            
            ).withColumn("Payment_type",\
                when(df.payment_type==1,"Credit Card").
                when(df.payment_type==1,"Cash").
                when(df.payment_type==1,"No Charge").
                when(df.payment_type==1,"Dispute").
                when(df.payment_type==1,"Uknown").
                when(df.payment_type==1,"Voided trip").
                otherwise("Others")            
            )
        logging.info("SUCCESS - File loaded and mapping added!")
    
    except Exception as e:
        logging.info(f"ERROR - Parquet file not loaded - {e}") 

def writeToDB():
    pass

def main():
    spark = createSpark()
    loadParquet(spark)        
    spark.stop()
    
if __name__=='__main__':
    main()