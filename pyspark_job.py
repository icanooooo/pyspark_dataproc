from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, lit
from helper.pyspark_helper import changeCurrency_udf

spark = SparkSession.builder.appName('amazon sales project').getOrCreate() #Cari tau ini apa/ngaipain

file_path = 'assets/amazon_sales.csv'
df = spark.read.csv(file_path, header=True, inferSchema=True)

df = df.withColumn("Date", to_date("Date", "MM-dd-yy"))

df_cancelled = df[df['Status'] == 'Cancelled']

df = df[df['Status'] != 'Cancelled']

df = df.withColumn("Amount_in_usd", changeCurrency_udf(df["Amount"], df["currency"], lit("USD")))

df = df.drop("Unnamed: 22")

df.show()