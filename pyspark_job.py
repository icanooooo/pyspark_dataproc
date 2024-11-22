from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, udf, lit
from pyspark.sql.types import DoubleType
from currency_converter import CurrencyConverter

def changeCurrency(amount, inCurrency, outCurrency):
    try:
        c = CurrencyConverter()

        amount_in_usd = c.convert(amount, inCurrency, outCurrency)
        return amount_in_usd
    except:
        return None

changeCurrency_udf = udf(changeCurrency, DoubleType())


spark = SparkSession.builder.appName('amazon sales project').getOrCreate() #Cari tau ini apa/ngaipain

file_path = 'assets/amazon_sales.csv'
df = spark.read.csv(file_path, header=True, inferSchema=True)

df = df.withColumn("Date", to_date("Date", "MM-dd-yy"))

df_cancelled = df[df['Status'] == 'Cancelled']

df = df[df['Status'] != 'Cancelled']

df = df.withColumn("Amount_in_usd", changeCurrency_udf(df["Amount"], df["currency"], lit("USD")))

df = df.drop("Unnamed: 22")

df.show()