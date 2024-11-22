from pyspark.sql.types import DoubleType
from currency_converter import CurrencyConverter
from pyspark.sql.functions import udf

def changeCurrency(amount, inCurrency, outCurrency):
    try:
        c = CurrencyConverter()

        amount_in_usd = c.convert(amount, inCurrency, outCurrency)
        return amount_in_usd
    except:
        return None

changeCurrency_udf = udf(changeCurrency, DoubleType())