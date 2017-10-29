from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.window import Window
import pyspark.sql.functions as func


if __name__ == "__main__":

	spark = SparkSession \
	    .builder \
	    .appName("Python Spark SQL basic example") \
	    .config("spark.some.config.option", "some-value") \
	    .getOrCreate();

	dataFrame=spark \
	    .read \
	    .format("csv") \
	    .option("header","true") \
	    .option("mode","DROPMALFORMED") \
	    .load("gdp.csv");


	df_lag = dataFrame.withColumn('prev_year_Value',func.lag(dataFrame['Value']).over(Window.partitionBy(dataFrame["Country Code"]).orderBy([func.col('Country Code').asc()])));
	resultDF = df_lag.withColumn('gdpGrowth',(df_lag['Value'] - df_lag['prev_year_Value']) / df_lag['Value']);


	window = Window.partitionBy(resultDF['Year']).orderBy([func.col('gdpGrowth').desc()]);
	df_ranks = resultDF.select('Country Name','Country Code', 'Year', 'Value','gdpGrowth', func.rank().over(window).alias("rank"));
	df_final = df_ranks.where("rank = 1 and gdpGrowth is not null").select('Country Name','Country Code', 'Year', 'Value','gdpGrowth').orderBy('Year');
	resultFinal=df_final.select('Year','Country Name','Country Code');
	resultFinal.show();
	resultFinal.rdd.map(lambda x: ",".join(map(str, x))).coalesce(1).saveAsTextFile("result_maxGdp.csv");
