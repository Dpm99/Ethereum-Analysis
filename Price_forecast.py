from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as f
from pyspark.sql import types as t
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StandardScaler
from pyspark.ml.regression import LinearRegression
spark = SparkSession.builder.getOrCreate()
prices = spark.read.csv('/data/ethereum/prices', header=True, inferSchema=True)
prices = prices.withColumn('date', f.date_format(prices.Date.cast(dataType=t.TimestampType()), "yyyy-MM-dd"))
#data.show()
x_prices1 = prices["date","24h Open (USD)"].orderBy("date", ascending=False).withColumnRenamed('24h Open (USD)', 'ordered 24h Open (USD)')
x_prices2 = prices["date","24h High (USD)"].orderBy("date", ascending=False).withColumnRenamed('24h High (USD)', 'ordered 24h High (USD)')
x_prices3 = prices["date","24h Low (USD)"].orderBy("date", ascending=False).withColumnRenamed('24h Low (USD)', 'ordered 24h Low (USD)')

transactions = spark.read.csv('/data/ethereum/transactions', header=True, inferSchema=True)
transactions = transactions.withColumn('date', f.date_format(transactions.block_timestamp.cast(dataType=t.TimestampType()), "yyyy-MM-dd"))
grouped_trans = transactions.groupBy("date").sum("value").withColumnRenamed('sum(value)', 'transctval')

x_feature = x_prices1.join(x_prices2, x_prices1.date==x_prices2.date,how='inner').drop(x_prices2.date)
x_feature = x_feature.join(x_prices3, x_feature.date==x_prices3.date, how='inner').drop(x_prices3.date)
x_feature = x_feature.join(grouped_trans, x_feature.date==grouped_trans.date, how='inner').drop(grouped_trans.date)

y_prices = prices["date","Closing Price (USD)"].orderBy("date", ascending=False)
train = x_feature.select("date")
y_train_test = y_prices.join(train,y_prices.date == train.date,how="inner").drop(train.date)
xy_featureslabels = y_train_test.join(x_feature,y_train_test.date == x_feature.date,how="inner").drop(x_feature.date)
xy_featureslabels = xy_featureslabels.drop(xy_featureslabels.date)

assembler = VectorAssembler(inputCols=['ordered 24h Open (USD)', 'ordered 24h High (USD)', 'ordered 24h Low (USD)', 'transctval'], outputCol="features")
data3 = assembler.transform(xy_featureslabels)
scalerfeature = StandardScaler(inputCol="features",outputCol="scaled_features")
scaleddf = scalerfeature.fit(data3)
scaleddata = scaleddf.transform(data3)


(train,test) = scaleddata.randomSplit([0.7,0.3])
algo = LinearRegression(featuresCol = 'scaled_features', labelCol='Closing Price (USD)', maxIter=10, regParam=0.3, elasticNetParam=0.8)
model = algo.fit(train)

evaluation_summary = model.evaluate(test)
print(evaluation_summary.meanAbsoluteError)
print(evaluation_summary.rootMeanSquaredError)
print(evaluation_summary.r2)
predictions = model.transform(test)
predictions.select("prediction","Closing Price (USD)","scaled_features").show()