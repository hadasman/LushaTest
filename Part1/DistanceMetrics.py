# from pyspark.sql.functions import percentile_approx, when, col, collect_list, explode
import pyspark.sql.functions as F
import matplotlib.pyplot as plt

class DistanceMetrics():
	def __init__(self, percentiles):
		self.percentiles = percentiles
		self.percentiles_percent = [f"{round(i*100, 2)}%" for i in percentiles]

	def plot_distance_percentiles(self, taxiDF):
		plt.ion()
		plt.subplots(figsize=(8, 5))
		plt.plot(self.percentiles_percent, taxiDF.select(F.percentile_approx("trip_distance", self.percentiles, 1000000).alias("quantiles")).collect()[0][0])
		plt.title("Values of Trip Distance for Percentile Range")
		plt.ylabel("Distance")
		plt.xlabel("Percentiles")
		plt.xticks([self.percentiles_percent[i] for i in range(len(self.percentiles_percent)) if i%2==1])

	def get_percentiles(self, percentile, taxiDF):
		return taxiDF.select(F.percentile_approx("trip_distance", percentile, 1000000).alias("quantiles")).head()[0][0]

	def get_distance_description_table(self, taxiDF, percentile_value):
		self.table_with_distance_description = taxiDF\
		.withColumn("distance_description", F.when(F.col("trip_distance")<percentile_value, "short").otherwise("long"))
	
	def __count_payment_types(self):
		return self.table_with_distance_description\
		.select("distance_description", "payment_type")\
		.groupBy("distance_description").agg(F.collect_list("payment_type").alias("types"))\
		.withColumn("payment_type", F.explode("types"))\
		.groupBy("distance_description", "payment_type")\
		.count()

	def get_most_popular_payment_types(self):
		type_counts_table = self.__count_payment_types()

		return type_counts_table\
		.groupBy("distance_description")\
		.agg(F.max("count").alias("count"))\
		.join(type_counts_table, "count")\
		.drop(F.col("count"))\
		.drop(type_counts_table["distance_description"])



