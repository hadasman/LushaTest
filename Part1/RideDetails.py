from pyspark.sql.functions import *

class RideDetails():
	def __init__(self, spark, pickup_counts_df, dropoff_counts_df, zones_path):
		self.spark = spark
		self.pickup_counts_df = pickup_counts_df
		self.dropoff_counts_df = dropoff_counts_df
		self.zones_path = zones_path

	def __read_zones(self):
		return self.spark.read.options(header='True').csv(self.zones_path)

	def merge_passenger_counts(self):
		self.merged = self.pickup_counts_df\
		.join(self.dropoff_counts_df, "LocationId")\
		.withColumn("total_counts", col("counts_pu")+col("counts_do"))\
		.select("LocationId", "total_counts")\
		.orderBy(desc("total_counts"))

	def get_max_count_location_id(self):
		self.max_count_location_id = self.merged.select("LocationId").head()[0]

	def get_zone_with_most_passengers(self):
		zonesDF = self.__read_zones()

		self.zone_with_most_passengers = zonesDF.select("Zone").where(col("LocationId")==self.max_count_location_id).head()[0]