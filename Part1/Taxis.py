import pdb
from pyspark.sql.functions import *

class Taxis():
	def __init__(self, spark):
		self.table_name = "data_part1/yellow_taxi_jan_25_2018"
		self.spark = spark
		self.condition_dict = {
		"pickup": {"input_col_name":"PULocationID", "output_col_name":"counts_pu"},
		"dropoff": {"input_col_name":"DOLocationID", "output_col_name":"counts_do"}
		}

	def read(self):
		self.table = self.spark.read.parquet(self.table_name).withColumn('time', date_format('tpep_pickup_datetime', 'HH'))

	def sum_passenger_counts(self, condition):
		column_name = self.condition_dict[condition]["input_col_name"]
		alias = self.condition_dict[condition]["output_col_name"]
		
		return self.table\
		.withColumnRenamed(column_name, "LocationId")\
		.groupBy("LocationId")\
		.agg(sum("passenger_count").alias(alias))\
		.orderBy("LocationId")

	def __aggregate_peak_hour_metrics(self, table):
		return table.agg(sum("total_amount").alias("revenue"), sum("passenger_count").alias("sum_passengers"), count("passenger_count").alias("count_rides"))

	def aggregate_peak_hour_metrics_total(self):
		return self.__aggregate_peak_hour_metrics(self.table\
				.select("time", "passenger_count", "total_amount")\
				.groupBy("time"))
		# .agg(sum("total_amount").alias("revenue"), sum("passenger_count").alias("sum_passengers"), count("passenger_count").alias("count_rides"))

	def aggregate_peak_hour_metrics_distance(self, table_with_distance_description):
		return self.__aggregate_peak_hour_metrics(table_with_distance_description\
		.select("distance_description", "time", "passenger_count", "total_amount")\
		.groupBy("distance_description", "time"))\
		.orderBy("distance_description", desc("revenue"), desc("sum_passengers"), desc("count_rides"))


	def __format_hour(self, hour):
		return hour if len(hour)==2 else f"0{hour}"

	def get_peak_hour_by_count_rides(self, taxiWithHourAggs):
		peak_hour = taxiWithHourAggs\
		.select("time", "count_rides")\
		.agg(max("count_rides").alias("count_rides"))\
		.join(taxiWithHourAggs, "count_rides")\
		.select("time")\
		.head()[0][0]

		return self.__format_hour(peak_hour)

	def get_peak_distance_hours_by_count_rides(self, table_with_distance_description):
		taxiWithDistanceAggs = self.aggregate_peak_hour_metrics_distance(table_with_distance_description)

		peakDF = taxiWithDistanceAggs\
		.select("time", "distance_description", "count_rides")\
		.groupBy("distance_description")\
		.agg(max("count_rides").alias("count_rides"))\
		.join(taxiWithDistanceAggs, "count_rides")\
		.drop(taxiWithDistanceAggs["distance_description"])

		self.long_peak = self.__format_hour(peakDF.select("time").where(col("distance_description")=="long").head()[0][0])
		self.short_peak = self.__format_hour(peakDF.select("time").where(col("distance_description")=="short").head()[0][0])



