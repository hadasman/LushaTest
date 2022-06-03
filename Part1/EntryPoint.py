import pyspark
from pyspark.sql import SparkSession
import numpy as np
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions import *
import pdb
from Taxis import Taxis
from RideDetails import RideDetails
from DistanceMetrics import DistanceMetrics

spark = SparkSession.builder.master("local[1]") \
                 .appName('SparkByExamples.com') \
                 .getOrCreate()

taxis_obj = Taxis(spark)
taxis_obj.read() 

################################################################################################################################
"""Question 1"""
PU_sum_counts = taxis_obj.sum_passenger_counts("pickup")
DO_sum_counts = taxis_obj.sum_passenger_counts("dropoff")

ride_details_obj = RideDetails(spark, PU_sum_counts, DO_sum_counts, "data_part1/taxi_zones.csv")
ride_details_obj.merge_passenger_counts()

ride_details_obj.get_max_count_location_id()
ride_details_obj.get_zone_with_most_passengers() 

# Print result
print("============================================================")
print(f"The zone with most passengers: {ride_details_obj.zone_with_most_passengers} (LocationId: {ride_details_obj.max_count_location_id})")
print("============================================================\n")

################################################################################################################################
"""Question 2"""
taxiWithHourAggs = taxis_obj.aggregate_peak_hour_metrics_total()

#assuming count_rides is peak:
peak_hour = taxis_obj.get_peak_hour_by_count_rides(taxiWithHourAggs)

print("============================================================")
print("For all taxis peak hour is {}:00".format(peak_hour))
print("============================================================\n")

################################################################################################################################
"""
Questions 3 & 4
Since there is a very large difference between the maximum distance and the rest, I checked a large range of percentiles (up to np.arange(0.0, 1.001, 0.001)).
There still was a great difference, where for the other percentiles the difference between the values was small. 
Therefore I didn't see a clearcut threshold for long/short distcnaes, so I am setting an arbitrary threshold at the 90th percentile (which is still very small compared ot other values).
The reason I am not using the median, which would be the most intuitive, is that this felt too arbitrary- the distances were still low compared with the average. 
	- Average: 2.717989442380494
	- Median: 1.5

Since long/short is a somewhat subjective measure, not necessarily determined by the amount of people making such trips, I made my decision by "eyeballing" 
the distribution of percentiles, as we can see in the figure- I estimated the slope starting to rise at around the 90th percentile. Although not the only way to define long/short,
I thought it was reasonable to take the value at which I started seeing a larger difference between values.

"""

distances_obj = DistanceMetrics(list(np.arange(0.82, 1.0, 0.007)))
distances_obj.plot_distance_percentiles(taxis_obj.table)

ninety_th_percentile = distances_obj.get_percentiles([0.9], taxis_obj.table)

distances_obj.get_distance_description_table(taxis_obj.table, ninety_th_percentile)

################################################################################################################################
"""Question 3"""

#assuming count_rides is peak:
taxis_obj.get_peak_distance_hours_by_count_rides(distances_obj.table_with_distance_description)

# Print result
print("============================================================")
print("For long drives peak hour is {}:00".format(taxis_obj.long_peak))
print("For short drives peak hour is {}:00".format(taxis_obj.short_peak))
print("============================================================\n")

################################################################################################################################
"""Question 4"""

leading_payment_types = distances_obj.get_most_popular_payment_types()

long_type = leading_payment_types.select("payment_type").where(col("distance_description")=="long").head()[0]
short_type = leading_payment_types.select("payment_type").where(col("distance_description")=="short").head()[0]

#  For both long and short trips payment method is the same: 1
print("============================================================")
print("For long drives the leading payment method is: {}".format(long_type))
print("For short drives the leading payment method is: {}".format(short_type))
print("============================================================\n")
















