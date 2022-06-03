
## Lusha Data Engineer coding assignment

###  Part 1 (python/ pyspark)

As a Data engineer you are asked to analyze a data set and uncover useful insights.
In order to achieve this you need to load **csv and parquet files from the "data_part1/yellow_taxi_jan_25_2018" folder**, and answer a few questions:
    
   1. Which Borough have the most pickups/dropoffs overall?
   2. What are the peak hours for taxi?
   3. What are the peak hours for long/short trips?
   5. How are people paying for the ride, on long/short trips?

------------


    
###  Part 2 (python) 

As a Data Engineer you are asked to consume messages from specific topic and load them into DWH relational table.
In order to achieve this the following steps should be implemented:

1.  subscribe to "Users" topic  
2.  write Kafka Consumer with the capability to consume messages and to load them into DWH of your choice in bulks of 5K or 2 minutes (the first condition met among the two) 
4.  flatten the data and load the messages in bulks into "Fact_Users" table 

**"Users" topic message example**
```
{
  "account_id": 12345,
  "attributes": {
    "account_id": 12345,
    "account_status": "active",
    "billing_end_date": "2021-01-03T09:32:53.000Z",
    "billing_plan_id": 1,
    "is_safari": false,
    "is_windows": true,
    "last_name": "cohen",
    "os": "Windows 10.0",
    "user_id": 1000001
  },
  "billing_plan_category": "free",
  "billing_plan_id": 1,
  "billing_plan_renewal_type": "month",
  "is_mobile": false,
  "rule_key": "plugin_v3",
  "user_id": 1000001,
  "variables": {
    "plugin_variable": "test_var"
  },
  "visitor_id": "hasdkashjascsac"
}

```
**"Fact_Users" relational table structure:**

```
account_id             
attributes_account_id        
attributes_account_status    
attributes_billing_end_date  
attributes_billing_plan_id   
attributes_is_safari         
attributes_is_windows       
attributes_last_name       
attributes_os                
attributes_user_id           
billing_plan_category
billing_plan_id
billing_plan_renewal_type
is_mobile
rule_key
variables_plugin_variable
visitor_id
```

------------

###  Part 3 (sql) 

The Marketing team would like to track their activities and understand which traffic source shows the highest performance. The traffic sources are reported via UTMs.

**Marketing team's KPIs:**

Registrations of new users<br/>
Billing out of users’ purchases

*The logic of attribution is as follows:*

**Registrations:** First Touch - the first UTM source the user encountered

**Billing from purchase:** Time Decay - 50% of the billing is attributed to the last UTM the user encountered before purchase, the rest of the money is distributed evenly between the prior UTMs the user encountered

*How is the real data reflected in the BI tables?*


Each time a user visits the company’s website, his visit and the UTMs in the URL are documented in the users_utm table ("users_utm" file from the "data" folder).<br/> Each user registration is documented in the users table ("users" file from the "data" folder).<br/>
Each purchase is documented in the purchases table ("purchases" file from the "data" folder).<br/>


**Instructions for the assignment:**<br/> Produce a SQL ETL script to populate aggregated table with the following structure:


CalendarDate date<br/>utmSource varchar(100)<br/>number_of_registrations int<br/>number_of_purchases int<br/>total_billing decimal (12,6)




------------

**Good luck!**

