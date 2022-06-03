-- Add column with only date (without time) to users_utm
DROP TABLE IF EXISTS temp_users_utm_with_date;
CREATE TABLE temp_users_utm_with_date
SELECT split(utmDate, " ")[0] AS date_col, * 
FROM temp_users_utm;

-- Aggregate table with the billings division for each user, to utmSource&date
DROP TABLE IF EXISTS agg_billings;
CREATE TABLE agg_billings
	-- Table with latest utm before purchASe - 50% of billing (no time in purchASe table- ASsuming purchASe wAS made on day of a utm visit)
SELECT a.userId, utmSource, date_col, Billing_amount/2 AS single_source_billing_amount
FROM 
	temp_purchASes a JOIN temp_users_utm_with_date b ON a.purchASeDate=b.date_col AND a.userId=b.userId

UNION

	-- Table with all utm visits of each user prior to latest visit (rest of billing divided evenly)
SELECT userId, tuple[0] AS utmSource, tuple[1] AS date_col, single_source_billing_amount 
FROM (
		-- Explode source & date together
	SELECT userId, Billing_amounts, explode(utmSourcesList) AS tuple, single_source_billing_amount 
	FROM (
		SELECT 
			userId, 
			collect_list(date_col) AS dates, 
			collect_list(Billing_amount)[0] AS Billing_amounts, 
			count(date_col) AS num_sources, 
			collect_list(array(utmSource, date_col)) AS utmSourcesList, -- collect source and date together, for exploding together later
			(collect_set(Billing_amount)[0]/2)/num_sources AS single_source_billing_amount 
		FROM (	
			SELECT a.userId, date_col, utmSource, Billing_amount
			FROM 
				temp_purchASes a JOIN temp_users_utm_with_date b ON a.purchASeDate>b.date_col AND a.userId=b.userId	)
		GROUP BY userId)
);

-- Aggregate user billings & total purchASes to sum across all users for each source & date
DROP TABLE IF EXISTS total_billings;
CREATE TABLE total_billings
SELECT 
	date_col, 
	utmSource,
	sum(single_source_billing_amount) AS total_billing,
	count(*) AS number_of_purchASes
FROM agg_billings GROUP BY date_col, utmSource;

-- Aggregate user registrations to sum across all users for each source & date (don't need users table for this, all info exists in users_utm)
DROP TABLE IF EXISTS agg_registrations;
CREATE TABLE agg_registrations
SELECT count(userId) AS number_registrations, utmSource, date_col 
FROM (
	SELECT 
		userId, 
		date_col, 
		utmSource, 
		min(date_col) OVER(PARTITION BY userId) AS earliest 
	FROM agg_billings) 
WHERE date_col=earliest
GROUP BY utmSource, date_col;

-- Merge information about billings, purchASes and registrations
DROP TABLE IF EXISTS final_output;
CREATE TABLE final_output
SELECT 
	coalesce(a.utmSource, b.utmSource) AS utmSource, 
	coalesce(a.date_col, b.date_col) AS CalendarDate, 
	CASE WHEN number_registrations IS NOT NULL THEN number_registrations ELSE 0 END AS number_of_registrations, 
	CASE WHEN number_of_purchASes IS NOT NULL THEN number_of_purchASes ELSE 0 END AS number_of_purchASes, 
	total_billing
FROM agg_registrations a full outer JOIN total_billings b ON a.utmSource=b.utmSource AND a.date_col=b.date_col;




