# End to End ETL Project AWS S3 and Snowflake Integration

#snowflake #aws #s3 #stage #task #stream #json

Task:

- Upload files to S3 bucket
- Connect S3 bucket to Snowflake instance
- Import data from S3 to Snowflake
- Create dummy ETL
- Use Snowflake Tasks to run ETL

![](_att/Pasted%20image%2020251203213025.png)

# 1. AWS S3 bucket

## 1.1 Creating a bucket

Go to your Amazon account and create new S3 bucket with default settings.

![](_att/Pasted%20image%2020251127191841.png)

![](_att/Pasted%20image%2020251127185519.png)

S3 bucket name: `demo-snowflake-202511`

![](_att/Pasted%20image%2020251127190050.png)

## 1.2 Uploading files

I uploaded two JSON files.

`s3://demo-snowflake-20251100/1426623.json`

`s3://demo-snowflake-20251100/1426624.json`

# 2. Access to S3

## 2.1. Creating a Policy

Go to Identity and Access Management (IAM)

![](_att/Pasted%20image%2020251127191909.png)

Create Policy.

![](_att/Pasted%20image%2020251127192217.png)

Use JSON switch and past next. Of course, replace `<bucket>` by your S3 bucket name)

```json
{
	"Version": "2012-10-17",
	"Statement": [
		{   
			"Effect": "Allow",
			"Action": [
				"s3:PutObject",
				"s3:GetObject",
				"s3:GetObjectVersion",
				"s3:DeleteObject",
				"s3:DeleteObjectVersion"
			],
			"Resource": "arn:aws:s3:::<bucket>/<prefix>/*"
		},
		{   
			"Effect": "Allow",
			"Action": [
				"s3:ListBucket",
				"s3:GetBucketLocation"
			],
			"Resource": "arn:aws:s3:::<bucket>",
			"Condition": {
				"StringLike": {
					"s3:prefix": [
						"<prefix>/*"
					]
				}
			}
		}
	]
}
```

![](_att/Pasted%20image%2020251127194431.png)

Give it a name and description. Notice that there are rights for List, Read and Write to S3 for All.  Create it.

![](_att/Pasted%20image%2020251127194749.png)

## 2.2. Creating a Role

Best practice is to create Policies (rules) for some Role and assign that Role for needed User. *But not to assign Policies to User directly.*

Go to Roles.

![](_att/Pasted%20image%2020251127195456.png)

![](_att/Pasted%20image%2020251130120241.png)

Copy your Account ID at step 4 and press Next. Select Policy you created before.

![](_att/Pasted%20image%2020251130120509.png)

Give it a name and Create role.

![](_att/Pasted%20image%2020251130120806.png)

Get into tour new Role and copy ARN (Application Resource Number).

**ARN** (example): `arn:aws:iam::963382508347:role/demo-snowflake-202511`



# 3. Creating Integration

Go to Snowflake SQL sheet:

```sql
-- 
USE ROLE accountadmin;

CREATE OR REPLACE DATABASE demo_s3;
CREATE OR REPLACE SCHEMA demo;

USE DATABASE demo_s3;
USE SCHEMA demo;

CREATE OR REPLACE STORAGE INTEGRATION aws_s3_integration
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = 'S3'
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::963382508347:role/demo-snowflake-202511'
STORAGE_ALLOWED_LOCATIONS = ('s3://demo-snowflake-20251100/');

-- If you use another ROLE for creating DATABASE - replace with it
GRANT USAGE ON INTEGRATION aws_s3_integration TO ROLE accountadmin;
```

Check.

```sql
SHOW INTEGRATIONS;
```

![](_att/Pasted%20image%2020251130130546.png)

```sql
DESC INTEGRATION aws_s3_integration;
```

![](_att/Pasted%20image%2020251130130907.png)

We pasted only `STORAGE_AWS_ROLE_ARN` and `STORAGE_ALLOWED_LOCATIONS`, but Snowflake generated `STORAGE_AWS_IAM_USER_ARN` and `STORAGE_AWS_EXTERNAL_ID`. Copy it.

**STORAGE_AWS_IAM_USER_ARN** (example): `arn:aws:iam::278240454219:user/m4o14000-s`
**STORAGE_AWS_EXTERNAL_ID** (example): `PMB69135_SFCRole=4_P1lWwUciM04Zd7d3/tccPYQCtLu=`

Go to AWS > IAM > Roles > Select your Role > Trust relationships > Edit trust policy.

![](_att/Pasted%20image%2020251130132331.png)

Replace with your values and save.

```json
{
	"Version": "2012-10-17",
	"Statement": [
		{
			"Effect": "Allow",
			"Principal": {
				"AWS": "<STORAGE_AWS_IAM_USER_ARN>"
			},
			"Action": "sts:AssumeRole",
			"Condition": {
				"StringEquals": {
					"sts:ExternalId": "<STORAGE_AWS_EXTERNAL_ID>"
				}
			}
		}
	]
}
```

# 4. Creating Staging

```sql
-- File Format is optional
CREATE OR REPLACE FILE FORMAT demo_s3.demo.demo_json_format
    TYPE = JSON
    NULL_IF = ('\\n', 'null', '')
    STRIP_OUTER_ARRAY = TRUE
    COMPRESSION = NONE
    COMMENT = 'JSON File Format with outer strip array flag True';


CREATE OR REPLACE STAGE demo_s3.demo.demo_aws_stg
    STORAGE_INTEGRATION = aws_s3_integration
    FILE_FORMAT = demo_s3.demo.demo_json_format  -- File Format is optional
    URL = 's3://demo-sf-20251100/cricket/';
```

Check:

```sql
LIST @demo_s3.demo.demo_aws_stg;
```

![](_att/Pasted%20image%2020251130220222.png)

# 5. Query JSON Data

I created some dummy ETL for json data with simple transformation.

Test selection from one file from stagin

```sql
SELECT
    t.$1:meta::VARIANT AS meta,
    t.$1:info::VARIANT AS info,
    t.$1:innings::VARIANT AS innings,
    --
    METADATA$FILENAME AS file_name,
    METADATA$FILE_ROW_NUMBER::INT AS file_row_number,
    METADATA$FILE_CONTENT_KEY::TEXT AS file_content_key,
    METADATA$FILE_LAST_MODIFIED AS file_last_modified
FROM @demo_s3.demo.demo_aws_stg/1426623.json
    (FILE_FORMAT => 'demo_s3.demo.demo_json_format') AS t;
```

Note about statement `FROM @demo_s3.demo.demo_aws_stg/1426623.json`. As you remember we created out stage with URL option `URL = 's3://demo-sf-20251100/cricket/'` and it also has prefix `cricket/`. So our files from `cricket` folder are locating in root of `demo_aws_stg` stage.

Next create first table and load our two files in it.

```sql
CREATE OR REPLACE TABLE demo_s3.demo.json_root_00 (
    meta OBJECT NOT NULL,
    info VARIANT NOT NULL,
    innings ARRAY NOT NULL,
    --
    sgt_file_name TEXT NOT NULL,
    stg_file_row_number INT NOT NULL,
    stg_file_content_key TEXT NOT NULL,
    stg_file_last_modified TIMESTAMP NOT NULL
)
COMMENT = 'This is raw table to store all the JSON data file with root elements extracted'
;


COPY INTO demo_s3.demo.json_root_00 FROM (
    SELECT
        t.$1:meta::OBJECT AS meta,
        t.$1:info::VARIANT AS info,
        t.$1:innings::ARRAY AS innings,
        --
        METADATA$FILENAME AS file_name,
        METADATA$FILE_ROW_NUMBER::INT AS file_row_number,
        METADATA$FILE_CONTENT_KEY::TEXT AS file_content_key,
        METADATA$FILE_LAST_MODIFIED AS file_last_modified
    FROM @demo_s3.demo.demo_aws_stg
        (FILE_FORMAT => 'demo_s3.demo.demo_json_format') AS t
)
ON_ERROR = CONTINUE
;
```

![](_att/Pasted%20image%2020251130224803.png)

# 6. ETL by Tasks & Streams

Very simple ETL: 

![](_att/Pasted%20image%2020251201200901.png)

- JSON files arrives to S3 and we see them in Staging.
- Snowflake every 10 minutes uploads files from staging to first table. Snowflake has its own history of copied files to table. If files already marked as LOADED, they are skipped. 

![](_att/Pasted%20image%2020251202200054.png)

- If no new rows (delta) appears in the first fact table, the entire ETL is skipped. Delta is provided by Streams.

## 6.1. First Scheduled Task

```sql
-- step1 - Creating a Task that runs every 10 min to load JSON data into raw table
CREATE OR REPLACE TASK demo_s3.demo.load_to_json_root_1_from_stg
    --WAREHOUSE = 'compute_xsmall'
    schedule = '10 minutes'
AS
    COPY INTO demo_s3.demo.json_root_1 FROM (
        SELECT
            t.$1:meta::OBJECT AS meta,
            t.$1:info::VARIANT AS info,
            t.$1:innings::ARRAY AS innings,
            --
            METADATA$FILENAME AS stg_file_name,
            METADATA$FILE_ROW_NUMBER::INT AS stg_file_row_number,
            METADATA$FILE_CONTENT_KEY::TEXT AS stg_file_content_key,
            METADATA$FILE_LAST_MODIFIED AS stg_file_last_modified
        FROM @demo_s3.demo.demo_aws_stg
            (FILE_FORMAT => 'demo_s3.demo.demo_json_format') AS t
    )
    ON_ERROR = CONTINUE
;
```

## 6.2. Stream on first table

```sql
-- step2 - Creating a Stream to track changes on Raw (1st) table
CREATE OR REPLACE STREAM demo_s3.demo.for_games_2_stream 
    ON TABLE demo_s3.demo.json_root_1 APPEND_ONLY = TRUE;
```

Troubleshooting Stream:

```sql
-- Show new data inserted/modified/deleted to table
SELECT * FROM demo_s3.demo.for_games_2_stream;

-- True/False if there are any changes
SELECT SYSTEM$STREAM_HAS_DATA('demo_s3.demo.for_games_2_stream') AS has_new_data;
```

## 6.3. First child Task to read stream & load data

```sql
-- step3 - Creating child Task to read stream & load data from Raw table to games_2
CREATE OR REPLACE TASK demo_s3.demo.load_to_games_2
    --WAREHOUSE = 'compute_xsmall'
    AFTER demo_s3.demo.load_to_json_root_1_from_stg
    WHEN 
        SYSTEM$STREAM_HAS_DATA('demo_s3.demo.for_games_2_stream')
AS
    INSERT INTO demo_s3.demo.games_2
    SELECT
        info:match_type::TEXT AS match_type,
        info:dates[0]::DATE AS event_date,
        info:event:name::TEXT AS event_name,
        info:season::TEXT AS season,
        info:venue::TEXT AS venue,
        info:gender::TEXT AS gender,
        info:teams[0]::TEXT AS team_a,
        info:teams[1]::TEXT AS team_b,
        info:outcome:winner::TEXT AS winner,
        --
        stg_file_name,
        stg_file_row_number,
        stg_file_content_key,
        stg_file_last_modified
    FROM demo_s3.demo.for_games_2_stream 
    WHERE info:outcome:winner::TEXT IS NOT NULL
;
```

Notice that data gets from Stream `demo_s3.demo.for_games_2_stream ` but not a first table.

## 6.4. & 6.5.  Treating dimentional tables

```sql
-- step4 - Creating a child Task after games data is populated
CREATE OR REPLACE TASK demo_s3.demo.load_to_dim_teams
    --WAREHOUSE = 'compute_xsmall'
    AFTER demo_s3.demo.load_to_games_2
AS
    INSERT INTO demo_s3.demo.dim_teams (team_name)
    WITH cte AS (
        SELECT team_a FROM demo_s3.demo.games_2
        UNION ALL
        SELECT team_b FROM demo_s3.demo.games_2
    )
    SELECT DISTINCT team_a AS team_name 
    FROM cte
    MINUS
    SELECT team_name 
    FROM demo_s3.demo.dim_teams
;    

-- step5 - Creating a child Task after games data is populated
CREATE OR REPLACE TASK demo_s3.demo.load_to_dim_venues
    --WAREHOUSE = 'compute_xsmall'
    AFTER demo_s3.demo.load_to_games_2
AS
    INSERT INTO demo_s3.demo.dim_venues (venue_name)
    SELECT DISTINCT venue AS venue_name
    FROM demo_s3.demo.games_2 
    MINUS
    SELECT venue_name 
    FROM demo_s3.demo.dim_venues
;
```

## 6.6.  Populating the fact table

Only new rows appends to fact tables at the end.

```sql
-- step6 - Populating the fact table
CREATE OR REPLACE TASK demo_s3.demo.load_to_fact_games_4
    --WAREHOUSE = 'compute_xsmall'
    AFTER demo_s3.demo.load_to_dim_teams, demo_s3.demo.load_to_dim_venues
AS
    INSERT INTO demo_s3.demo.fact_games_4
    WITH old AS (
        SELECT id FROM demo_s3.demo.fact_games_4
    ), fact AS (
    SELECT
        g.stg_file_content_key AS id,
        --
        g.match_type,
        g.event_date,
        g.event_name || ' - Season ' || season AS event_name,
        venue_id,
        g.gender,
        t1.team_id AS team_a_id,
        t2.team_id AS team_b_id,
        t3.team_id AS winner_id,
        --
        g.stg_file_name
    FROM demo_s3.demo.games_2 AS g
    LEFT JOIN demo_s3.demo.dim_venues AS v
        ON g.venue = v.venue_name
    LEFT JOIN demo_s3.demo.dim_teams AS t1
        ON g.team_a = t1.team_name
    LEFT JOIN demo_s3.demo.dim_teams AS t2
        ON g.team_b = t2.team_name
    LEFT JOIN demo_s3.demo.dim_teams AS t3
        ON g.winner = t3.team_name
    )
    SELECT *
    FROM fact 
    LEFT JOIN old 
        ON fact.id = old.id
    WHERE old.id IS NULL
;
```

## 6.7 Resume all Tasks

Root task is the last to resume.

```sql
ALTER TASK demo_s3.demo.load_to_games_2 RESUME;
ALTER TASK demo_s3.demo.load_to_dim_teams RESUME;
ALTER TASK demo_s3.demo.load_to_dim_venues RESUME;
ALTER TASK demo_s3.demo.load_to_fact_games_4 RESUME;
ALTER TASK demo_s3.demo.load_to_json_root_1_from_stg RESUME;
```

Manual task execution:

```sql
-- root task
EXECUTE TASK demo_s3.demo.load_to_json_root_1_from_stg;
```

## 6.7 Task Graph

![](_att/Pasted%20image%2020251201201049.png)

## 6.8 Task History

```sql
-- To see all tasks history with last executed task first
SELECT * FROM TABLE(information_schema.task_history())
WHERE DATE(scheduled_time) = '2025-12-02'
ORDER BY scheduled_time DESC;

-- To see results of a specific task in last 6 hours
SELECT * FROM TABLE(information_schema.task_history(
    scheduled_time_range_start => DATEADD('hour', -1, CURRENT_TIMESTAMP())--, task_name => 'Task Name'
));

-- To see results in a given time period
SELECT * FROM TABLE(information_schema.task_history(
    scheduled_time_range_start => TO_TIMESTAMP_LTZ('2025-12-02 10:00'), 
    scheduled_time_range_end => TO_TIMESTAMP_LTZ('2025-12-03 10:00')
    -- example '2022-07-17 10:00:00.000 -0700'
));
```

## 6.9 Tasks screenshots

Root task execution.

![](_att/Pasted%20image%2020251202202708.png)

Second task skipped some times because no new data.

![](_att/Pasted%20image%2020251202202751.png)