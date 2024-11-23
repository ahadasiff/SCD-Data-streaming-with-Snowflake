USE DATABASE SCD_DEMO;

USE SCHEMA SCD_DEMO.SCD2;

USE ROLE ACCOUNTADMIN;
GRANT CREATE INTEGRATION ON ACCOUNT TO SYSADMIN;
USE ROLE SYSADMIN;


-- Creting Storage Integration
CREATE OR REPLACE STORAGE INTEGRATION s3_init_real_timee
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::376129854541:role/s3_integration_datawarehousing_role'
STORAGE_ALLOWED_LOCATIONS = ('s3://scd-datawarehousing-as');

DESC STORAGE INTEGRATION s3_init_real_timee;

-- Creating Stage
create or replace stage customer_ext_stage
  url='s3://scd-datawarehousing-as/'
  Storage_Integration = s3_init_real_timee
  
show stages;
list @customer_ext_stage;

-- Creating File Format
CREATE OR REPLACE FILE FORMAT csv
TYPE = CSV
FIELD_DELIMITER = ','
SKIP_HEADER = 1;


-- Creating Snowpipe
CREATE OR REPLACE PIPE customer_s3_pipe
AUTO_INGEST = TRUE
AS
COPY INTO CUSTOMER_RAW
FROM @customer_ext_stage
FILE_FORMAT = CSV;

  
show pipes;

select SYSTEM$PIPE_STATUS('customer_s3_pipe');

select count(*) from customer_raw;

select * from customer_raw


