CREATE STORAGE INTEGRATION s3_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::335827416402:group/S3FullAccess'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('s3://running-activity-aog')
;

CREATE STAGE my_s3_stage
  STORAGE_INTEGRATION = s3_int
  URL = 's3://running-activity-aog';

CREATE STAGE RUNNING_ACTIVITY_S3 
	URL = 's3://running-activity-aog' 
	DIRECTORY = ( ENABLE = true );