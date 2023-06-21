SELECT * FROM @MANAGE_DB.EXTERNAL_STAGES.US_ACCIDENTS_DATA_STAGE_S3
(file_format => MANAGE_DB.FILE_FORMATS.CSV_FORMAT);

CREATE DATABASE IF NOT EXISTS ACCIDENTS; 

CREATE SCHEMA ACCIDENTS.USA;

-- FOR JSON : USE 'CREATE OR REPLACE TABLE SCHEMA.TABLE (RAW_FILE VARIANT);' , IT'S HIGHLY COMPRESSED, USED FOR SEMISTRUCTURED DATA, OPTIMIZES QUERYS IN  SNOWFLAKE. 

CREATE OR REPLACE TABLE ACCIDENTS.USA.RAW (
  id VARCHAR(10),
  Source VARCHAR(20),
  Severity INTEGER,
  Start_Time TIMESTAMP,
  End_Time TIMESTAMP, 
  Start_Lat DOUBLE,
  Start_Lng DOUBLE,
  End_Lat DOUBLE,
  End_Lng DOUBLE,
  Distance_mi FLOAT,
  Description VARCHAR(5000),
  Street VARCHAR(100),
  City VARCHAR(50),
  County VARCHAR(50),
  State VARCHAR(11),
  Zipcode VARCHAR(30),
  Country VARCHAR(12),
  Timezone VARCHAR(50),
  Airport_Code VARCHAR(13),
  Weather_Timestamp TIMESTAMP,
  Temperature_F FLOAT,
  Wind_Chill_F FLOAT,
  Humidity FLOAT,
  Pressure_in FLOAT,
  Visibility_mi FLOAT,
  Wind_Direction VARCHAR(14),
  Wind_Speed_mph FLOAT,
  Precipitation_in FLOAT,
  Weather_Condition VARCHAR(50),
  Amenity VARCHAR(50),
  Bump VARCHAR(50),
  Crossing_Give_Way VARCHAR(50),
  Junction VARCHAR(50),
  No_Exit VARCHAR(50),
  Railway VARCHAR(50),
  Roundabout VARCHAR(50),
  Station VARCHAR(50),
  Stop VARCHAR(50),
  Traffic_Calming VARCHAR(50),
  Traffic_Signal VARCHAR(50),
  Turning_Loop VARCHAR(50),
  Sunrise_Sunset VARCHAR(50),
  Civil_Twilight VARCHAR(50),
  Nautical_Twilight VARCHAR(50),
  Astronomical_Twilight VARCHAR(50)
);
