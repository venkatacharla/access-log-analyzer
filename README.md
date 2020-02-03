# Introduction
Nasa Web Access Log Analyzer Application

## Objective
- Get top N visitor
- Get top N urls

### Code walkthrough
Input Download URL - ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz	
Created project access-log-analyzer to get Top N Url and Visitor and run the test cases
1. src/main/scala
	* *access-log-analyzer.UtilityCode.scala* - Created object with all functions to call in processing object.
	* *access-log-analyzer.LogAnalytics.scala* - The spark Dataset processing logic is present here. This is the one will create 
2. src/main/resources
	* *accesslogparam.conf* - all configurations are maintained in a Typesafe config file.
3. src/test/scala
	* *access-log-analyzer.LogAnalysisTestCase.scala* - This is the Unit test class for LogAnalytics.

### Configurations
Configuration file name- *accesslogparam.conf*

Properties set in configuration file-
- **ftpFileLoc**- This is the ftp location from where the input gz file will be downloaded. If set to blank, then file-download will not happen, assuming that the file is already on local filesystem. *STRING value*
- **fileLocation**- This is the local filesystem path where gz file is downloaded. If the property "fileLocation" is not set, then the application assumes that the gz file is present at this location. *REQUIRED STRING value*	
- **valueOfN**- This property is to set the value of N in the topNVisitors and topNUrls. *REQUIRED INT value*
- **filterResponseCodes**- When evaluating topNUrls, this property is used to calculate top N url for the codes which defining here

Incase required configuration properties are not set, then the application exits with code 0.

### Assumptions
1. Data is structured in the following format- 
	* `<visitor> - - [<date> <timezone>] "<method> <url> <protocol>" <resonseCode> <unknownvariable>`
  	  `E.g.- lmsmith.tezcat.com - - [01/Jul/1995:00:02:20 -0400] "GET /images/NASA-logosmall.gif HTTP/1.0" 200 786`
  	  String split functions have been used to derive date and other attributes based on the assumption that the data log line will follow this format.

2. When log-lines are tokenized using tokenizer **SPACE** (" "), there are entries with-
	* *Reading between " " value as one column and cleansing to take url removing method and protocal
	* *Creating variable with all http response codes to remove bad data from zip file if not matching with list of values then the dat awill go to rejected file
	
3. To evaluate topNurls, creating list of values in config file - excluding 304 and 404, if wants include them just add it in config file
	* *filtering null record while fetching top N url's with applying filter condition

### Software versions
	- Scala version- 2.11.8
	- Spark version- 2.4.3
	- SBT version- 1.0.1
	- IDE- Eclipse Scale IDE
	
### Steps to compile
1. Go to the project root directory, where build.sbt is present
2. Run cmd- `sbt clean assembly`. 
3. The jar is generated in the target directory. Check jar full path in the console-log.


### Steps to run unit test
1. Go to the project root directory, where build.sbt is present
2. Run cmd- `sbt test`.
3. The test report is generated in the console-log.

### Steps to run the application on the local machine
#### System Setup
mkdir <app_base_path>/conf

mkdir <app_base_path>/input

mkdir <app_base_path>/input/jar

mkdir <app_base_path>/output
	
	
Copy accessloganalyzer.conf to <app_base_path>/conf

Copy jar to <app_base_path>/input/jar

Update accessloganalyzer.conf environment specific properties.

Run application

#### Command to execute	
```
spark-submit --class loganalyzer.LogAnalytics \
--master local[4] <full-path-to-jar's-dir>/AccessLogStats.jar \
<full-path-to-accesslogparam.confr> <path-of-output-file-dir>

Note: 
pass config file path inclusing file name
pass path of the output file dir

### Running it in docker
To run this docker file
- Download it from https://1drv.ms/u/s!AqoZASkgVNRcaU8YLBCyu1PUDqs?e=Ar7G3o
- Run `gzip -d spark-docker.tar.gz`
- Run `docker load --input spark-docker.tar`
- Run `docker run -p 4040:4040 -v <your_host_path>/spark-data:/opt/spark-data spark-docker:2.3.3`
- The output will be written to <your_host_path>/spark-data/NasaWebAccessStats/ouput
	

