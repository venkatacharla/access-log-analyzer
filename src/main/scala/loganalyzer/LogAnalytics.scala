package loganalyzer

import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

object LogAnalytics {

  lazy val logger = LoggerFactory.getLogger(this.getClass.getName)
  var configFilePath: String = ""
  var resultPathDir = ""

  def main(args: Array[String]): Unit = {

    //Parse command-line-arguments
    parseInputArgs(args)

    val (topN, filterResponseCodesSeq, inputFilePath) = UtilityCode.validateAndGetLocationResponseCode(configFilePath)
    val filterColumn: String = "_c6"

    logger.info("File has been downloaded to file location -" + inputFilePath)
    logger.info("Config file parameters as below::\n" +
      "\ntop N records :" + topN +
      "\ntop N records :" + filterResponseCodesSeq.mkString("|") +
      "\nInput File Path :" + inputFilePath
    )
    logger.info("parameter initialization completed!!!")
    //Creating spark session
    val spark: SparkSession = UtilityCode.getSparkSession()
    //Reading the file
    val accessLogData: DataFrame = UtilityCode.readData(spark, inputFilePath, sep = " ")
    val totalRecCount:Long=accessLogData.count()
    logger.info(s"reading data($totalRecCount records) from file path $inputFilePath completed successfully!!!")
    processLogData(accessLogData,filterColumn,topN, filterResponseCodesSeq, inputFilePath)
  }

  def processLogData(accessLogData:DataFrame,filterColumn:String,topN:Int, filterResponseCodesSeq:Seq[String], inputFilePath:String): Unit ={
    val (rejectFilePath, unprocessedRecordFilePath, topNUrlFilePath, topNVisitorFilePath) = UtilityCode.getOutputFilePaths(resultPathDir, topN)
    //Featch bad data records and save it into file
    val rejectedRecordsDF: DataFrame = UtilityCode.getRejectDF(accessLogData, filterColumn,UtilityCode.VALID_RESPONSE_CODES)
    UtilityCode.saveRejectedRecords(rejectedRecordsDF, rejectFilePath, sep = " ")

    //val unprocessedDataLines : DataFrame = UtilityCode.getUnProcessedRecords(accessLogData, filteredAccessLogLines,rejectedRecordsDF)
    //UtilityCode.saveUnProcessedRecords(unprocessedDataLines, unprocessedRecordFilePath, sep = " ")
    val accessLogCleansedData:DataFrame = accessLogData.transform(UtilityCode.formatColumns)
    //val accessLogCleansedData: DataFrame = accessLogData.select(col("_c0").as("visitor"), to_date(col("_c3"),
     // "[dd/MMM/yyyy:HH:mm:ss").as("visitor_time"), split(col("_c5"), " ").getItem(1).as("url"), col("_c6").as("status"))
    logger.info("data cleansing and transformation done successfully")
    //Define window for ranking operation
    val win: WindowSpec = Window.partitionBy("visitor_time").orderBy(col("count").desc)
    //Fetch topN visitors  
    val topNVisitor: DataFrame = UtilityCode.getTopNRecords(accessLogCleansedData, "visitor", topN, win)
    UtilityCode.saveFinalResult(topNVisitor, topNVisitorFilePath, "topN visitors")
  
    val filteredAccessLogLines: DataFrame = UtilityCode.filterDF(accessLogCleansedData, "status", filterResponseCodesSeq)
    logger.info(s"data filtering with response code($filterResponseCodesSeq) successfully!!!")
    //Fetch topN urls    
    val topNUrl: DataFrame = UtilityCode.getTopNRecords(filteredAccessLogLines, "url", topN, win)
    UtilityCode.saveFinalResult(topNUrl, topNUrlFilePath, "topN URL")

  }


  //Function to parse command-line arguments and set local variables
  def parseInputArgs(args: Array[String]) = {
    if (args.length < 2) {
      logger.error("The application property file path and/or output directory path are missing in the argument list!")
      System.exit(0);
    } else {
      configFilePath = args(0)
      resultPathDir = args(1)
      logger.info("parsing input argument completed successfully!!!")
    }

  }

}