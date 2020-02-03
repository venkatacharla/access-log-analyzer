package loganalyzer

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import org.slf4j.LoggerFactory
import org.apache.spark.sql.functions._
import scala.collection.JavaConverters
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import java.net.URL
import java.net.HttpURLConnection
import java.io.InputStream
import java.io.OutputStream
import java.io.BufferedOutputStream
import java.io.FileOutputStream
import java.io.IOException
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.WindowSpec
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

object UtilityCode {

  lazy val logger = LoggerFactory.getLogger(this.getClass.getName)
  val VALID_RESPONSE_CODES: Seq[String] = Seq("200", "302", "304", "400", "403", "404", "500", "501")

  /**
    *
    * @param master
    * @return
    */
  //Creating function to get spark session
  def getSparkSession(master: String = "local[*]"): SparkSession = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark = SparkSession.builder().appName("LogAnalytics").config("spark.tmp.warehouse", "file:///tmp").master(master).getOrCreate()
    spark
  }

  /**
    *
    * @param spark
    * @param filePath
    * @param sep
    * @return
    */
  //Creating function to create RDD
  def readData(spark: SparkSession, filePath: String, sep: String = " "): DataFrame = {
    spark.read.option("sep", sep).option("inferSchema", "true").csv(filePath)
  }

  /**
    *
    * @param rawDataFrame
    * @param colName
    * @return
    */
  //Creating function to get reject records
  def getRejectDF(rawDataFrame: DataFrame, colName: String,validResponseCodes:Seq[String]): DataFrame = {
    rawDataFrame.filter(!col(colName).rlike(literal="([1-5][0-9][0-9])") || col(colName).isNull)
  }

  /**
    *
    * @param dataFrame
    * @param filteredDF
    * @param rejectedDF
    * @return
    */
  //Creating function to get unprocessed data- ex 304 is excluding then these data write it into unprocessed data
  def getUnProcessedRecords(dataFrame: DataFrame, filteredDF: DataFrame,rejectedDF: DataFrame): DataFrame = {
    dataFrame.except(filteredDF).except(rejectedDF)
  }

  /**
    *
    * @param df
    * @param path
    * @param sep
    * @param dfType
    */
  def saveRejectedRecords(df: DataFrame, path: String, sep: String = " ", dfType: String = "rejected"): Unit = {
    saveOutput(df, path, sep = " ", header = false, dfType)
  }

  /**
    *
    * @param df
    * @param path
    * @param sep
    * @param dfType
    */
  def saveUnProcessedRecords(df: DataFrame, path: String, sep: String = " ", dfType: String = "unprocessed"): Unit = {
    saveOutput(df, path, sep = " ", header = false, dfType)
  }

  /**
    *
    * @param df
    * @param path
    * @param sep
    * @param dfType
    */
  //Creating function to get final results
  def saveFinalResult(df: DataFrame, path: String, sep: String = ",", dfType: String = ""): Unit = {
    saveOutput(df, path, sep = ",", header = true, dfType)

  }

  /**
    *
    * @param df
    * @param path
    * @param sep
    * @param header
    * @param dfType
    */
  //Creating function to save output files
  def saveOutput(df: DataFrame, path: String, sep: String = " ", header: Boolean = false, dfType: String): Unit = {
    try {
      df.coalesce(1).write.mode("append").option("header", header)
        .option("dateFormat", "yyyy-MM-dd").option("sep", sep).csv(path)
      val finalRecCount: Long = df.count()
      logger.info(s"Saving the $dfType data($finalRecCount records) has been completed successfully")
    }
    catch {
      case e: Exception =>
        logger.error(s"Error has been occurred while saving file to loc ($path)\n" + e.getMessage)
    }
  }
/**
    *
    * @param df
    * @return
    */
  def formatColumns(df:DataFrame):DataFrame={
  df.select(col("_c0").as("visitor"), to_date(col("_c3"),
  "[dd/MMM/yyyy:HH:mm:ss").as("visitor_time"), split(col("_c5"), " ").getItem(1).as("url"), col("_c6").as("status"))
  }

  /**
    *
    * @param df
    * @param colName
    * @param statusCode
    * @return
    */
  def filterDF(df: DataFrame, colName: String = "_c6", statusCode: Seq[String]): DataFrame = {
    //df.filter(col(colName) === statusCode)
    df.filter(col(colName).isin(statusCode: _*))
  }

  /**
    *
    * @param df
    * @param groupByColName
    * @param topNNo
    * @param win
    * @return
    */
  //Creating function to get top N records
  def getTopNRecords(df: DataFrame, groupByColName: String, topNNo: Int, win: WindowSpec): DataFrame = {
    val groupData: DataFrame = df.na.drop().groupBy(groupByColName, "visitor_time").count()
    logger.info(s"HIT count done based on group by cols ($groupByColName,visitor_time)")
    val topNDataFrame: DataFrame = groupData.withColumn("RANK", dense_rank() over (win))
      .filter(col("RANK") <= topNNo).withColumnRenamed("count", "HITS")
      .orderBy(col("visitor_time").asc, col("HITS").desc)
    logger.info(s"Top ($topNNo) has been generated for  $groupByColName")
    topNDataFrame
  }

  /**
    *
    * @param resultPathDir
    * @param topN
    * @return
    */
  def getOutputFilePaths(resultPathDir: String, topN: Int): (String, String, String, String) = {
    val today = new SimpleDateFormat("yyyMMddHH").format(new Date)
    val rejectedFilePath: String = resultPathDir + s"notprocessed_$today"
    val unprocessedRecordFilePath: String = resultPathDir + s"unprocessedrecords_$today"
    val topNUrlFilePath: String = resultPathDir + s"top${topN}_url_$today"
    val topNVisitorFilePath: String = resultPathDir + s"top${topN}_visitor_$today"
    (rejectedFilePath, unprocessedRecordFilePath, topNUrlFilePath, topNVisitorFilePath)
  }

  /**
    *
    * @param configFilePath
    * @return
    */
  def validateAndGetLocationResponseCode(configFilePath: String): (Int, Seq[String], String) = {
    //Read config file and set configuration properties
    val configFile = ConfigFactory.parseFile(new File(configFilePath))
    logger.info("Parsed config file- " + configFilePath)
    val numOfResultsToFetch: Int = configFile.getInt("analyzer.valueOfN.value")
    val filterResponseCodes = configFile.getStringList("analyzer.filterResponseCodes.value")
    val filterResponseCodesSeq: Seq[String] = JavaConverters.asScalaIteratorConverter(filterResponseCodes.iterator())
      .asScala.toSeq

    //blank value of downloadURL means file is already present. This setting is for re-run approach or incase docker image is not able to connect outside.
    val downloadURL = configFile.getString("analyzer.ftpFileLoc.value")
    val fileLocation: String = configFile.getString("analyzer.fileLocation.value")
    //please check it can we remove below checks
    if (numOfResultsToFetch <= 0) {
      logger.error("The number of results to fetch is not defined in property file")
      System.exit(0);
    }
    if (fileLocation.length() <= 0) {
      logger.error("The file system directory to keep downloaded file is not defined in property file")
      System.exit(0);
    }


    if (!downloadURL.equals("")) {
      if (getFile(downloadURL, fileLocation) == 0) {
        logger.error("Exception : Exiting execution to error while downloading file.")
        System.exit(0)
      }
      logger.info("Download complete at location- " + fileLocation)
    } else {
      logger.info("Download not required. File is already at location- " + fileLocation)
    }
    (numOfResultsToFetch, filterResponseCodesSeq, fileLocation)
  }

  /**
    *
    * @param downloadURL
    * @param filePath
    * @return
    */
  def getFile(downloadURL: String, filePath: String): Int = {
    val url = new URL(downloadURL)
    var return_code = 1
    var in: InputStream = null
    var out: OutputStream = null
    try {
      val connection = url.openConnection()
      connection.setConnectTimeout(5000)
      connection.setReadTimeout(5000)
      connection.connect()
      logger.info("Connection to FTP server successfully.")

      in = connection.getInputStream
      val fileToDownloadAs = new java.io.File(filePath)
      out = new BufferedOutputStream(new FileOutputStream(fileToDownloadAs))
      logger.info("Downloading file from location- " + downloadURL)
      logger.info("Downloading file to location- " + filePath)

      val byteArray = Stream.continually(in.read).takeWhile(-1 !=).map(_.toByte).toArray
      out.write(byteArray)
    } catch {
      case ex: Exception => {
        //logger.error(new ExceptionAttributes("error", s"getFile from ftp failed. Msg: $ex", "getFile").toString());
        return_code = 0
      }
    } finally {
      try {
        in.close()
        out.close()
      } catch {
        case ex: Exception => //logger.error(new ExceptionAttributes("error", s"Closing input/output streams. Msg: $ex", "getFile").toString());
      }
    }
    return return_code
  }
}

/*
object ResponseCode{
  val STATUS200 = "200"
  val STATUS302 = "302"
  val STATUS304 = "304"
  val STATUS400 = "400"
  val STATUS403 = "403"
  val STATUS404 = "404"
  val STATUS500 = "500"
  val STATUS501 = "501"

}
*/
