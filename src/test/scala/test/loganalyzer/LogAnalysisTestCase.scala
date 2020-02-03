import loganalyzer.{LogAnalytics, UtilityCode}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions.col
import org.scalatest.FunSuite
import org.scalatest._
import org.scalatest.Assertions._

class LogAnalysisTestCase  extends FunSuite with BeforeAndAfter{
  val spark= UtilityCode.getSparkSession()
  var df:DataFrame=null
  //reading test data file
  val filePath:String="inputdata/testdata.txt"

  before {
    df= UtilityCode.readData(spark,filePath).persist()
  }
  //Creating unit test cases to test rejected records..other than the below response code data will go to reject file
  test("validate reject record count") {
    val rejectDF=UtilityCode.getRejectDF(df,"_c6",Seq("200", "302", "304", "400", "403", "404", "500", "501"))
    val rejectedRecordCount= rejectDF.count()
    val expectedCount:Long= 4L
    assert(rejectedRecordCount === expectedCount)
  }
  //Creating unit test case ti test top record passing visitor value and matching with manual counted values
  //Testing the data between particular date and matching againist test data file
  //Testing number of visitors for a day and matching with test data file
  test("validate top N visitors") {
    val cleanDF:DataFrame = df.transform(UtilityCode.formatColumns)
    val win: WindowSpec = Window.partitionBy("visitor_time").orderBy(col("count").desc)
    val topNVisitors:DataFrame=UtilityCode.getTopNRecords(cleanDF,"visitor",5,win).withColumn("visitor_time",col("visitor_time").cast("string"))
    val actualRow1:Row=topNVisitors.filter(col("visitor")==="lmsmith.tezcat.com").head()
    val expectedRow1:Row= Row("lmsmith.tezcat.com","1995-07-01",3,1)
    val actualRow2:Row=topNVisitors.filter(col("visitor_time")==="1995-07-09").head()
    val expectedRow2:Row=Row("www-d4.proxy.aol.com","1995-07-09",1,1)
    val actualRow3:Row=topNVisitors.filter(col("HITS")===3).head()
    val expectedRow3:Row= Row("lmsmith.tezcat.com","1995-07-01",3,1)
    assert(actualRow1 === expectedRow1)
    assert(actualRow2 === expectedRow2)
    assert(actualRow3 === expectedRow3)
  }
  //Creating the below unit test cases to check top N url matching againist test data file including respopnse code filter
  //Testing top url ffor particular day passing url and date
  //Checking the count of url and day and matching with test data file count
  //Testing for number of hits for a day and matching with test data file count
  test("validate top N url with filtered responses") {
    val accessLogCleansedData:DataFrame = df.transform(UtilityCode.formatColumns)
    val filteredAccessLogLines: DataFrame = UtilityCode.filterDF(accessLogCleansedData, "status", Seq("200", "302", "403", "404", "500", "501"))
    val win: WindowSpec = Window.partitionBy("url").orderBy(col("count").desc)
    val topNurls:DataFrame=UtilityCode.getTopNRecords(filteredAccessLogLines,"url",5,win).withColumn("visitor_time",col("visitor_time").cast("string"))
    val actualRow1:Row=topNurls.filter(col("url")==="/images/kscmap-tiny.gif").head()
    val expectedRow1:Row= Row("/images/kscmap-tiny.gif","1995-07-01",2,1)
    val actualRow2:Row=topNurls.filter(col("visitor_time")==="1995-07-09").head()
    val expectedRow2:Row= Row("/procurement.html","1995-07-09",1,1)
    val actualRow3:Row=topNurls.filter(col("HITS")===2).head()
    val expectedRow3:Row= Row("/images/kscmap-tiny.gif","1995-07-01",2,1)    
    assert(actualRow1 === expectedRow1)
    assert(actualRow2 === expectedRow2)
    assert(actualRow3 === expectedRow3)    
  }  
  //Creating the below unit test cases to check top N url matching againist test data file with out any respopnse code filter
  //Testing top url for particular day passing url and date including all responsecodes
  //Checking the count for url and day and matching with test data file count including all responsecodes
  //Testing for a url num ber of hits for a day and matching with test data file count including all responsecodes
  test("validate top N url with all responses") {
    val accessLogCleansedData:DataFrame = df.transform(UtilityCode.formatColumns)
    val filteredAccessLogLines: DataFrame = UtilityCode.filterDF(accessLogCleansedData, "status", Seq("200", "302","304","400", "403", "404", "500", "501"))
    val win: WindowSpec = Window.partitionBy("url").orderBy(col("count").desc)
    val topNurls:DataFrame=UtilityCode.getTopNRecords(filteredAccessLogLines,"url",5,win).withColumn("visitor_time",col("visitor_time").cast("string"))
    val actualRow1:Row=topNurls.filter(col("url")==="/images/kscmap-tiny.gif").head()
    val expectedRow1:Row= Row("/images/kscmap-tiny.gif","1995-07-01",3,1)
    assert(actualRow1 === expectedRow1)
  }  
}
