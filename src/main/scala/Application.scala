import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object Application extends App {

  val two = 2
  val three = 3
  val four = 4
  val five = 5
  val six = 6
  val conf = new SparkConf()
  Logger.getLogger("org").setLevel(Level.OFF)

  conf.setAppName("joiningOfTwoFiles")
  conf.setMaster("local[*]")

  val spark = SparkSession
    .builder()
    .config(conf)
    .getOrCreate()
  val filepath = "src/main/resources/D1.csv"
  //loading .csv file
  val footballDataDF = spark.read.option("header", "true").option("inferschema", "true").csv(filepath)
  val footballData = footballDataDF.select("Div", "Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG", "FTR").toDF
  val object1 = new Operations
  object1.findingHomeTeam(footballData, spark).show
  object1.topTenTeam(footballData, spark).show

  //converting dataframe to dataset
  import spark.implicits._

  val footballdataset = footballDataDF.map(col => Football(col.getString(two), col.getString(three), col.getInt(four), col.getInt(five), col.getString(six)))

  //Total number of matches played by each team
  val HomeTeam = footballdataset.groupBy("HomeTeam").count().withColumnRenamed("HomeTeam","Team")
  val AwayTeam = footballdataset.groupBy("AwayTeam").count().withColumnRenamed("AwayTeam","Team")
  val totalTeam = HomeTeam.union(AwayTeam)
   totalTeam.groupBy("Team").sum("count").show

}
