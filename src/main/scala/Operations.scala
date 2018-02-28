
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

class Operations {

  def findingHomeTeam(inputFrame: DataFrame, spark: SparkSession): DataFrame  = {

    inputFrame.createOrReplaceTempView("footballmatchesdata")
    spark.sql("select HomeTeam, Count(HomeTeam) as numberOfMatches from footballmatchesdata GROUP BY (HomeTeam)")
  }

  def topTenTeam(inputFrame: DataFrame,spark: SparkSession): DataFrame = {

    val totalHomeMatch = inputFrame.groupBy("HomeTeam").count
    val totalAwayMatch= inputFrame.groupBy("AwayTeam").count
    val totalHomeWining = inputFrame.where(inputFrame("FTR")==="H").groupBy("HomeTeam").count()
    val totalAwayWining = inputFrame.where(inputFrame("FTR")==="A").groupBy("AwayTeam").count()
    val totalNumberOfMatches = totalHomeMatch.union(totalAwayMatch).groupBy("HomeTeam").sum("count")
    val totalWin =  totalHomeWining.union(totalAwayWining).groupBy("HomeTeam").sum("count")
    val tableConsistingTotalMatchesAndWinMatches = totalNumberOfMatches.join(totalWin,Seq("HomeTeam"))
    val column = Seq("Team","totalmatch","totalwin")
    tableConsistingTotalMatchesAndWinMatches.toDF(column: _*).createOrReplaceTempView("footballmatches")
    val tableConsistingWinningPercentage = spark.sql("select Team ,(totalwin/totalmatch)*100 as result from footballmatches ORDER BY result desc Limit 10")
    tableConsistingWinningPercentage
  }



}
