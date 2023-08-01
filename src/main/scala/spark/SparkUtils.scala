package spark
import org.apache.spark.sql.SparkSession

object SparkUtils {

  def getSparkSession: SparkSession = {

    val outSparkSession: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkSession")
      .getOrCreate()

    outSparkSession.sparkContext.setLogLevel("ERROR")

    outSparkSession

  }

}
