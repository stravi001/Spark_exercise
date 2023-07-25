import org.apache.spark.sql.{Dataset, SaveMode}
import CsvFileToAvro.getSparkSession
import org.apache.spark.sql.functions.{col, expr, sum}
object PartA extends App {

  def valutDataset: Dataset[CsvData] = {

    val spark = getSparkSession

    import spark.implicits._

    val dsSource = spark.read.options(Map("inferSchema" -> "true", "delimiter" -> ",", "header" -> "true")).csv("./src/main/resources/valut.csv").as[CsvData]

    dsSource

  }
  def task1: Long = {

    valutDataset.count

  }
  def task2: Long = {

    valutDataset.dropDuplicates("vaultId")
                .count

  }

  def task3: Dataset[VaultData] = {

    val spark = getSparkSession

    import spark.implicits._

    val dsContentAgg: Dataset[VaultData] = valutDataset.select(expr("vaultId"),
                                                               expr("(length(content) - length(replace(lower(content), 'gold'))) / length('gold') as numberOfGoldCoins"),
                                                               expr("(length(content) - length(replace(lower(content), 'silver'))) / length('silver') as numberOfSilverCoins"),
                                                               expr("(length(content) - length(replace(lower(content), 'euro'))) / length('euro') as numberOfEuroCoins"))
                                                        .groupBy("vaultId")
                                                        .agg(sum("numberOfGoldCoins").as("numberOfGoldCoins"),
                                                             sum("numberOfSilverCoins").as("numberOfSilverCoins"),
                                                             sum("numberOfEuroCoins").as("numberOfEuroCoins"))
                                                        .orderBy(col("vaultId").asc)
                                                        .as[VaultData]

    dsContentAgg

  }

   def task4: Dataset[Double] = {

     val spark = getSparkSession

     import spark.implicits._

     val dsContentAgg: Dataset[Double] = task3.select(expr("numberOfGoldCoins + (numberOfSilverCoins  + numberOfEuroCoins / 5.2) / 10 as contentInGoldCoins"))
                                              .agg(sum("contentInGoldCoins").as("contentInGoldCoins"))
                                              .as[Double]

     dsContentAgg

  }

  def task5: Dataset[Double] = {

    val spark = getSparkSession

    import spark.implicits._

    val dsContentAgg: Dataset[Double] = task3.select(expr("numberOfGoldCoins * 10 + numberOfSilverCoins + numberOfEuroCoins / 5.2 as contentInSilverCoins"))
                                             .agg(sum("contentInSilverCoins").as("contentInSilverCoins"))
                                             .as[Double]

    dsContentAgg

  }

  def task6: Dataset[Double] = {

    val spark = getSparkSession

    import spark.implicits._

    val dsContentAgg: Dataset[Double] = task3.select(expr("(numberOfGoldCoins * 10 + numberOfSilverCoins) * 5.2 + numberOfEuroCoins as contentInEuroCoins"))
      .agg(sum("contentInEuroCoins").as("contentInEuroCoins"))
      .as[Double]

    dsContentAgg

  }

  def task7(inpDataFrame: Dataset[VaultData], inpOutputPath: String): Unit = {

    inpDataFrame.write.format("avro").mode(SaveMode.Overwrite).save(inpOutputPath)

  }

  println("Task 1: How many records does file contain = " + task1)
  println("Task 2: How many vaults does scrooge have according to the csv = " + task2)
  task3.show()
  println("Task 4: Scrooge's total wealth in gold = " + task4.first)
  println("Task 5: Scrooge's total wealth in silver = " + task5.first)
  println("Task 6: Scrooge's total wealth in euro = " + task6.first)
  task7(task3, "./src/main/resources/vaults_content_cnt.avro")



}