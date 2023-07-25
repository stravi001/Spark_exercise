import CsvFileToAvro.getSparkSession
import org.apache.spark.sql.{Dataset, Row}
import PartA.task3
import org.apache.spark.sql.functions.sum
object PartB extends App{

  def joinValutAggAndVaultMaster: Dataset[VaultAggMasterJoin] = {

    val spark = getSparkSession

    import spark.implicits._

    val dsVaultMaster = spark.read.options(Map("inferSchema" -> "true", "delimiter" -> ",", "header" -> "true")).csv("./src/main/resources/vault_master.csv").as[VaultMasterData]

    val dsJoined: Dataset[VaultAggMasterJoin] = task3.join(dsVaultMaster, Seq("vaultid"), "inner") .as[VaultAggMasterJoin]

    dsJoined

  }

  def task1A: String = {

    val spark = getSparkSession

    import spark.implicits._

    val dsResult: Dataset[String] = joinValutAggAndVaultMaster.select("name")
                                                              .sort($"numberOfGoldCoins".desc)
                                                              .as[String]

    dsResult.first()

  }

  def task1B: String = {

    val spark = getSparkSession

    import spark.implicits._

    val dsResult: Dataset[String] = joinValutAggAndVaultMaster.select("name")
                                                              .sort($"numberOfSilverCoins".desc)
                                                              .as[String]

    dsResult.first()

  }

  def task1C: String = {

    val spark = getSparkSession

    import spark.implicits._

    val dsResult: Dataset[String] = joinValutAggAndVaultMaster.select("name")
                                                              .sort($"numberOfEuroCoins".desc)
                                                              .as[String]

    dsResult.first()

  }

  def task2A: Dataset[Row] = {

    val spark = getSparkSession

    import spark.implicits._

    val dsResult: Dataset[Row] = joinValutAggAndVaultMaster.select("location", "numberOfGoldCoins")
                                                              .groupBy("location")
                                                              .agg(sum("numberOfGoldCoins").as("numberOfGoldCoins"))
                                                              .sort($"numberOfGoldCoins".desc)

    dsResult

  }

  def task2B: Dataset[Row] = {

    val spark = getSparkSession

    import spark.implicits._

    val dsResult: Dataset[Row] = joinValutAggAndVaultMaster.select("location", "numberOfSilverCoins")
                                                           .groupBy("location")
                                                           .agg(sum("numberOfSilverCoins").as("numberOfSilverCoins"))
                                                           .sort($"numberOfSilverCoins".desc)

    dsResult

  }

  def task2C: Dataset[Row] = {

    val spark = getSparkSession

    import spark.implicits._

    val dsResult: Dataset[Row] = joinValutAggAndVaultMaster.select("location", "numberOfEuroCoins")
                                                           .groupBy("location")
                                                           .agg(sum("numberOfEuroCoins").as("numberOfEuroCoins"))
                                                           .sort($"numberOfEuroCoins".desc)

    dsResult

  }

  println("Task 1.a: Vault with the most gold = " + task1A)
  println("Task 1.b: Vault with the most silver = " + task1B)
  println("Task 1.c: Vault with the most euro = " + task1C)
  println("Task 2.a: Location with the most gold = " + task2A.first.getString(0))
  println("Task 2.b: Location with the most silver = " + task2B.first.getString(0))
  println("Task 2.c: Location with the most euro = " + task2C.first.getString(0))

}
