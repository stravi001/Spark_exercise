import org.apache.spark.sql.functions.col
import CsvFileToAvro.get_spark_session
object Part_B extends App{

  def join_valut_and_vault_master = {

    val spark = get_spark_session

    val ds_vault = spark.read.options(Map("inferSchema" -> "true", "delimiter" -> ",", "header" -> "true")).csv("./src/main/resources/valut.csv")
    val ds_vault_master = spark.read.options(Map("inferSchema" -> "true", "delimiter" -> ",", "header" -> "true")).csv("./src/main/resources/vault_master.csv")

    val ds_join = ds_vault.join(ds_vault_master, Seq("vaultid"), "inner")

    ds_join

  }

  def Task_1_a = {

    val v_max_gold = join_valut_and_vault_master
      .selectExpr(
        "vaultId",
        "name",
        "(length(content) - length(replace(lower(content), 'gold'))) / length('gold') as numberOfGoldCoins")
      .groupBy("vaultId", "name")
      .sum("numberOfGoldCoins")
      .toDF(colNames = "vaultId", "name", "numberOfGoldCoins")
      .orderBy(col("numberOfGoldCoins").desc) //jinak bez col chyba
      .first

    v_max_gold

  }

  def Task_1_b = {

    val v_max_silver = join_valut_and_vault_master
      .selectExpr(
        "vaultId",
        "name",
        "(length(content) - length(replace(lower(content), 'silver'))) / length('silver') as numberOfSilverCoins")
      .groupBy("vaultId", "name")
      .sum("numberOfSilverCoins")
      .toDF(colNames = "vaultId", "name", "numberOfSilverCoins")
      .orderBy(col("numberOfSilverCoins").desc) //jinak bez col chyba
      .first

    v_max_silver

  }

  def Task_1_c = {

    val v_max_euro = join_valut_and_vault_master
      .selectExpr(
        "vaultId",
        "name",
        "(length(content) - length(replace(lower(content), 'euro'))) / length('euro') as numberOfEuroCoins")
      .groupBy("vaultId", "name")
      .sum("numberOfEuroCoins")
      .toDF(colNames = "vaultId", "name", "numberOfEuroCoins")
      .orderBy(col("numberOfEuroCoins").desc) //jinak bez col chyba
      .first

    v_max_euro

  }

  def Task_2_a = {

    val v_max_gold = join_valut_and_vault_master
      .selectExpr(
        "location",
        "(length(content) - length(replace(lower(content), 'gold'))) / length('gold') as numberOfGoldCoins")
      .groupBy("location")
      .sum("numberOfGoldCoins")
      .toDF(colNames = "location", "numberOfGoldCoins")
      .orderBy(col("numberOfGoldCoins").desc) //jinak bez col chyba
      .first

    v_max_gold

  }

  def Task_2_b = {

    val v_max_silver = join_valut_and_vault_master
      .selectExpr(
        "location",
        "(length(content) - length(replace(lower(content), 'silver'))) / length('silver') as numberOfSilverCoins")
      .groupBy("location")
      .sum("numberOfSilverCoins")
      .toDF(colNames = "location", "numberOfSilverCoins")
      .orderBy(col("numberOfSilverCoins").desc) //jinak bez col chyba
      .first

    v_max_silver

  }

  def Task_2_c = {

    val v_max_euro = join_valut_and_vault_master
      .selectExpr(
        "location",
        "(length(content) - length(replace(lower(content), 'euro'))) / length('euro') as numberOfEuroCoins")
      .groupBy("location")
      .sum("numberOfEuroCoins")
      .toDF(colNames = "location", "numberOfEuroCoins")
      .orderBy(col("numberOfEuroCoins").desc) //jinak bez col chyba
      .first

    v_max_euro

  }

  println("Task 1.a: Vault with the most gold = " + Task_1_a.getString(1) + " (ID " + Task_1_a.getInt(0) + ")")
  println("Task 1.b: Vault with the most silver = " + Task_1_b.getString(1) + " (ID " + Task_1_b.getInt(0) + ")")
  println("Task 1.c: Vault with the most euro = " + Task_1_c.getString(1) + " (ID " + Task_1_c.getInt(0) + ")")
  println("Task 2.a: Location with the most gold = " + Task_2_a.getString(0))
  println("Task 2.b: Location with the most silver = " + Task_2_b.getString(0))
  println("Task 2.c: Location with the most euro = " + Task_2_c.getString(0))

}
