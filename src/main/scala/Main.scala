import loggerUtils.vLogger
import SparkUtils.getSparkSession
import CsvFileToAvro._
import org.apache.spark.sql.functions._
import utils.getCount

import java.io.{File, FileOutputStream, PrintWriter}
import org.apache.spark.sql.{Dataset, Row, SaveMode}
object Main extends App {

  val logger = vLogger

  val spark = getSparkSession

  import spark.implicits._

  val vDefaultInputPath = new defaultInputPath()
  val vDefaultOutputPath = new defaultOutputPath()
  val inpVaultCsv = new writeCsvToAvroInputs(vDefaultInputPath.path + "/csv/valut.csv", vDefaultOutputPath.path + "/avro/vault.avro", ",", spark)
  val inpVaultMasterDataCsv = new writeCsvToAvroInputs(vDefaultInputPath.path + "/csv/vault_master.csv", vDefaultOutputPath.path + "/avro/vault_master.avro", ",", spark)

  val write = new PrintWriter(new FileOutputStream(new File(vDefaultOutputPath.path + "/mainOutput.txt"),false))

  println()

  write.write("Transfer Tasks\r")
  write.write("Task 1: File converted to: ")
  write.write(writeCsvToAvro[CsvData](inpVaultCsv.inpCsvPath, inpVaultCsv.inpOutputPath, inpVaultCsv.inpDelimiter, inpVaultMasterDataCsv.inpSparkSession))
  write.write("\r")
  write.write("Task 2: File converted to: ")
  write.write(writeCsvToAvro[VaultMasterData](inpVaultMasterDataCsv.inpCsvPath, inpVaultMasterDataCsv.inpOutputPath, inpVaultMasterDataCsv.inpDelimiter, inpVaultMasterDataCsv.inpSparkSession))
  write.write("\r")
  write.write("\r")

  val vaultCsvToDatasetInputs = new loadCsvToDatasetInputs(inpCsvPath = vDefaultInputPath.path + "/csv/valut.csv", inpSparkSession = spark)
  val dsVault = loadCsvToDataset[CsvData](vaultCsvToDatasetInputs.inpCsvPath, vaultCsvToDatasetInputs.inpDelimiter, vaultCsvToDatasetInputs.inpSparkSession)

  write.write("Analysis Tasks")
  write.write("\r")
  write.write("Part A")
  write.write("\r")
  write.write("Task 1: How many records does file contain = " + dsVault.count())
  write.write("\r")
  write.write("Task 2: How many vaults does scrooge have according to the csv = " + dsVault.dropDuplicates("vaultId").count())

  val dsContent = dsVault.map(mp => (mp.vaultId, getCount(mp.content, "gold"), getCount(mp.content, "silver"), getCount(mp.content, "euro")))
    .groupByKey(gk => gk._1)
    .agg(sum(col("_2")).as[Long], sum(col("_3")).as[Long], sum(col("_4")).as[Long])
    .map(mp2 => (mp2._1, mp2._2, mp2._3, mp2._4))
    .sort(asc("_1"))
    .withColumnRenamed("_1", "vaultId")
    .withColumnRenamed("_2", "numberOfGoldCoins")
    .withColumnRenamed("_3", "numberOfSilverCoins")
    .withColumnRenamed("_4", "numberOfEuroCoins")
    .as[VaultData]

  dsContent.coalesce(1).write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).option("header", "true").save(vDefaultOutputPath.path+ "/task_3")

  write.write("\r")
  write.write("Task 3: Dataset saved to: " + vDefaultOutputPath.path+ "/task_3")
  write.write("\r")
  write.write("Task 4: Scrooge's total wealth in gold = " + dsContent.map(content => content.numberOfGoldCoins + (content.numberOfSilverCoins + (content.numberOfEuroCoins/5.2))/10).agg(sum("value")).first.getDouble(0))
  write.write("\r")
  write.write("Task 5: Scrooge's total wealth in silver = " + dsContent.map(content => content.numberOfGoldCoins*10 + content.numberOfSilverCoins + content.numberOfEuroCoins/5.2).agg(sum("value")).first.getDouble(0))
  write.write("\r")
  write.write("Task 6: Scrooge's total wealth in euro = " + dsContent.map(content => ((content.numberOfGoldCoins*10) + content.numberOfSilverCoins)*5.2 + content.numberOfEuroCoins).agg(sum("value")).first.getDouble(0))

  dsContent.write.format("avro").mode(SaveMode.Overwrite).save(vDefaultOutputPath.path+ "/task_7")

  write.write("\r")
  write.write("\r")
  write.write("Part B")
  write.write("\r")

  val vaultMasterCsvToDatasetInputs = new loadCsvToDatasetInputs(inpCsvPath = vDefaultInputPath.path + "/csv/vault_master.csv", inpSparkSession = spark)
  val dsVaultMaster = loadCsvToDataset[VaultMasterData](vaultMasterCsvToDatasetInputs.inpCsvPath, vaultMasterCsvToDatasetInputs.inpDelimiter, vaultMasterCsvToDatasetInputs.inpSparkSession)
  val dsJoinedDataAndMaster: Dataset[(VaultData, VaultMasterData)] = dsContent.joinWith(dsVaultMaster, dsContent("vaultID") === dsVaultMaster("vaultID"), "inner")

  //TODO: proc to ku... nefunguje stejne jako lokalita?
  val vaultMaxGoldCoins = dsJoinedDataAndMaster
    .map(mp => (mp._1.numberOfGoldCoins, mp._2.name))
    .groupByKey(gk => gk._2)
    .agg(sum(col("_1")).as[Long])
    .sort(desc("sum(_1)"))
    .first()

  val vaultMaxSilverCoins = dsJoinedDataAndMaster
    .map(mp => (mp._1.numberOfSilverCoins, mp._2.name))
    .groupByKey(gk => gk._2)
    .agg(sum(col("_1")).as[Long])
    .sort(desc("sum(_1)"))
    .first()

  val vaultMaxEuroCoins= dsJoinedDataAndMaster
    .map(mp => (mp._1.numberOfEuroCoins, mp._2.name))
    .groupByKey(gk => gk._2)
    .agg(sum(col("_1")).as[Long])
    .sort(desc("sum(_1)"))
    .first()

  val locationMaxGoldCoins = dsJoinedDataAndMaster
    .map(mp => (mp._1.numberOfGoldCoins, mp._2.location))
    .groupByKey(gk => gk._2)
    .agg(sum(col("_1")).as[Long])
    .sort(desc("sum(_1)"))
    .first()

  val locationMaxSilverCoins = dsJoinedDataAndMaster
    .map(mp => (mp._1.numberOfSilverCoins, mp._2.location))
    .groupByKey(gk => gk._2)
    .agg(sum(col("_1")).as[Long], max(col("_1")).as[Long])
    .map(mp2 => (mp2._1, mp2._2))
    .first()

  val locationMaxEuroCoins = dsJoinedDataAndMaster
    .map(mp => (mp._1.numberOfEuroCoins, mp._2.location))
    .groupByKey(gk => gk._2)
    .agg(sum(col("_1")).as[Long])
    .sort(desc("sum(_1)"))
    .first()

  write.write("Task 1.a: Vault with the most gold = " + vaultMaxGoldCoins._1)
  write.write("\r")
  write.write("Task 1.b: Vault with the most silver = " + vaultMaxSilverCoins._1)
  write.write("\r")
  write.write("Task 1.c: Vault with the most euro = " + vaultMaxEuroCoins._1)
  write.write("\r")
  write.write("Task 2.a: Location with the most gold = " + locationMaxGoldCoins._1)
  write.write("\r")
  write.write("Task 2.b: Location with the most silver = " + locationMaxSilverCoins._1)
  write.write("\r")
  write.write("Task 2.c: Location with the most euro = " + locationMaxEuroCoins._1)

  write.close()

}
