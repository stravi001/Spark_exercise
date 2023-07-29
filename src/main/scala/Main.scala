import sparkUtils.SparkUtils
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.functions._
import java.io.{File, FileOutputStream, PrintWriter}
import org.apache.spark.sql.{Dataset, SaveMode}
import caseClasses._
import utils._

object Main extends App {

  val logger = Logger("Logger")

  val sparkUtils = new SparkUtils

  val spark = sparkUtils.getSparkSession

  import spark.implicits._

  val classUtils = new Utils
  val classCsvFileToAvro = new CsvFileToAvro

  val inpVaultCsv = WriteCsvToAvroInputs(classUtils.defaultInputPath + "/csv/valut.csv", classUtils.defaultOutputPath + "/avro/vault.avro", ",", spark)
  val inpVaultMasterDataCsv = WriteCsvToAvroInputs(classUtils.defaultInputPath + "/csv/vault_master.csv", classUtils.defaultOutputPath + "/avro/vault_master.avro", ",", spark)

  val write = new PrintWriter(new FileOutputStream(new File(classUtils.defaultOutputPath + "/mainOutput.txt"),false))

  write.write("Transfer Tasks")
  write.write("\r")
  write.write("Task 1: File converted to: ")
  write.write(classCsvFileToAvro.writeCsvToAvro[CsvData](inpVaultCsv.inpCsvPath, inpVaultCsv.inpOutputPath, inpVaultCsv.inpDelimiter, inpVaultMasterDataCsv.inpSparkSession))
  write.write("\r")
  write.write("Task 2: File converted to: ")
  write.write(classCsvFileToAvro.writeCsvToAvro[VaultMasterData](inpVaultMasterDataCsv.inpCsvPath, inpVaultMasterDataCsv.inpOutputPath, inpVaultMasterDataCsv.inpDelimiter, inpVaultMasterDataCsv.inpSparkSession))
  write.write("\r")
  write.write("\r")

  val vaultCsvToDatasetInputs = LoadCsvToDatasetInputs(inpCsvPath = classUtils.defaultInputPath + "/csv/valut.csv", inpSparkSession = spark)
  val dsVault = classCsvFileToAvro.loadCsvToDataset[CsvData](vaultCsvToDatasetInputs.inpCsvPath, vaultCsvToDatasetInputs.inpDelimiter, vaultCsvToDatasetInputs.inpSparkSession)

  write.write("Analysis Tasks")
  write.write("\r")
  write.write("Part A")
  write.write("\r")
  write.write("Task 1: How many records does file contain = " + dsVault.count())
  write.write("\r")
  write.write("Task 2: How many vaults does scrooge have according to the csv = " + dsVault.dropDuplicates("vaultId").count())

  val dsContent = dsVault.map(mp => (mp.vaultId, classUtils.getCount(mp.content, "gold"), classUtils.getCount(mp.content, "silver"), classUtils.getCount(mp.content, "euro")))
    .groupByKey(gk => gk._1)
    .agg(sum(col("_2")).as[Long], sum(col("_3")).as[Long], sum(col("_4")).as[Long])
    .map(mp2 => (mp2._1, mp2._2, mp2._3, mp2._4))
    .sort(asc("_1"))
    .withColumnRenamed("_1", "vaultId")
    .withColumnRenamed("_2", "numberOfGoldCoins")
    .withColumnRenamed("_3", "numberOfSilverCoins")
    .withColumnRenamed("_4", "numberOfEuroCoins")
    .as[VaultData]

  dsContent.coalesce(1).write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).option("header", "true").save(classUtils.defaultOutputPath + "/task_3")

  write.write("\r")
  write.write("Task 3: Dataset saved to: " + classUtils.defaultOutputPath + "/task_3")
  write.write("\r")
  write.write("Task 4: Scrooge's total wealth in gold = " + dsContent.map(content => content.numberOfGoldCoins + (content.numberOfSilverCoins + (content.numberOfEuroCoins/5.2))/10).agg(sum("value")).first.getDouble(0))
  write.write("\r")
  write.write("Task 5: Scrooge's total wealth in silver = " + dsContent.map(content => content.numberOfGoldCoins*10 + content.numberOfSilverCoins + content.numberOfEuroCoins/5.2).agg(sum("value")).first.getDouble(0))
  write.write("\r")
  write.write("Task 6: Scrooge's total wealth in euro = " + dsContent.map(content => ((content.numberOfGoldCoins*10) + content.numberOfSilverCoins)*5.2 + content.numberOfEuroCoins).agg(sum("value")).first.getDouble(0))

  dsContent.write.format("avro").mode(SaveMode.Overwrite).save(classUtils.defaultOutputPath + "/task_7")

  write.write("\r")
  write.write("\r")
  write.write("Part B")
  write.write("\r")

  val vaultMasterCsvToDatasetInputs = LoadCsvToDatasetInputs(inpCsvPath = classUtils.defaultInputPath + "/csv/vault_master.csv", inpSparkSession = spark)
  val dsVaultMaster = classCsvFileToAvro.loadCsvToDataset[VaultMasterData](vaultMasterCsvToDatasetInputs.inpCsvPath, vaultMasterCsvToDatasetInputs.inpDelimiter, vaultMasterCsvToDatasetInputs.inpSparkSession)
  val dsJoinedDataAndMaster: Dataset[(VaultData, VaultMasterData)] = dsContent.joinWith(dsVaultMaster, dsContent("vaultID") === dsVaultMaster("vaultID"), "inner")

  //Task  1.a
  val vaultMaxGoldCoins = dsJoinedDataAndMaster
    .map(mp => (mp._2.name, mp._1.numberOfGoldCoins))
    .groupByKey(gk => gk._1)
    .agg(sum(col("_2")).as[Long])
    .sort(desc("sum(_2)"))
    .first()

  //Task  1.b
  val valueVaultMaxSilverCoins = dsJoinedDataAndMaster
    .map(mp => (mp._2.name, mp._1.numberOfSilverCoins))
    .groupByKey(gk => gk._1)
    .agg(sum(col("_2")).as[Long])
    .sort(desc("sum(_2)"))
    .first()

  val vaultMaxSilverCoins = dsJoinedDataAndMaster
    .map(mp => (mp._2.name, mp._1.numberOfSilverCoins))
    .filter(f => f._2 == valueVaultMaxSilverCoins._2)
    .map(mp2 => mp2._1)
    .collect()
    .mkString(",")

  //Task  1.c
  val valueVaultMaxEuroCoins = dsJoinedDataAndMaster
    .map(mp => (mp._2.name, mp._1.numberOfEuroCoins))
    .groupByKey(gk => gk._1)
    .agg(sum(col("_2")).as[Long])
    .map(mp2 => (mp2._1, mp2._2))
    .agg(max(col("_2")).as[Long])
    .first()

  val vaultMaxEuroCoins = dsJoinedDataAndMaster
    .map(mp => (mp._2.name, mp._1.numberOfEuroCoins))
    .filter(f => f._2 == valueVaultMaxEuroCoins.getLong(0))
    .map(mp2 => mp2._1)
    .collect()
    .mkString(",")

  //Task  2.a
  val locationMaxGoldCoins = dsJoinedDataAndMaster
    .map(mp => (mp._2.location, mp._1.numberOfGoldCoins))
    .groupByKey(gk => gk._1)
    .agg(sum(col("_2")).as[Long])
    .sort(desc("sum(_2)"))
    .first()

  //Task  2.b
  val valueLocationAggSilverCoins = dsJoinedDataAndMaster
    .map(mp => (mp._2.location, mp._1.numberOfSilverCoins))
    .groupByKey(gk => gk._1)
    .agg(sum(col("_2")).as[Long])

  val valueLocationMaxSilverCoins = valueLocationAggSilverCoins
    .sort(desc("sum(_2)"))
    .first()

  val locationMaxSilverCoins = valueLocationAggSilverCoins
    .filter(f => f._2 == valueLocationMaxSilverCoins._2)
    .map(mp => mp._1)
    .collect()
    .mkString(",")

  //Task  2.c
  val valueLocationAggEuroCoins = dsJoinedDataAndMaster
    .map(mp => (mp._2.location, mp._1.numberOfEuroCoins))
    .groupByKey(gk => gk._1)
    .agg(sum(col("_2")).as[Long])

  val valueLocationMaxEuroCoins = valueLocationAggEuroCoins
    .agg(max(col("sum(_2)")).as[Long])
    .first()

  val locationMaxEuroCoins = valueLocationAggEuroCoins
    .filter(f => f._2 == valueLocationMaxEuroCoins.getLong(0))
    .map(mp2 => mp2._1)
    .collect()
    .mkString(",")

  write.write("Task 1.a: Vault with the most gold = " + vaultMaxGoldCoins._1)
  write.write("\r")
  write.write("Task 1.b: Vault with the most silver = " + vaultMaxSilverCoins)
  write.write("\r")
  write.write("Task 1.c: Vault with the most euro = " + vaultMaxEuroCoins)
  write.write("\r")
  write.write("Task 2.a: Location with the most gold = " + locationMaxGoldCoins._1)
  write.write("\r")
  write.write("Task 2.b: Location with the most silver = " + locationMaxSilverCoins)
  write.write("\r")
  write.write("Task 2.c: Location with the most euro = " + locationMaxEuroCoins)

  write.close()

}
