import caseClasses.{CsvData, VaultData, VaultMasterData}
import spark.SparkUtils
import files.{AvroWriter, CsvReader, CsvWriter}
import org.apache.spark.sql.Dataset
import text.Utils.getCount

object Main extends App {

  // konstanty
  val DefaultInputPath = "./src/main/resources/inputs"
  val DefaultOutputPath = "./src/main/resources/outputs"
  val DefaultDelimiter = ","
  val ValueGoldToSilver = 10
  val ValueSilverToEuro = 5.2

  val spark = SparkUtils.getSparkSession

  import spark.implicits._

  val csvReader = new CsvReader(spark)
  val avroWriter = new AvroWriter
  val csvWriter = new CsvWriter

  val csvData = csvReader
    .read[CsvData](DefaultInputPath + "/csv/valut.csv", DefaultDelimiter)
    .persist()

  val vaultMaster = csvReader
    .read[VaultMasterData](DefaultInputPath + "/csv/vault_master.csv", DefaultDelimiter)
    .persist()

  println("Transfer Tasks")
  println("Task 1:")
  avroWriter.dsToAvro(csvData, DefaultOutputPath + "/avro/vault.avro")
  println("Task 2:")
  avroWriter.dsToAvro(vaultMaster, DefaultOutputPath + "/avro/vault_master.avro")

  println("Analysis Tasks")
  println("Part A")
  val recordsCount = csvData.count()
  println("Task 1: How many records does file contain = " + recordsCount)
  val uniqueVaults = csvData.dropDuplicates("vaultId").count()
  println("Task 2: How many vaults does scrooge have according to the csv = "+ uniqueVaults)

  val vaultsContent = csvData.map { mp =>
    VaultData(
      vaultId = mp.vaultId,
      numberOfGoldCoins = getCount(mp.content, "gold"),
      numberOfSilverCoins = getCount(mp.content, "silver"),
      numberOfEuroCoins = getCount(mp.content, "euro")
    )
  }.groupByKey(gk => gk.vaultId)
    .reduceGroups { (row1, row2) =>
      row1.copy(
        numberOfGoldCoins = row1.numberOfGoldCoins + row2.numberOfGoldCoins,
        numberOfSilverCoins = row1.numberOfSilverCoins + row2.numberOfSilverCoins,
        numberOfEuroCoins = row1.numberOfEuroCoins + row2.numberOfEuroCoins
      )
    }.map {mp2 =>
      VaultData(
        mp2._2.vaultId,
        mp2._2.numberOfGoldCoins,
        mp2._2.numberOfSilverCoins,
        mp2._2.numberOfEuroCoins
      )
    }.sort("vaultId")
    .persist()

  println("Task 3: How much gold, silver, euro is contained within every vault:")
  vaultsContent.show()

  val goldWealth = vaultsContent.map { mp =>
    mp.numberOfGoldCoins + mp.numberOfSilverCoins.toDouble / ValueGoldToSilver + mp.numberOfEuroCoins / ValueSilverToEuro / ValueGoldToSilver //numberOfSilverCoins vracelo bez toDouble Long = cele cislo a pote byl spatny vysledek
  }.reduce((row1, row2) => row1 + row2)

  println("Task 4: Scrooge's total wealth in gold = " + goldWealth)

  val silverWealth = vaultsContent.map { mp =>
    mp.numberOfGoldCoins * ValueGoldToSilver + mp.numberOfSilverCoins + mp.numberOfEuroCoins / ValueSilverToEuro
  }.reduce((row1, row2) => row1 + row2)

  println("Task 5: Scrooge's total wealth in silver = " + silverWealth)

  val euroWealth = vaultsContent.map { mp =>
    mp.numberOfGoldCoins * ValueGoldToSilver * ValueSilverToEuro + mp.numberOfSilverCoins * ValueSilverToEuro + mp.numberOfEuroCoins
  }.reduce((row1, row2) => row1 + row2)

  println("Task 6: Scrooge's total wealth in euro = " + euroWealth)

  println("Task 7:")

  csvWriter.dsToCsv(vaultsContent,
    DefaultOutputPath + "/csv/task7.csv",
    DefaultDelimiter)

  println("Part B")

  val csvDataWithMaster: Dataset[(VaultData, VaultMasterData)] = vaultsContent.joinWith(
    vaultMaster,
    vaultsContent("vaultId") === vaultMaster("vaultId"),
    "inner")
    .map(mp =>
      (VaultData(
        vaultId = mp._1.vaultId,
        numberOfGoldCoins = mp._1.numberOfGoldCoins,
        numberOfSilverCoins = mp._1.numberOfSilverCoins,
        numberOfEuroCoins = mp._1.numberOfEuroCoins),
        VaultMasterData(
          vaultId = mp._2.vaultId,
          name = mp._2.name,
          location = mp._2.location)
      )
    )
    .persist()

  val maxGoldCoinsByName = csvDataWithMaster
    .map(mp => (mp._1.numberOfGoldCoins, mp._2.name))
    .groupByKey(gk => gk._2)
    .reduceGroups { (row1, row2) =>
      row1.copy(
        row1._1 + row2._1,
        row1._2
      )
    }.map(mp2 => mp2._2._1)
    .sort($"value".desc)
    .first()

  val maxGoldCoinsVaultName = csvDataWithMaster
    .filter(f => f._1.numberOfGoldCoins == maxGoldCoinsByName)
    .map(mp => mp._2.name)
    .collect()
    .mkString(",")

  println("Task 1.a: Vault with the most gold = " + maxGoldCoinsVaultName)

  val maxSilverCoinsByName = csvDataWithMaster
    .map(mp => (mp._1.numberOfSilverCoins, mp._2.name))
    .groupByKey(gk => gk._2)
    .reduceGroups { (row1, row2) =>
      row1.copy(
        row1._1 + row2._1,
        row1._2
      )
    }.map(mp2 => mp2._2._1)
    .sort($"value".desc)
    .first()

  val maxSilverCoinsVaultName = csvDataWithMaster
    .filter(f => f._1.numberOfSilverCoins == maxSilverCoinsByName)
    .map(mp => mp._2.name)
    .collect()
    .mkString(",")

  println("Task 1.b: Vault with the most silver = " + maxSilverCoinsVaultName)

  val maxEuroCoinsByName = csvDataWithMaster
    .map(mp => (mp._1.numberOfEuroCoins, mp._2.name))
    .groupByKey(gk => gk._2)
    .reduceGroups { (row1, row2) =>
      row1.copy(
        row1._1 + row2._1,
        row1._2
      )
    }.map(mp2 => mp2._2._1)
    .sort($"value".desc)
    .first()

  val maxEuroCoinsVaultName = csvDataWithMaster
    .filter(f => f._1.numberOfEuroCoins == maxEuroCoinsByName)
    .map(mp => mp._2.name)
    .collect()
    .mkString(",")

  println("Task 1.c: Vault with the most euro = " + maxEuroCoinsVaultName)

  val csvDataWithMasterByLocation = csvDataWithMaster
    .map(mp => (mp._1.numberOfGoldCoins, mp._1.numberOfSilverCoins, mp._1.numberOfEuroCoins, mp._2.location))
    .groupByKey(gk => gk._4)
    .reduceGroups { (row1, row2) =>
      row1.copy(
        row1._1 + row2._1,
        row1._2 + row2._2,
        row1._3 + row2._3,
        row1._4)
    }.persist()

  val maxGoldCoinsByLocation = csvDataWithMasterByLocation
    .map(mp => (mp._2._1, mp._2._4))
    .sort($"_1".desc)
    .map(mp2 => mp2._1)
    .first()

  val maxGoldCoinsVaultLocation = csvDataWithMasterByLocation
    .filter(f => f._2._1 == maxGoldCoinsByLocation)
    .map(mp => mp._2._4)
    .collect()
    .mkString(",")

  println("Task 2.a: Location with the most gold = " + maxGoldCoinsVaultLocation)

  val maxSilverCoinsByLocation = csvDataWithMasterByLocation
    .map(mp => (mp._2._2, mp._2._4))
    .sort($"_1".desc)
    .map(mp2 => mp2._1)
    .first()

  val maxSilverCoinsVaultLocation = csvDataWithMasterByLocation
    .filter(f => f._2._2 == maxSilverCoinsByLocation)
    .map(mp => mp._2._4)
    .collect()
    .mkString(",")

  println("Task 2.b: Location with the most silver = " + maxSilverCoinsVaultLocation)

  val maxEuroCoinsByLocation = csvDataWithMasterByLocation
    .map(mp => (mp._2._3, mp._2._4))
    .sort($"_1".desc)
    .map(mp2 => mp2._1)
    .first()

  val maxEuroCoinsVaultLocation = csvDataWithMasterByLocation
    .filter(f => f._2._3 == maxEuroCoinsByLocation)
    .map(mp => mp._2._4)
    .collect()
    .mkString(",")

  println("Task 2.c: Location with the most euro = " + maxEuroCoinsVaultLocation)

  spark.close()

}
