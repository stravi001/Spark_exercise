import org.apache.spark.sql.{Dataset, Encoder, SaveMode, SparkSession}
import SparkUtils.getSparkSession
object CsvFileToAvro extends App {

  class defaultInputPath(val path: String = "./src/main/resources/inputs")
  class defaultOutputPath(val path: String = "./src/main/resources/outputs")
  class writeCsvToAvroInputs(val inpCsvPath: String, val inpOutputPath: String, val inpDelimiter: String, val inpSparkSession: SparkSession)

  def loadCsvToDataset[T: Encoder](inpCsvPath: String, inpDelimiter: String, inpSparkSession: SparkSession): Dataset[T] = {

    val outDataset = inpSparkSession.read.options(Map("inferSchema" -> "true", "delimiter" -> inpDelimiter, "header" -> "true")).csv(inpCsvPath).as[T]

    outDataset

  }

  def writeCsvToAvro[T : Encoder](inpCsvPath: String, inpOutputPath: String, inpDelimiter: String, inpSparkSession: SparkSession): String = {

    loadCsvToDataset[T](inpCsvPath, inpDelimiter,inpSparkSession).write.format("avro").mode(SaveMode.Overwrite).save(inpOutputPath)

    inpOutputPath

  }

  val sparkSession = getSparkSession

  import sparkSession.implicits._

  val vDefaultInputPath = new defaultInputPath()
  val vDefaultOutputPath = new defaultOutputPath()
  val inpVaultCsv = new writeCsvToAvroInputs(vDefaultInputPath.path + "/csv/valut.csv", vDefaultOutputPath.path + "/avro/vault.avro", ",", sparkSession)
  val inpVaultMasterDataCsv = new writeCsvToAvroInputs(vDefaultInputPath.path + "/csv/vault_master.csv", vDefaultOutputPath.path + "/avro/vault_master.avro", ",", sparkSession)

  writeCsvToAvro[CsvData](inpVaultCsv.inpCsvPath, inpVaultCsv.inpOutputPath, inpVaultCsv.inpDelimiter, inpVaultMasterDataCsv.inpSparkSession)
  writeCsvToAvro[VaultMasterData](inpVaultMasterDataCsv.inpCsvPath, inpVaultMasterDataCsv.inpOutputPath, inpVaultMasterDataCsv.inpDelimiter, inpVaultMasterDataCsv.inpSparkSession)

}
