import org.apache.spark.sql.{SaveMode, SparkSession}
object CsvFileToAvro extends App {
  def getSparkSession: SparkSession = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("Spark CSV to Avro")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    spark

  }
  def ValutCsvToAvro(inpCsvPath: String, inpOutputPath: String, inpDelimiter: String): Unit = {

    val spark = getSparkSession

    import spark.implicits._

    val dsSource = spark.read.options(Map("inferSchema" -> "true", "delimiter" -> inpDelimiter, "header" -> "true")).csv(inpCsvPath).as[CsvData]

    dsSource.write.format("avro").mode(SaveMode.Overwrite).save(inpOutputPath)

  }

  def VaultMasterCsvToAvro(inpCsvPath: String, inpOutputPath: String, inpDelimiter: String): Unit = {

    val spark = getSparkSession

    import spark.implicits._

    val dsSource = spark.read.options(Map("inferSchema" -> "true", "delimiter" -> inpDelimiter, "header" -> "true")).csv(inpCsvPath).as[VaultMasterData]

    dsSource.write.format("avro").mode(SaveMode.Overwrite).save(inpOutputPath)

  }

  ValutCsvToAvro("./src/main/resources/valut.csv", "./src/main/resources/vault.avro", ",")
  VaultMasterCsvToAvro("./src/main/resources/vault_master.csv", "./src/main/resources/vault_master.avro", ",")

}
