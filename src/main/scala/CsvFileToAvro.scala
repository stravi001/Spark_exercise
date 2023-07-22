import org.apache.spark.sql.{SaveMode, SparkSession}
object CsvFileToAvro extends App {
  def get_spark_session: SparkSession = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("Spark CSV to Avro")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    spark

  }
  def csv_to_avro(inp_csv_path: String, inp_output_path: String, inp_delimiter: String): Unit = {

    val spark = get_spark_session

    val ds_source = spark.read.options(Map("inferSchema" -> "true", "delimiter" -> inp_delimiter, "header" -> "true")).csv(inp_csv_path)

    ds_source.printSchema()

    ds_source.show()

    ds_source.write.format("avro").mode(SaveMode.Overwrite).save(inp_output_path)

  }

  csv_to_avro("./src/main/resources/valut.csv", "./src/main/resources/vault.avro", ",")

  csv_to_avro("./src/main/resources/vault_master.csv", "./src/main/resources/vault_master.avro", ",")

  /*val spark = GetSparkSession

  val ds_vault = spark.read.options(Map("inferSchema" -> "true", "delimiter" -> ",", "header" -> "true")).csv("./src/main/resources/valut.csv")
  ds_vault.show()
  ds_vault.printSchema()

  ds_vault.write.format("avro").mode(SaveMode.Overwrite).save("./src/main/resources/vault.avro")

  val ds_vault_master = spark.read.options(Map("inferSchema" -> "true", "delimiter" -> ",", "header" -> "true")).csv("./src/main/resources/vault_master.csv")
  ds_vault_master.show()
  ds_vault_master.printSchema()

  ds_vault_master.write.format("avro").mode(SaveMode.Overwrite).save("./src/main/resources/vault_master.avro")*/

}
