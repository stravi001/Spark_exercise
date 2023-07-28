import org.apache.spark.sql.SparkSession

class loadCsvToDatasetInputs(val inpCsvPath: String, val inpDelimiter: String = ",", val inpSparkSession: SparkSession)
