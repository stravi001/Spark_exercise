package caseClasses
import org.apache.spark.sql.SparkSession
case class LoadCsvToDatasetInputs(inpCsvPath: String, inpDelimiter: String = ",", inpSparkSession: SparkSession)
