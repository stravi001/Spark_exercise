package caseClasses

import org.apache.spark.sql.SparkSession
case class WriteCsvToAvroInputs(inpCsvPath: String, inpOutputPath: String, inpDelimiter: String, inpSparkSession: SparkSession)
