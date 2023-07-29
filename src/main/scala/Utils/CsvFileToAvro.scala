package utils

import org.apache.spark.sql.{Dataset, Encoder, SaveMode, SparkSession}

class CsvFileToAvro {

  def loadCsvToDataset[T: Encoder](inpCsvPath: String, inpDelimiter: String, inpSparkSession: SparkSession): Dataset[T] = {

    val outDataset = inpSparkSession.read.options(Map("inferSchema" -> "true", "delimiter" -> inpDelimiter, "header" -> "true")).csv(inpCsvPath).as[T]

    outDataset

  }

  def writeCsvToAvro[T: Encoder](inpCsvPath: String, inpOutputPath: String, inpDelimiter: String, inpSparkSession: SparkSession): String = {

    loadCsvToDataset[T](inpCsvPath, inpDelimiter, inpSparkSession).write.format("avro").mode(SaveMode.Overwrite).save(inpOutputPath)

    inpOutputPath

  }


}
