package files

import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

class CsvReader(sparkSession: SparkSession) {
  def read[T: Encoder](path: String, delimiter: String): Dataset[T] = {

    println("Reading csv: " + path)

    sparkSession
      .read
      .options(Map("inferSchema" -> "true", "delimiter" -> delimiter, "header" -> "true"))
      .csv(path)
      .as[T]
  }

}

