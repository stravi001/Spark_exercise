package files

import org.apache.spark.sql.{Dataset, Encoder, SaveMode}
class CsvWriter {
  def dsToCsv[T: Encoder](ds: Dataset[T], path: String, delimiter: String): Unit = {

    ds
      .coalesce(1)
      .write
      .option("header", "true")
      .option("sep", delimiter)
      .mode("overwrite")
      .csv(path)

    println(s"Csv saved to path: $path")
  }
}
