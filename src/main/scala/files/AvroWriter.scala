package files

import org.apache.spark.sql.{Dataset, Encoder}

class AvroWriter {
  def dsToAvro[T: Encoder](ds: Dataset[T], path: String): Unit = {

    ds
      .write
      .format("avro")
      .mode("overwrite")
      .save(path)

    println("Avro saved to path: " + path)
  }
}