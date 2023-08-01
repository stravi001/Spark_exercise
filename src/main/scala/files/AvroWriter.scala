package files

import org.apache.spark.sql.{Dataset, Encoder, SaveMode}

class AvroWriter {
  def dsToAvro[T: Encoder](ds: Dataset[T], path: String): Unit = {

    ds
      .write
      .format("avro")
      .mode(SaveMode.Overwrite)
      .save(path)

    println(s"Avro saved to path: $path")
  }
}