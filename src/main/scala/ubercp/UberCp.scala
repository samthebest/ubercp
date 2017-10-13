package ubercp

import org.apache.spark.sql.SparkSession
import org.rogach.scallop._

object UberCp {
  def main(args: Array[String]): Unit = {
    val conf = new ScallopConf(args) {
      val numFiles = opt[Int](default = Some(1),
        descr = "The number of files to output. Default = 1")
      val shuffle = opt[Boolean](default = Some(false),
        descr = "Will shuffle data. Mandatory if you want to increase the number of files")
      val formatIn = opt[String](default = Some("text"),
        descr = "The format of input files. Supported options are: text, parquet, csv, tsv")
      val formatOut = opt[String](default = Some("text"),
        descr = "The format of output files. Supported options are: text, parquet, csv, tsv")
      val inPath = opt[String](descr = "The input path")
      val outPath = opt[String](descr = "The output path")
      val partitioned = opt[Boolean](default = Some(false), descr = "If the input data is partitioned, set this to" +
        " true (and uber cp will preserve partitioning.  If set to false, it will break.")
      verify()
    }

    val ss = SparkSession.builder().appName("uber-cp").getOrCreate()

    ss

//    sqlContext.read.parquet(in).coalesce(numFiles).write.parquet(out)


  }
}
