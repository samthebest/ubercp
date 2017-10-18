package ubercp

import org.apache.spark.sql.SparkSession
import org.rogach.scallop._

object UberCp {
  // TODO Compression codecs
  def main(args: Array[String]): Unit = {
    val conf = new ScallopConf(args) {
      // TODO Nicer behaviour would be to preserve numFiles as default
      val numFiles = opt[Int](default = Some(1),
        descr = "The number of files to output. Default = 1")
      val shuffle = opt[Boolean](default = Some(false),
        descr = "Will shuffle data. ATM (see SPARK-5997) Mandatory if you want to increase the number of files")
      val formatIn = opt[String](default = Some("text"),
        descr = "The format of input files. Supported options are: text, parquet, csv, tsv. Default = text")
      val formatOut = opt[String](default = Some(formatIn()),
        descr = "The format of output files. Supported options are: text, parquet, csv, tsv. Default = format-in")
      val inPath = opt[String](descr = "The input path")
      val outPath = opt[String](descr = "The output path")
      val partitioned = opt[Boolean](default = Some(false), descr = "If the input data is partitioned, set this to" +
        " true (and uber cp will preserve partitioning.  If set to false, it will break.")
      val master = opt[String](default = Some("local"), descr = "Master URL for Spark. Default = local")
      verify()
    }

    val ss = SparkSession.builder().appName("uber-cp").master(conf.master()).getOrCreate()

    (conf.formatIn(), conf.formatOut(), conf.shuffle(), conf.partitioned()) match {
      case ("text", "text", false, false) =>

        ss.sparkContext.textFile(conf.inPath()).coalesce(conf.numFiles()).saveAsTextFile(conf.outPath())
      case ("parquet", "parquet", false, false) =>
        // Have to use repartition instead of coalesce thanks to Spark regressions SPARK-17998 and SPARK-20144
        ss.read.parquet(conf.inPath())
        .repartition(conf.numFiles())
        .write.parquet(conf.outPath())

      case ("tsv", "parquet", false, false) =>
        // Have to use repartition instead of coalesce thanks to Spark regressions SPARK-17998 and SPARK-20144

        ss.read.format("com.databricks.spark.csv").option("delimiter", "\t").load(conf.inPath())
        .repartition(conf.numFiles())
        .write.parquet(conf.outPath())

    case (inf, outf, shuffle, partitioned) =>
      throw new UnsupportedOperationException("Unsupported argument combination: " +
        s"formatIn = $inf, formatOut = $outf, shuffle = $shuffle, partitioned = $partitioned")

  }
}

}
