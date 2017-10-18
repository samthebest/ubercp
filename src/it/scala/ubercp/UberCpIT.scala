package ubercp

import java.io.{File, PrintWriter}

import org.specs2.mutable.Specification
import BashUtils._
import org.apache.spark.sql.SparkSession

object UberCpIT extends Specification {
  sequential

  "UberCp" should {
    val tmpPath = ("pwd".!!!._1 + "/uber-cp-tmp").trim()

    println("tmpPath = " + tmpPath)

    s"rm -r $tmpPath".!!
    s"mkdir $tmpPath".!?

    assert("./bin/build.sh | grep -v \"Including from cache\" | grep -v \"Merging\"".!! == 0, "Build failed")

    "Output help documentation" in {
      "./bin/help.sh".!! must_=== 0
    }

    def writeToFile(p: String, s: String): Unit = {
      val pw = new PrintWriter(new File(p))
      try pw.write(s) finally pw.close()
    }

    val ss = SparkSession.builder().appName("uber-cp-it").master("local").getOrCreate()

    // FIXME Tried putting log4j.properties in src/main temporarily for the build to supress logs, but this doesn't
    // work anymore.
    def runUberCp(format: String, inName: String, outName: String, n: Int, outFormat: Option[String] = None): Int =
      (s"./bin/uber-cp-local.sh -f $format " + outFormat.map("--format-out " + _ + " ").getOrElse("") +
        s"-i $tmpPath/$inName -o $tmpPath/$outName -n $n").!!

    def countFiles(name: String): Long = ss.sparkContext.binaryFiles(s"$tmpPath/$name").count()

    "Coalesce 100 text files to 10 text files correctly" in {
      s"mkdir $tmpPath/100-text-files".!?

      (1 to 100).foreach(i => writeToFile(tmpPath + "/100-text-files/part-" + i, "some data " + i))

      runUberCp("text", "100-text-files", "10-text-files", 10) must_=== 0

      countFiles("10-text-files") must_=== 10

      ss.sparkContext.textFile(s"$tmpPath/10-text-files").collect().toList.sorted must_===
        (1 to 100).map("some data " + _).toList.sorted
    }

    // TODO Use different data for each test to make tests more robust
    val data = (1 to 5000).map(i => DummyData("some data", i)).toList

    "Coalesce 30 parquet files to 5 parquet files correctly" in {
      import ss.implicits._

      ss.sparkContext.makeRDD(data).repartition(30).toDS()
      .write.parquet(s"$tmpPath/30-parquet-files")

      runUberCp("parquet", "30-parquet-files", "5-parquet-files", 5, Some("parquet")) must_=== 0

      countFiles("5-parquet-files") must_=== 5

      ss.read.parquet(s"$tmpPath/5-parquet-files").as[DummyData].collect().toList.sortBy(_.bar) must_=== data
    }

    "Coalesce 40 tsv files to 7 parquet files correctly" in {
      import ss.implicits._

      ss.sparkContext.makeRDD(data).repartition(40).toDS()
      .write.format("com.databricks.spark.csv").option("delimiter", "\t").save(s"$tmpPath/40-tsv-files")

      runUberCp("tsv", "40-tsv-files", "7-parquet-files", 7, Some("parquet")) must_=== 0

      countFiles("/7-parquet-files") must_=== 7

      ss.read.parquet(s"$tmpPath/7-parquet-files").withColumnRenamed("_c0", "foo").withColumnRenamed("_c1", "bar")
      .as[DummyDataString].collect().toList.map(_.toDummyData).sortBy(_.bar) must_=== data
    }

    "Coalesce 40 csv files to 8 parquet files correctly" in {
      import ss.implicits._

      ss.sparkContext.makeRDD(data).repartition(40).toDS()
      .write.format("com.databricks.spark.csv").save(s"$tmpPath/40-csv-files")

      runUberCp("csv", "40-csv-files", "8-parquet-files", 8, Some("parquet")) must_=== 0

      countFiles("/8-parquet-files") must_=== 8

      ss.read.parquet(s"$tmpPath/8-parquet-files").withColumnRenamed("_c0", "foo").withColumnRenamed("_c1", "bar")
      .as[DummyDataString].collect().toList.map(_.toDummyData).sortBy(_.bar) must_=== data
    }

    "Give error message for Unsupported argument combination" in {
      (s"./bin/uber-cp-local.sh -f text --format-out parquet  " +
        s"-i $tmpPath/100-text-files -o $tmpPath/10-text-files-foo -n 10").!!!
      ._2 must contain("Unsupported argument combination: ")
    }

    "Convert 10 tsv files to 5 parquet files correctly" in {
      pending
    }
  }
}

case class DummyData(foo: String, bar: Int)
case class DummyDataString(foo: String, bar: String) {
  def toDummyData: DummyData = DummyData(foo, bar.toInt)
}