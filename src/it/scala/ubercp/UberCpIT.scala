package ubercp

import java.io.{File, PrintWriter}

import org.specs2.mutable.Specification
import BashUtils._
import org.apache.spark.sql.SparkSession

object UberCpIT extends Specification {
  "UberCp" should {
    val tmpPath = ("pwd".!!!._1 + "/uber-cp-tmp").trim()

    println("tmpPath = " + tmpPath)

    s"rm -r $tmpPath".!!
    s"mkdir $tmpPath".!?

    assert("./bin/build.sh".!! == 0, "Build failed")

    "Output help documentation" in {
      "./bin/help.sh".!! must_=== 0
    }

    def writeToFile(p: String, s: String): Unit = {
      val pw = new PrintWriter(new File(p))
      try pw.write(s) finally pw.close()
    }

    val ss = SparkSession.builder().appName("uber-cp-it").master("local").getOrCreate()

    "Coalesce 100 text files to 10 text files correctly" in {
      s"mkdir $tmpPath/100-text-files".!?

      (1 to 100).foreach(i => writeToFile(tmpPath + "/100-text-files/part-" + i, "some data " + i))

      s"./bin/run-local.sh -f text -i $tmpPath/100-text-files -o $tmpPath/10-text-files -n 10".!! must_=== 0

      println("10 text files: " + s"$tmpPath/10-text-files")

      ss.sparkContext.textFile(s"$tmpPath/10-text-files").collect().toList.sorted must_===
        (1 to 100).map("some data " + _).toList.sorted
    }

    "Coalesce 30 parquet files to 5 parquet files correctly" in {
      failure("write me")
    }

    "Give error message for unsupported argument combination" in {
      failure("write me")
    }

    "Convert 10 tsv files to 5 parquet files correctly" in {
      pending
    }
  }
}