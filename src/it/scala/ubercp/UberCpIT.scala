package ubercp

import org.specs2.mutable.Specification
import BashUtils._
import org.apache.spark.sql.SparkSession

object UberCpIT extends Specification {
  val tmpName = "uber-cp-tmp"

  s"rm -r $tmpName".!!
  s"mkdir $tmpName".!?

  val tmpPath = "pwd".!!!._1 + "/" + tmpName

  "UberCp" should {
    assert("./bin/build.sh".!! == 0, "Build failed")

    "Output help documentation" in {
      "./bin/help.sh".!! must_=== 0
    }

    val ss = SparkSession.builder().appName("uber-cp-it").master("local").getOrCreate()

    "Coalesce 100 text files to 10 text files correctly" in {
      s"mkdir $tmpPath/100-text-files".!?

      (1 to 100).map(i => "echo \"some data " + i  + "\" > " + tmpPath + "/100-text-files/part-" + i).foreach(_.!?)

      s"./bin/run-local.sh -f text -i $tmpPath/100-text-files -o $tmpPath/10-text-files".!! must_=== 0

      ss.sparkContext.textFile(s"$tmpPath/10-text-files").collect().toList must_===
        (1 to 100).map("some data " + _).toList
    }

    "Coalesce 100 parquet files to 10 parquet files correctly" in {
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