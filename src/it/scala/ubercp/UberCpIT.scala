package ubercp

import org.specs2.mutable.Specification
import BashUtils._

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

    "Coalesce 100 text files to 10 text files correctly" in {
      failure("write me")
    }

    "Coalesce 100 parquet files to 10 parquet files correctly" in {
      failure("write me")
    }

    "Convert 10 tsv files to 5 parquet files correctly" in {
      failure("write me")
    }

    "Give error message for unsupported argument combination" in {
      failure("write me")
    }
  }
}