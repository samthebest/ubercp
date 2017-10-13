package ubercp

import org.specs2.mutable.Specification
import BashUtils._

object UberCpIT extends Specification {
  val tmpName = "uber-cp-tmp"

  s"rm -r $tmpName && mkdir $tmpName".!?

  val tmpPath = "pwd".!!._1 + "/" + tmpName

  "UberCp" should {
    "pass" in {
      success
    }
  }
}