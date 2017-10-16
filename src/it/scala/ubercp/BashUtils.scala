package ubercp

import scala.sys.process._

object BashUtils {
  def `.`: String = System.getProperty("user.dir")

  def executeString(s: String): (String, String, Int) = {
    var stderr = ""
    var stdout = ""
    val exitCode = Seq("bash", "-c", s) ! ProcessLogger(stdout += "\n" + _, stderr += "\n" + _)
    (stdout, stderr, exitCode)
  }

  def executeStringAndPrintln(s: String): Int = {
    println(" + " + s)
    Seq("bash", "-c", s) ! ProcessLogger(println, System.err.println)
  }


  implicit class PimpedString(s: String) {
    def !! : Int = executeStringAndPrintln(s)
    def !? : Unit = {
      val exitCode = executeStringAndPrintln(s)
      require(exitCode == 0, s"Command returned non-zero exit code\nCommand: $s\nExit code: $exitCode")
    }
    def !!! : (String, String, Int) = executeString(s)
  }
}
