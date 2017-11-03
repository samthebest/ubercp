package ubercp

trait ColumnType
case object Value extends ColumnType
case object Attribute extends ColumnType
case object Index extends ColumnType

case class Path(path: List[String])

object Path {
  def apply(s: String): Path = Path(List(s))

  type Record = Map[(Path, ColumnType), Any]

  implicit class PimpedPath(p: Path) {
    def /(s: String): Path = Path(p.path :+ s)
  }
}
