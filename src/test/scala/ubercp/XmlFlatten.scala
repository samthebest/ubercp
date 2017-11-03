package ubercp

import org.specs2.mutable.Specification
import Path._

object XmlFlatten extends Specification {
  sequential

  val xmlTextOnly =
    """
      |<note>
      |<to>Tove</to>
      |<from>Jani</from>
      |<cc>Bob</cc>
      |<heading>Reminder</heading>
      |<body>Don't forget me this weekend!</body>
      |</note>
      |
      |<note>
      |<to>Alice</to>
      |<from>Bob</from>
      |<bcc>Fred</bcc>
      |<heading>JFDI</heading>
      |<subject>JFDI!</subject>
      |<body>Buy socks</body>
      |</note>
    """.stripMargin

  // TODO Auto detect Double, Int, and Boolean (true, false, and Yes/No)

  implicit def toPath(s: String): Path = Path(s)

  // Should write this first, and only implement schema discovery that is necessary
  val flattenedTextOnly: List[Record] = List(
    Map(
      (Path("to"), Value) -> "Tove",
      (Path("from"), Value) -> "Jani",
      (Path("cc"), Value) -> "Bob",
      (Path("heading"), Value) -> "Reminder",
      (Path("body"), Value) -> "Don't forget me this weekend!"
    ),
    Map(
      (Path("to"), Value) -> "Alice",
      (Path("from"), Value) -> "Bob",
      (Path("bcc"), Value) -> "Fred",
      (Path("heading"), Value) -> "JFDI",
      (Path("subject"), Value) -> "JFDI!",
      (Path("body"), Value) -> "Buy socks"
    )
  )

  val xmlWithAttributes =
    """
      |<person sex="female">
      |  <firstname>Anna</firstname>
      |  <lastname>Smith</lastname>
      |</person>
      |
      |<person sex="male">
      |  <sex>M</sex>
      |  <firstname>Fred</firstname>
      |</person>
    """.stripMargin

  val flattenedWithAttributes: List[Record] = List(
    Map(
      (Path("sex"), Attribute) -> "female",
      (Path("firstname"), Value) -> "Anna",
      (Path("lastname"), Value) -> "Smith"
    ),
    Map(
      (Path("sex"), Attribute) -> "male",
      (Path("firstname"), Value) -> "Fred",
      (Path("sex"), Value) -> "M"
    )
  )

  val xmlWithArray =
    """
      |<numbers>
      |    <value>three</value>
      |    <value>two</value>
      |    <value>one</value>
      |</numbers>
      |
      |<numbers>
      |    <value>one</value>
      |    <value>two</value>
      |    <value>three</value>
      |</numbers>
    """.stripMargin

  // May want optional record GUID generation if it's assumed the record doesn't already have an ID
  val flattenedWithArray: List[Record] = List(
    Map(
      (Path("value"), Value) -> "three",
      (Path("value"), Index) -> 0
    ),
    Map(
      (Path("value"), Value) -> "two",
      (Path("value"), Index) -> 1
    ),
    Map(
      (Path("value"), Value) -> "one",
      (Path("value"), Index) -> 2
    ),
    Map(
      (Path("value"), Value) -> "one",
      (Path("value"), Index) -> 0
    ),
    Map(
      (Path("value"), Value) -> "two",
      (Path("value"), Index) -> 1
    ),
    Map(
      (Path("value"), Value) -> "three",
      (Path("value"), Index) -> 2
    )
  )

  val nestedXml =
    """
      |<person sex="female">
      |  <family>
      |    <brother>Bob</brother>
      |    <sister>Alice</sister>
      |  </family>
      |  <lastname>Smith</lastname>
      |</person>
      |
      |<person sex="male">
      |  <sex>M</sex>
      |  <firstname>Fred</firstname>
      |</person>
    """.stripMargin

  val flattenedNestedXml: List[Record] = List(
    Map(
      (Path("sex"), Attribute) -> "female",
      (Path(List("family", "brother")), Value) -> "Bob",
      (Path(List("family", "sister")), Value) -> "Alice",
      (Path("lastname"), Value) -> "Smith"
    ),
    Map(
      (Path("sex"), Attribute) -> "male",
      (Path("firstname"), Value) -> "Fred",
      (Path("sex"), Value) -> "M"
    )
  )

  val nestedWithArrays =
    """
      |<person>
      |  <family>
      |    <brother>Bob</brother>
      |    <brother>Bill</brother>
      |    <sister>Alice</sister>
      |  </family>
      |  <lastname>Smith</lastname>
      |</person>
      |
      |<person sex="male">
      |  <sex>M</sex>
      |  <firstname>Fred</firstname>
      |</person>
    """.stripMargin

  val flattenedNestedWithArrays: List[Record] = List(
    Map(
      (Path("sex"), Attribute) -> "female",
      (Path("family") / "brother", Value) -> "Bob",
      (Path("family") / "brother", Index) -> "0",
      (Path("family") / "sister", Value) -> "Alice",
      (Path("lastname"), Value) -> "Smith"
    ),
    Map(
      (Path("sex"), Attribute) -> "female",
      (Path("family") / "brother", Value) -> "Bill",
      (Path("family") / "brother", Index) -> "1",
      (Path("family") / "sister", Value) -> "Alice",
      (Path("lastname"), Value) -> "Smith"
    ),
    Map(
      (Path("sex"), Attribute) -> "male",
      (Path("firstname"), Value) -> "Fred",
      (Path("sex"), Value) -> "M"
    )
  )

  val arraysInArrays =
    """
      |<numbers>
      |    <values>
      |      <number>one</number>
      |      <number>two</number>
      |    </values>
      |    <values>
      |      <number>three</number>
      |    </values>
      |
      |</numbers>
      |
      |<numbers>
      |    <values>
      |      <number>four</number>
      |    </values>
      |</numbers>
    """.stripMargin

  val flattenedArraysInArrays: List[Record] = List(
    Map(
      "values.number" -> "one",
      "values.number:index" -> 0,
      "values:index" -> 0
    ),
    Map(
      "values.number" -> "two",
      "values.number:index" -> 1,
      "values:index" -> 0
    ),
    Map(
      "values.number" -> "three",
      "values.number:index" -> 0,
      "values:index" -> 1
    ),

    Map(
      "values.number" -> "four",
      "values.number:index" -> 0,
      "values:index" -> 0
    )
  )

  // This should automatically cause a table split (to avoid a big explode)
  val multipleArraysInArrays =
    """
      |<numbers>
      |    <values>
      |      <number>one</number>
      |      <number>two</number>
      |    </values>
      |    <values>
      |      <number>three</number>
      |    </values>
      |
      |    <doubles>
      |      <double>1.0</double>
      |      <double>2.0</double>
      |    </doubles>
      |
      |</numbers>
      |
      |<numbers>
      |    <values>
      |      <number>four</number>
      |    </values>
      |</numbers>
    """.stripMargin

  // Num output tables = O(num arrays at the same level)

  val inconsistentSchemaExample = ???

}
