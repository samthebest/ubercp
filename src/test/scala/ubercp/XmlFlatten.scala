package ubercp

import org.specs2.mutable.Specification

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

  val textOnlySchema = "note" -> Map(
    "to" -> "String",
    "from" -> "String",
    "cc" -> "String",
    "bcc" -> "String",
    "heading" -> "String",
    "subject" -> "String",
    "body" -> "String"
  )

  // Should write this first, and only implement schema discovery that is necessary
  val flattenedTextOnly = List(
    Map(
      "to" -> "Tove",
      "from" -> "Jani",
      "cc" -> "Bob",
      "heading" -> "Reminder",
      "body" -> "Don't forget me this weekend!"
    ),
    Map(
      "to" -> "Alice",
      "from" -> "Bob",
      "bcc" -> "Fred",
      "heading" -> "JFDI",
      "subject" -> "JFDI!",
      "body" -> "Buy socks"
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

  val withAttributesSchema = "person" -> Map(
    "sex:attribute" -> "StringAttribute",
    "firstname" -> "String",
    "lastname" -> "String",
    "sex" -> "String"
  )

  val flattenedWithAttributes = List(
    Map(
      "sex:attribute" -> "female",
      "firstname" -> "Anna",
      "lastname" -> "Smith"
    ),
    Map(
      "sex:attribute" -> "male",
      "firstname" -> "Fred",
      "sex" -> "M"
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

  val withArraySchema = "numbers" -> Map(
    "value" -> "StringArray"
  )

  // May want optional record GUID generation if it's assumed the record doesn't already have an ID
  val flattenedWithArray = List(
    Map(
      "value" -> "three",
      "value:index" -> "0"
    ),
    Map(
      "value" -> "two",
      "value:index" -> "1"
    ),
    Map(
      "value" -> "one",
      "value:index" -> "2"
    ),
    Map(
      "value" -> "one",
      "value:index" -> "0"
    ),
    Map(
      "value" -> "two",
      "value:index" -> "1"
    ),
    Map(
      "value" -> "three",
      "value:index" -> "2"
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

  val flattenedNestedXml = List(
    Map(
      "sex:attribute" -> "female",
      "family.brother" -> "Bob",
      "family.sister" -> "Alice",
      "lastname" -> "Smith"
    ),
    Map(
      "sex:attribute" -> "male",
      "firstname" -> "Fred",
      "sex" -> "M"
    )
  )

  val nestedArraysInArrays = ???

  val nestedArraysInArraysAndStructs = ???

  val inconsistentSchemaExample = ???

  "XmlFlatten.discoverSchema" should {

  }
}
