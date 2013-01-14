package com.socrata.rows

import java.{lang => jl}
import org.joda.time.format.{DateTimeFormat, ISODateTimeFormat}
import org.joda.time.DateTimeZone
import com.rojoma.json.ast._
import com.socrata.soql.types._
import com.socrata.soql.environment.TypeName

trait ESColumnMap {
  def toES(data: AnyRef): AnyRef
  def fromES(data: AnyRef): AnyRef = data
  def propMap: JValue = JObject(Map("type" -> JString("string")))
}

object ESColumnMap {

  implicit def StringToDataType(dataTypeDbName: String): SoQLType = {
    val dtName = dataTypeDbName.toLowerCase match {
      case "calendar_date" => "floating_timestamp" // convert legacy name
      case "checkbox" => "boolean"  // convert legacy name
      case x => x
    }
    SoQLType.typesByName(TypeName(dtName))
  }

  def apply(dt: SoQLType): ESColumnMap = {

    dt match {
      case SoQLFixedTimestamp => esDateColumnMap
      case SoQLFloatingTimestamp => esCalendarDateColumnMap
      case SoQLLocation => esLocationColumnMap
      case SoQLNumber => esNumberLikeColumnMap
      case SoQLBoolean => esBooleanColumnMap
      case SoQLText => esTextLikeColumnMap
      case _ => esTextLikeColumnMap
    }
  }

  private val esTextLikeColumnMap = new ESTextLikeColumnMap()
  private val esNumberLikeColumnMap = new ESNumberLikeColumnMap()
  private val esCalendarDateColumnMap = new ESCalendarDateColumnMap()
  private val esDateColumnMap = new ESDateColumnMap()
  private val esLocationColumnMap = new ESLocationColumnMap()
  private val esBooleanColumnMap = new ESBooleanColumnMap()

  class ESTextLikeColumnMap extends ESColumnMap {
    def toES(data: AnyRef): AnyRef = JString(data.toString)

    override def propMap: JValue = JObject(Map(
      "type" -> JString("string"),
      "index" -> JString("analyzed"),
      "store" -> JString("yes"),
      // in ELASTICSEARCH.YML.  There should be a custom analyzer defined:
      // index:
      //   analysis:
      //     analyzer:
      //       one_lower_token:
      //         type: custom
      //         tokenizer: keyword
      //         filter: [lowercase]
      "analyzer" -> JString("one_lower_token"),
      "omit_norms" -> JBoolean(true)
    ))
  }

  class ESNumberLikeColumnMap extends ESTextLikeColumnMap {
    override def propMap: JValue = JObject(Map(
      "type" -> JString("double"),
      "store" -> JString("yes"),
      "omit_norms" -> JBoolean(true)
    ))

    override def toES(data: AnyRef): AnyRef =
      try {
        data match {
          case x: String =>
            java.lang.Double.parseDouble(x)
          case _ =>
        }
        super.toES(data)
      } catch {
        case e: NumberFormatException =>
          JNull
      }
  }

  class ESBooleanColumnMap extends ESColumnMap {
    override def propMap: JValue = JObject(Map(
      "type" -> JString("boolean"),
      "index" -> JString("not_analyzed"),
      "store" -> JString("no"),
      "omit_norms" -> JBoolean(true)
    ))

    def toES(data: AnyRef) = Option(data) match {
      case Some(str: String) => JBoolean(jl.Boolean.parseBoolean(str))
      case _ => JNull
    }
  }

  class ESCalendarDateColumnMap extends ESColumnMap {

    val tsParser = DateTimeFormat.forPattern("MM/dd/yyyy hh:mm aa").withZoneUTC
    val tsFormat = ISODateTimeFormat.dateTimeNoMillis
    val Iso8601 = """(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(:\d{2})?)""".r
    val Mmddyyyy = """(\d{2}/\d{2}/\d{4} \d{2}:\d{2} [A|P]M)""".r

    override def propMap: JValue = JObject(Map(
      "type" -> JString("date"),
      "format" -> JString("dateOptionalTime"),
      "store" -> JString("no"),
      "omit_norms" -> JBoolean(true)
    ))

    def toES(data: AnyRef) = {
      data match {
        case Iso8601(s,r) =>
          val localDateTime = ISODateTimeFormat.dateTimeParser.parseLocalDateTime(s)
          JString(tsFormat.print(localDateTime))
        case Mmddyyyy(s) =>
          val localDateTime = tsParser.parseLocalDateTime(s)
          JString(tsFormat.print(localDateTime))
        case _ => JNull

      }
    }
  }

  class ESDateColumnMap extends ESColumnMap {

    val tsParser = DateTimeFormat.forPattern("MM/dd/yyyy hh:mm aaZ")
    val tsFormat = ISODateTimeFormat.dateTimeNoMillis.withZone(DateTimeZone.UTC)

    override def propMap: JValue = JObject(Map(
      "type" -> JString("date"), "format" -> JString("dateOptionalTime"),
      "store" -> JString("no"),
      "omit_norms" -> JBoolean(true)
    ))

    def toES(data: AnyRef) = {
      Option(data) match {
        case Some(s: String) =>
          val dateTime = tsParser.parseDateTime(data.toString)
          JString(tsFormat.print(dateTime))
        case _ => JNull

      }
    }
  }

  class ESLocationColumnMap extends ESColumnMap {

    val fmt = """^\(([0-9.-]+), ([0-9.-]+)\)$""".r

    override def propMap: JValue = JObject(Map(
      "type" -> JString("geo_point"),
      "store" -> JString("yes"),
      "omit_norms" -> JBoolean(true)
    ))

    def toES(data: AnyRef) = {
      data match {
        case fmt(lat, lon) =>
          JString(s"$lat,$lon")
        case _ =>
          JNull
      }
    }

    override def fromES(data: AnyRef): AnyRef = {
      data
    }
  }
}
