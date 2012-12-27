package com.socrata.rows

import java.{lang => jl}
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.DateTimeZone
import com.rojoma.json.ast._
import com.socrata.util.deepcast.DeepCast
import com.blist.models.views.DataType
import com.blist.util.values.LocationValue
import com.blist.rows.format.DataTypeConverter

trait ESColumnMap {
  def toES(data: AnyRef): AnyRef
  def fromES(data: AnyRef): AnyRef = data
  def propMap: JValue = JObject(Map("type" -> JString("string")))
}

object ESColumnMap {

  implicit def StringToDataType(dataTypeDbName: String) = DataType.Type.fromDbName(dataTypeDbName)

  def apply(dt: DataType.Type): ESColumnMap = {
    dt match {
      case DataType.Type.CALENDAR_DATE => esCalendarDateColumnMap
      case DataType.Type.DATE => esDateColumnMap
      case DataType.Type.LOCATION => esLocationColumnMap
      case DataType.Type.NUMBER => esNumberLikeColumnMap
      case DataType.Type.CHECKBOX => esBooleanColumnMap
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

    def toES(data: AnyRef) = JBoolean(DataTypeConverter.cast(data, DataType.Type.CHECKBOX).asInstanceOf[jl.Boolean].booleanValue())
  }

  class ESCalendarDateColumnMap extends ESColumnMap {
    override def propMap: JValue = JObject(Map(
      "type" -> JString("date"),
      "format" -> JString("dateOptionalTime"),
      "store" -> JString("no"),
      "omit_norms" -> JBoolean(true)
    ))

    def toES(data: AnyRef) = JString(DataTypeConverter.cast(data, DataType.Type.CALENDAR_DATE).asInstanceOf[String])
  }

  class ESDateColumnMap extends ESColumnMap {
    override def propMap: JValue = JObject(Map(
      "type" -> JString("date"), "format" -> JString("dateOptionalTime"),
      "store" -> JString("no"),
      "omit_norms" -> JBoolean(true)
    ))

    def toES(data: AnyRef): AnyRef = {
      val ts = (DataTypeConverter.cast(data, DataType.Type.DATE).asInstanceOf[java.sql.Timestamp]).getTime
      JString(ISODateTimeFormat.dateTime().withZone(DateTimeZone.UTC).print(ts))
    }
  }

  class ESLocationColumnMap extends ESColumnMap {
    override def propMap: JValue = JObject(Map(
      "type" -> JString("geo_point"),
      "store" -> JString("yes"),
      "omit_norms" -> JBoolean(true)
    ))

    def toES(data: AnyRef): AnyRef = {
      val loc = data match {
        case x: LocationValue => x
        case x: String =>
          val locMap = DeepCast.mapOfObject.cast(DataTypeConverter.cast(x, DataType.Type.LOCATION))
          new LocationValue(locMap)
      }
      JString("%s,%s".format(loc.getLatitude.toString, loc.getLongitude.toString))
    }

    override def fromES(data: AnyRef): AnyRef = {
      val latLon = data.toString.split(",")
      val loc = new LocationValue()
      loc.setLatitude(new java.math.BigDecimal(latLon(0)))
      loc.setLongitude(new java.math.BigDecimal(latLon(1)))
      loc
    }
  }
}
