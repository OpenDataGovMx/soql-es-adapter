package com.socrata.json.codec.elasticsearch

import com.rojoma.json.ast._
import com.rojoma.json.codec.JsonCodec
import com.rojoma.json.matcher._
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment._
import com.socrata.soql.types.{SoQLArray, SoQLText, SoQLType}

class DatasetContextCodec(resource: Option[String] = None) extends JsonCodec[DatasetContext[SoQLType]] {
  import DatasetContextCodec._

  implicit val schemalessDatasetContext = new SchemalessDatasetContext {
    val locale = com.ibm.icu.util.ULocale.ENGLISH
  }

  private val typeVar = Variable[String]()
  private val formatVar = Variable[String]()
  private val indexVar = Variable[String]()
  private val fieldPat = PObject(
    "type" -> typeVar,
    "format" -> POption(formatVar),
    "index" -> POption(indexVar)
  )

  private def decodeType(v: JValue) = {
    v match {
      case fieldPat(result) =>
        val soqlTypeName = es2SoqlType(typeVar(result))
        val soqlType = SoQLType.typesByName(TypeName(soqlTypeName))
        soqlType match {
          case SoQLText =>
            // Elastic search array is quite simple, they claim.  But it is quite strange.
            // Array is not a distinct type.  All datatypes have built in support of array.
            // SoQL array is mapped to "not_analyzed" string.  This is the only case
            // where elastic search type alone cannot resolve soda2.
            // mapping index field can be used to tell apart array and string types.
            indexVar.get(result) match {
              case Some("not_analyzed") => SoQLArray
              case _ => soqlType
            }
          case _ => soqlType
        }
    }
  }

  private def skipToValue(jvalue: Option[JValue], path: Seq[String]): Option[JValue] = {
    path match {
      case Nil => jvalue
      case h :: t =>
        val jobject = jvalue match {
          case Some(jo: JObject) => jo.get(h)
          case _ => None
        }
        skipToValue(jobject, t)
    }
  }

  def encode(myObj: DatasetContext[SoQLType]): JValue = {
    throw new UnsupportedOperationException
  }

  def decode(jValue: JValue): Option[DatasetContext[SoQLType]] = {

    import scala.language.existentials

    val optPath = if (resource.isDefined) Seq(resource.get) else Nil

    skipToValue(Some(jValue), optPath ++ Seq("data", "properties")) match {
      case Some(jo : JObject) =>
        val datasetContext = new DatasetContext[SoQLType] {
          implicit val ctx = this
          val locale = com.ibm.icu.util.ULocale.ENGLISH
          val colMap = jo.foldLeft(OrderedMap.empty[ColumnName, SoQLType]){(ordMap, kv) =>
            ordMap + (ColumnName(kv._1) -> decodeType(kv._2))
          }
          val schema = colMap
        }

        Some(datasetContext)
      case _ => None
    }
  }
}

object DatasetContextCodec {
  val es2SoqlType = Map(
    "string" -> "text",
    "boolean" -> "boolean",
    "geo_point" -> "location",
    "date" -> "floating_timestamp",
    "double" -> "number",
    "object" -> "object"
  )
}