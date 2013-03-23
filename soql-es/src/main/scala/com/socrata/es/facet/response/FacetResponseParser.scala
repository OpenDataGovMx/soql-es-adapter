package com.socrata.es.facet.response

import annotation.tailrec
import com.socrata.es.facet._
import com.rojoma.json.io._
import com.socrata.soql.adapter.elasticsearch.{ESResultSet, ESJsonBadParse, FacetName}
import com.socrata.soql.collection.OrderedMap
import com.rojoma.json.ast.{JObject, JValue}
import com.socrata.soql.adapter.elasticsearch.ESGroupingResultSet._
import com.rojoma.json.codec.JsonCodec
import com.rojoma.json.io.StartOfObjectEvent
import com.rojoma.json.io.EndOfArrayEvent
import com.rojoma.json.io.StartOfArrayEvent


/**
 * This parser handle both TermsStats and Columns facets.
 */
trait FacetResponseParser {

  def parseFacet(reader: JsonReader, facet: FacetName, facetType: String, total: Option[Long])
    : Tuple2[Option[Long], OrderedMap[JValue, JObject]] = {
    parseTotalOptionFacetEntries(reader, facet, facetType, total)
  }

  def parseEntries(
    reader: JsonReader,
    facetName: FacetName,
    groupCol: String,
    aggregateCol: String,
    acc: OrderedMap[JValue, JObject] = OrderedMap.empty,
    total: Option[Long]): OrderedMap[JValue, JObject] = {

    facetToGroupRow(reader, facetName, groupCol, aggregateCol, acc)
  }

  @tailrec
  private def facetToGroupRow(reader: JsonReader, facetName: FacetName, groupCol: String, aggregateCol: String, acc: OrderedMap[JValue, JObject] = OrderedMap.empty): OrderedMap[JValue, JObject] = {

    reader.lexer.head match {
      case _: EndOfArrayEvent =>
        reader.lexer.next() // consume ]
        acc
      case _ =>
        reader.read() match {
          case facet@JObject(fields) =>
            val groupKey = facet(facetGroupKey(facetName)) // group key is
            // term property is removed and re-added using group col.
            // for the rest of the properties, we append aggregate col to the key.
            val groupRow = (fields -- unwantedFields).map {
                case (k, v) => (aggregateFnMap(k) + "_" + aggregateCol, v)
              } + (groupCol -> groupKey)
            val acc2 = acc + (groupKey -> JObject(groupRow))
            facetToGroupRow(reader, facetName, groupCol, aggregateCol, acc2)
          case _ =>
            throw new ESJsonBadParse(reader.lexer.head, StartOfObjectEvent())
        }
    }
  }

  /**
   * @param facetType can be columns for multi-columns and terms-stats for single column
   * @return true if we found facet, false means we find facet content
   */
  @tailrec
  private def parseTotalOptionFacetEntries(reader: JsonReader, facet: FacetName, facetType: String, total: Option[Long])
    : Tuple2[Option[Long], OrderedMap[JValue, JObject]] = {


    val lexer: JsonEventIterator = reader.lexer.asInstanceOf[JsonEventIterator]

    // total is only available in columns facet, non-exist in terms-stats facet.
    ESResultSet.skipToField(lexer, Set("total", facetType)) match {
      case Some("total") =>
        val ttl = JsonCodec.fromJValue[Long](reader.read())
        parseTotalOptionFacetEntries(reader, facet, facetType, ttl) // keep going
      case Some(field) if field == facetType =>
        lexer.next() match {
          case sa: StartOfArrayEvent =>
            val groupRow = this.parseEntries(reader, facet, facet.groupKey, facet.groupValue, total = total)
            lexer.next() // consume }
            (total, groupRow)
          case unexpected =>
            throw new ESJsonBadParse(lexer.head, StartOfArrayEvent())
        }
      case Some(_) => // ignore any fields other than "total" and facetType
        lexer.dropNextDatum()
        parseTotalOptionFacetEntries(reader, facet, facetType, total)
      case None =>
        throw new ESJsonBadParse(lexer.head, StartOfArrayEvent())
    }
  }
}

object FacetResponseParser {

  def apply(facetName: FacetName): FacetResponseParser = {
    facetName.facetType match {
      case ColumnsFacetType =>
        ColumnsFacetResponseParser
      case StatisticalFacetType =>
        StatisticalFacetResponseParser
      case TermsStatsFacetType =>
        TermsStatsFacetResponseParser
      case TermsFacetType =>
        TermsFacetResponseParser
      case unk =>
        throw new Exception("unknown facet")
    }
  }
}