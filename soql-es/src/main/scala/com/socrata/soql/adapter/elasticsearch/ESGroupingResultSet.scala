package com.socrata.soql.adapter.elasticsearch

import com.rojoma.json.io._
import java.io.InputStream
import java.nio.charset.Charset
import annotation.tailrec
import com.rojoma.json.ast.{JValue, JObject}
import com.socrata.soql.collection.OrderedMap

class ESGroupingResultSet(inputStream: InputStream, charset: Charset)
  extends ESPlainResultSet(inputStream, charset) {

  import ESResultSet._
  import ESGroupingResultSet._

  /**
   * Unlike regular query, there is no free total row count for facet.
   */
  override def rowStream(): Tuple2[Option[Long], Stream[JObject]] = {
    skipToField(lexer, "facets")
    if (!lexer.hasNext) throw new JsonParserEOF(lexer.head.position)
    val facetsResult = parseFacets(lexer)
    // TODO: Facets result may not be sorted in the right order yet.
    (None, facetsResult.values.toStream)
  }

  @tailrec
  private def parseFacets(lexer: JsonEventIterator, acc: OrderedMap[JValue, JObject] = OrderedMap.empty): OrderedMap[JValue, JObject] = {

    lexer.head match {
      case o: EndOfObjectEvent =>
        lexer.next() // consume }
        acc
      case _ =>
        val groupRow = parseOneFacet(lexer)
        val combinedRow = groupRow.map {
          case (k, v) =>
            val combinedResult = JObject(v.toMap ++ acc.get(k).getOrElse(JObject(Map.empty)).toMap)
            (k -> combinedResult)
        }
        parseFacets(lexer, combinedRow)
    }
  }

  private def parseOneFacet(lexer: JsonEventIterator): OrderedMap[JValue, JObject] = {

    lexer.next() match {
      case o: StartOfObjectEvent =>
        parseOneFacet(lexer)
      case FieldEvent(FacetName(facet)) =>
        skipToField(lexer, facetDataKey(facet)) // dataKey is different depending on the type of facet which can be inferred by facet name
        lexer.next() match {
          case sa: StartOfArrayEvent =>
            val groupRow = facetToGroupRow(jsonReader, facet, facet.groupKey, facet.groupValue)
            lexer.next() // consume }
            groupRow
          case unexpected =>
            throw new ESJsonBadParse(lexer.head, StartOfArrayEvent())
        }
      case unexpected =>
        throw new ESJsonBadParse(lexer.head, FieldEvent("some_facet_name"))
    }
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
}

object ESGroupingResultSet {

  // Field either we do not want or field is not a constant translation that require remove and re-add
  val unwantedFields = Seq("term", "total_count", "keys")

  val aggregateFnMap = Map(
    "total" -> "sum",
    "mean" -> "avg",
    "min" -> "min",
    "max" -> "max",
    "count" -> "count"
  )

  private def facetDataKey(facetName: FacetName) =
    if (facetName.isMultiColumn) "entries" else "terms"

  private def facetGroupKey(facetName: FacetName) =
    if (facetName.isMultiColumn) "keys" else "term"
}