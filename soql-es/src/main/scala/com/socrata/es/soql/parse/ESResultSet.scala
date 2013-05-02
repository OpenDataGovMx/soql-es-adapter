package com.socrata.es.soql.parse

import com.rojoma.json.io._
import java.io.{InputStreamReader, InputStream}
import java.nio.charset.Charset
import annotation.tailrec
import com.rojoma.json.ast.{JValue, JObject}
import com.rojoma.json.codec.JsonCodec
import com.rojoma.json.codec.JsonCodec._
import com.socrata.soql.{SoQLAnalysis, SoQLAnalyzer}
import com.socrata.soql.types.SoQLType


class ESJsonBadParse(event: JsonEvent, expect: JsonEvent) extends
JsonReaderException("Received unexpected event %s at %d %d, expecting %s".format(event.toString, event.position.column, event.position.row, expect.toString))
with JsonReadException {
  def position = event.position
}

trait ESResultSet {

  protected val lexer: JsonEventIterator

  protected val jsonReader: JsonReader

  def rowStream(): Tuple2[Option[Long], Stream[JObject]]
}

class ESPlainResultSet(analysis: SoQLAnalysis[SoQLType], inputStream: InputStream, charset: Charset) extends ESResultSet {

  import ESResultSet._

  protected val reader = new InputStreamReader(inputStream, charset)

  protected val tokenIter = new JsonTokenIterator(reader)

  protected val lexer = new JsonEventIterator(tokenIter)

  protected val jsonReader = new JsonReader(lexer)

  protected val honorSelect = true

  private val selection = analysis.selection.keySet.map(_.name)

  /**
   * Restrict ES _source output to selected columns
   * TODO: scripts/expressions are not handled yet because it is likely to require a totally different processor.
   * @param map
   * @return
   */
  def select(map: Map[String, JValue]): Map[String, JValue] = map.filterKeys(selection.contains(_))

  def rowStream(): Tuple2[Option[Long], Stream[JObject]] = {
    skipToField(lexer, Seq("hits", "total"))
    val total = JsonCodec.fromJValue[Long](jsonReader.read())
    skipToField(lexer, "hits")
    if (!lexer.hasNext) throw new JsonParserEOF(lexer.head.position)
    lexer.next() match {
      case sa: StartOfArrayEvent =>
        (total, resultStream(jsonReader))
      case _ =>
        throw new ESJsonBadParse(lexer.head, StartOfArrayEvent())
    }
  }

  protected def resultStream(reader: JsonReader): Stream[JObject] = {

    if (reader.lexer.head.isInstanceOf[EndOfArrayEvent]) Stream.empty
    else
      reader.read() match {
        case JObject(outerRow) =>
          outerRow("_source") match {
            case jo@JObject(_) =>
              if (honorSelect) JObject(select(jo.toMap)) #:: resultStream(reader)
              else jo #:: resultStream(reader)

            case _ => throw new ESJsonBadParse(reader.lexer.head, StartOfObjectEvent())
          }
        case _ => throw new ESJsonBadParse(reader.lexer.head, StartOfObjectEvent())
      }
  }
}

object ESResultSet {

  def parser(analysis: SoQLAnalysis[SoQLType], inputStream: InputStream, charset: Charset = scala.io.Codec.UTF8.charSet): ESResultSet =
    if (analysis.isGrouped) new ESGroupingResultSet(analysis, inputStream, charset)
    else new ESPlainResultSet(analysis, inputStream, charset)

  @tailrec
  def skipToField(lexer: JsonEventIterator, field: String): Boolean = {
    if (!lexer.hasNext) false
    else lexer.next() match {
      case FieldEvent(s) if (s == field) =>
        true
      case o: StartOfObjectEvent =>
        skipToField(lexer, field)
      case _ =>
        lexer.dropNextDatum()
        skipToField(lexer, field)
    }
  }

  @tailrec
  def skipToField(lexer: JsonEventIterator, fields: Seq[String]): Boolean = {
    fields match {
      case h :: t =>
        skipToField(lexer, h)
        skipToField(lexer, t)
      case Nil =>
        true
    }
  }

  /**
   * Skip to any fields given in fields set
   * @param fields fields we are looking for
   * @return first field that match any one in fields
   */
  @tailrec
  def skipToField(lexer: JsonEventIterator, fields: Set[String]): Option[String] = {
    if (!lexer.hasNext) None
    else lexer.next() match {
      case FieldEvent(s) if (fields.contains(s)) =>
        Some(s)
      case o: StartOfObjectEvent =>
        skipToField(lexer, fields)
      case _ =>
        lexer.dropNextDatum()
        skipToField(lexer, fields)
    }
  }

}