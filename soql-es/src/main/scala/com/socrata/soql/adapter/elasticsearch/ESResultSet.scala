package com.socrata.soql.adapter.elasticsearch

import com.rojoma.json.io._
import java.io.{InputStreamReader, InputStream}
import java.nio.charset.Charset
import annotation.tailrec
import com.rojoma.json.ast.JObject


class ESJsonBadParse(event: JsonEvent, expect: JsonEvent) extends
  JsonReaderException("Received unexpected event %s at %d %d, expecting %s".format(event.toString, event.position.column, event.position.row, expect.toString))
  with JsonReadException {
  def position = event.position
}

trait ESResultSet {

  protected val lexer: JsonEventIterator

  protected val jsonReader: JsonReader

  def rowStream(): Stream[JObject]
}

class ESPlainResultSet(inputStream: InputStream, charset: Charset) extends ESResultSet {

  import ESResultSet._

  protected val reader = new InputStreamReader(inputStream, charset)

  protected val tokenIter = new JsonTokenIterator(reader)

  protected val lexer = new JsonEventIterator(tokenIter)

  protected val jsonReader = new JsonReader(lexer)

  def rowStream(): Stream[JObject] = {
    skipToField(lexer, Seq("hits", "hits"))
    if (!lexer.hasNext) throw new JsonParserEOF(lexer.head.position)
    lexer.next() match {
      case sa: StartOfArrayEvent =>
        resultStream(jsonReader)
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
              jo #:: resultStream(reader)
            case _ => throw new ESJsonBadParse(reader.lexer.head, StartOfObjectEvent())
          }
        case _ => throw new ESJsonBadParse(reader.lexer.head, StartOfObjectEvent())
      }
  }
}

object ESResultSet {

  def parser(isGrouped: Boolean, inputStream: InputStream, charset: Charset = scala.io.Codec.UTF8): ESResultSet =
    if (isGrouped) new ESGroupingResultSet(inputStream, charset)
    else new ESPlainResultSet(inputStream, charset)

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
}