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


class ESResultSet(inputStream: InputStream, charset: Charset = Charset.forName("UTF-8")) {

  import ESResultSet._

  private val reader = new InputStreamReader(inputStream, charset)

  private val tokenIter = new JsonTokenIterator(reader)

  private val lexer = new JsonEventIterator(tokenIter)

  private val jsonReader = new JsonReader(lexer)

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

  private def resultStream(reader: JsonReader): Stream[JObject] = {
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

  @tailrec
  private def skipToField(lexer: JsonEventIterator, field: String): Boolean = {
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
  private def skipToField(lexer: JsonEventIterator, fields: Seq[String]): Boolean = {
    fields match {
      case h :: t =>
        skipToField(lexer, h)
        skipToField(lexer, t)
      case Nil =>
        true
    }
  }
}