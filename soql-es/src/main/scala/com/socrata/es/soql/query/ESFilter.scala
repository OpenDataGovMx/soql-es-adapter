package com.socrata.es.soql.query

import com.rojoma.json.ast._
import com.socrata.soql.typed._
import com.socrata.soql.types._
import util.parsing.input.Position
import com.socrata.es.soql.{SoQLAdapterException, NotImplementedException}

trait ESFilter {

  def toFilter(xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0, canScript: Boolean = false): JValue

  def toScript(xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0): Tuple2[String, Map[XlateCtx.Value, AnyRef]]
}

class RequireScriptFilter(message: String, position: Position) extends SoQLAdapterException(message, position)


case class ESCoreExpr[+Type](coreExpr: CoreExpr[Type]) extends ESFilter {

  def toFilter(xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0, canScript: Boolean = false): JValue = {
    try {
      coreExpr match {
        case col: ColumnRef[_] => ESColumnRef(col).toFilter(xlateCtx, level, canScript)
        case lit: StringLiteral[_] => ESStringLiteral(lit).toFilter(xlateCtx, level, canScript)
        case lit: NumberLiteral[_] => ESNumberLiteral(lit).toFilter(xlateCtx, level, canScript)
        case fn: FunctionCall[_] => ESFunctionCall(fn).toFilter(xlateCtx, level, canScript)
        case lit: BooleanLiteral[_] => ESBooleanLiteral(lit).toFilter(xlateCtx, level, canScript)
        case lit: NullLiteral[_] => throw new NotImplementedException("Null literal not supported yet.", lit.position)
      }
    } catch {
      case ex: RequireScriptFilter =>
        if (canScript) {
          val (js, ctx) = toScript(xlateCtx, level)
          val jsTryCatch =
            if(ctx.get(XlateCtx.TryCatch).isDefined) "try { %s; } catch(e) { null; }".format(js)
            else js
          JObject(Map("script" -> JObject(Map(
            // if ESLang is not set, we use default mvel.  Some expressions like casting require js
            "lang" -> JString(ctx.getOrElse(XlateCtx.ESLang, "mvel").asInstanceOf[String]),
            "script" -> JString(jsTryCatch)
          ))))
        }
        else if (level > 0) throw ex
        else throw new NotImplementedException("Expression not implemented", coreExpr.position)
    }
  }

  def toScript(xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0): Tuple2[String, Map[XlateCtx.Value, AnyRef]] = {
    coreExpr match {
      case col: ColumnRef[_] => ESColumnRef(col).toScript(xlateCtx, level)
      case lit: StringLiteral[_] => ESStringLiteral(lit).toScript(xlateCtx, level)
      case lit: NumberLiteral[_] => ESNumberLiteral(lit).toScript(xlateCtx, level)
      case fn: FunctionCall[_] => ESFunctionCall(fn).toScript(xlateCtx, level)
      case lit: BooleanLiteral[_] => ESBooleanLiteral(lit).toScript(xlateCtx, level)
      case lit: NullLiteral[_] => throw new NotImplementedException("", lit.position)
    }
  }
}

case class ESFunctionCall[T](fn: FunctionCall[T]) extends ESFilter {

  def toFilter(xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0, canScript: Boolean = false): JValue = {

    ESFunction(fn.function.name) match {
      case Some(f) => f(fn, xlateCtx, level, canScript)
      case None => throw new RequireScriptFilter("Require script filter", fn.position)
    }
  }

  def toScript(xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0): Tuple2[String, Map[XlateCtx.Value, AnyRef]] = {

    ESScriptFunction(fn.function.name) match {
      case Some(f) => f(fn, xlateCtx, level)
      case None => throw new NotImplementedException("Expression not implemented " + fn.toString, fn.position)
    }
  }
}

case class ESColumnRef[T](col: ColumnRef[T]) extends ESFilter {
  def toFilter(xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0, canScript: Boolean = false): JValue = {
    col.typ match {
      case x: SoQLNumber.type => JString(col.column.name)
      case x: SoQLText.type => JString(col.column.name)
      case x: SoQLBoolean.type if (level == 0) =>
        // This trick makes standalone boolean column work - select * where boolColumn
        JObject(Map("term" -> JObject(Map(col.column.name -> JBoolean(true)))))
      case x => JString(col.column.name)
    }
  }

  def toScript(xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0): Tuple2[String, Map[XlateCtx.Value, AnyRef]] = {
    ("doc['%s'].value".format(col.column.name), xlateCtx)
  }
}

case class ESStringLiteral[T](lit: StringLiteral[T]) extends ESFilter {
  def toFilter(xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0, canScript: Boolean = false): JValue = {
    val v = lit.value
    JString(if (xlateCtx.contains(XlateCtx.LowercaseStringLiteral)) v.toLowerCase else v)
  }

  def toScript(xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0): Tuple2[String, Map[XlateCtx.Value, AnyRef]] = {
    (toFilter(xlateCtx, level).toString(), xlateCtx)
  }
}

case class ESNumberLiteral[T](lit: NumberLiteral[T]) extends ESFilter {
  def toFilter(xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0, canScript: Boolean = false): JValue = {
    JNumber(lit.value)
  }

  def toScript(xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0): Tuple2[String, Map[XlateCtx.Value, AnyRef]] = {
    (JNumber(lit.value).toString, xlateCtx)
  }
}

case class ESBooleanLiteral[T](lit: BooleanLiteral[T]) extends ESFilter {
  def toFilter(xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0, canScript: Boolean = false): JValue = {
    JBoolean(lit.value)
  }

  def toScript(xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0): Tuple2[String, Map[XlateCtx.Value, AnyRef]] = {
    (JBoolean(lit.value).toString, xlateCtx)
  }
}

