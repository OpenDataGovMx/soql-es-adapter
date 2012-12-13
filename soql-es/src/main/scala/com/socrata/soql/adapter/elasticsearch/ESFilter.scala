package com.socrata.soql.adapter.elasticsearch

import com.rojoma.json.ast._
import com.rojoma.json.ast.JObject
import com.rojoma.json.ast.JString
import com.socrata.soql.adapter.{SoQLAdapterException, NotImplementedException, XlateCtx}
import com.socrata.soql.typed._
import com.socrata.soql.types._
import com.socrata.soql.typed.StringLiteral
import com.socrata.soql.typed.BooleanLiteral
import com.socrata.soql.typed.NullLiteral
import com.socrata.soql.typed.NumberLiteral
import com.socrata.soql.typed.ColumnRef
import com.socrata.soql.typed.FunctionCall
import com.socrata.soql.functions.MonomorphicFunction
import scala.Tuple2
import util.parsing.input.Position

trait ESFilter {

  def toFilter(xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0, canScript: Boolean = false): JValue

  def toScript(xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0): Tuple2[String, Map[XlateCtx.Value, AnyRef]]
}

class RequireScriptFilter(message: String, position: Position) extends SoQLAdapterException(message, position)

case class ESTypedFF[+Type](typedFF: TypedFF[Type]) extends ESFilter {

  def toFilter(xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0, canScript: Boolean = false): JValue = {
    try {
      typedFF match {
        case col: ColumnRef[_] => ESColumnRef(col).toFilter(xlateCtx, level, canScript)
        case lit: StringLiteral[_] => ESStringLiteral(lit).toFilter(xlateCtx, level, canScript)
        case lit: NumberLiteral[_] => ESNumberLiteral(lit).toFilter(xlateCtx, level, canScript)
        case fn: FunctionCall[_] => ESFunctionCall(fn).toFilter(xlateCtx, level, canScript)
        case lit: BooleanLiteral[_] => throw new NotImplementedException("Bool literal not supported yet.", lit.position)
        case lit: NullLiteral[_] => throw new NotImplementedException("Null literal not supported yet.", lit.position)
      }
    } catch {
      case ex: RequireScriptFilter =>
        if (canScript) {
          val (js, ctx) = toScript(xlateCtx, level)
          JObject(Map("script" -> JObject(Map(
            // if ESLang is not set, we use default mvel.  Some expressions like casting require js
            "lang" -> JString(ctx.getOrElse(XlateCtx.ESLang, "mvel").asInstanceOf[String]),
            "script" -> JString(js)
          ))))
        }
        else if (level > 0) throw ex
        else throw new NotImplementedException("Expression not implemented", typedFF.position)
    }
  }

  def toScript(xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0): Tuple2[String, Map[XlateCtx.Value, AnyRef]] = {
    typedFF match {
      case col: ColumnRef[_] => ESColumnRef(col).toScript(xlateCtx, level)
      case lit: StringLiteral[_] => ESStringLiteral(lit).toScript(xlateCtx, level)
      case lit: NumberLiteral[_] => ESNumberLiteral(lit).toScript(xlateCtx, level)
      case fn: FunctionCall[_] => ESFunctionCall(fn).toScript(xlateCtx, level)
      case lit: BooleanLiteral[_] => throw new NotImplementedException("", lit.position)
      case lit: NullLiteral[_] => throw new NotImplementedException("", lit.position)
    }
  }
}

case class ESFunctionCall[T](fn: FunctionCall[T]) extends ESFilter {

  def toFilter(xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0, canScript: Boolean = false): JValue = {
    import ESFunction._
    fn.function match {
      case MonomorphicFunction(SoQLFunctions.IsNull, _) => isNull(fn, xlateCtx, level, canScript)
      case MonomorphicFunction(SoQLFunctions.IsNotNull, _) => isNotNull(fn, xlateCtx, level, canScript)
      case MonomorphicFunction(SoQLFunctions.Not, _) => not(fn, xlateCtx, level, canScript)
      case MonomorphicFunction(SoQLFunctions.In, _) => in(fn, xlateCtx, level, canScript)
      case MonomorphicFunction(SoQLFunctions.NotIn, _) => notIn(fn, xlateCtx, level, canScript)
      case MonomorphicFunction(SoQLFunctions.Eq, _) => equ(fn, xlateCtx, level, canScript)
      case MonomorphicFunction(SoQLFunctions.EqEq, _) => equ(fn, xlateCtx, level, canScript)
      case MonomorphicFunction(SoQLFunctions.TextToFixedTimestamp, _) => textToFloatingTimestamp(fn, xlateCtx, level, canScript)
      case MonomorphicFunction(SoQLFunctions.Neq, _) => neq(fn, xlateCtx, level, canScript)
      case MonomorphicFunction(SoQLFunctions.BangEq, _) => neq(fn, xlateCtx, level, canScript)
      case MonomorphicFunction(SoQLFunctions.And, _) => andOr(fn, xlateCtx, level, canScript)
      case MonomorphicFunction(SoQLFunctions.Or, _) => andOr(fn, xlateCtx, level, canScript)
      case MonomorphicFunction(SoQLFunctions.UnaryMinus, _) => unaryMinus(fn, xlateCtx, level, canScript)
      case MonomorphicFunction(SoQLFunctions.NotBetween, _) => notBetween(fn, xlateCtx, level, canScript)
      case MonomorphicFunction(SoQLFunctions.WithinCircle, _) => withinCircle(fn, xlateCtx, level, canScript)
      case MonomorphicFunction(SoQLFunctions.WithinBox, _) => withinBox(fn, xlateCtx, level, canScript)
      case MonomorphicFunction(SoQLFunctions.Between, _) => between(fn, xlateCtx, level, canScript)
      case MonomorphicFunction(SoQLFunctions.Lt, _) => lgte(fn, xlateCtx, level, canScript)
      case MonomorphicFunction(SoQLFunctions.Lte, _) => lgte(fn, xlateCtx, level, canScript)
      case MonomorphicFunction(SoQLFunctions.Gt, _) => lgte(fn, xlateCtx, level, canScript)
      case MonomorphicFunction(SoQLFunctions.Gte, _) => lgte(fn, xlateCtx, level, canScript)
      case MonomorphicFunction(SoQLFunctions.Like, _) => like(fn, xlateCtx, level, canScript)
      case MonomorphicFunction(SoQLFunctions.NotLike, _) => notLike(fn, xlateCtx, level, canScript)
      case _ =>
        throw new RequireScriptFilter("Require script filter", fn.position)
    }
  }

  def toScript(xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0): Tuple2[String, Map[XlateCtx.Value, AnyRef]] = {
    import ESScriptFunction._
    fn.function match {
      case MonomorphicFunction(SoQLFunctions.IsNull, _) => isNull(fn, xlateCtx, level)
      case MonomorphicFunction(SoQLFunctions.IsNotNull, _) => isNotNull(fn, xlateCtx, level)
      case MonomorphicFunction(SoQLFunctions.Not, _) => not(fn, xlateCtx, level)
      case MonomorphicFunction(SoQLFunctions.UnaryMinus, _) => unaryMinus(fn, xlateCtx, level)
      case MonomorphicFunction(SoQLFunctions.TextToNumber, _) => textToNumber(fn, xlateCtx, level)
      case MonomorphicFunction(SoQLFunctions.NumberToText, _) => numberToText(fn, xlateCtx, level)
      case MonomorphicFunction(SoQLFunctions.Between, _) => between(fn, xlateCtx, level)
      case MonomorphicFunction(SoQLFunctions.NotBetween, _) => notBetween(fn, xlateCtx, level)
      case MonomorphicFunction(SoQLFunctions.In, _) => in(fn, xlateCtx, level)
      case MonomorphicFunction(SoQLFunctions.NotIn, _) => notIn(fn, xlateCtx, level)

      case MonomorphicFunction(SoQLFunctions.Eq, _) => infix(fn, xlateCtx, level)
      case MonomorphicFunction(SoQLFunctions.EqEq, _) => infix(fn, xlateCtx, level)
      case MonomorphicFunction(SoQLFunctions.And, _) => infix(fn, xlateCtx, level)
      case MonomorphicFunction(SoQLFunctions.Or, _) => infix(fn, xlateCtx, level)
      case MonomorphicFunction(SoQLFunctions.BinaryMinus, _) => infix(fn, xlateCtx, level)
      case MonomorphicFunction(SoQLFunctions.BinaryPlus, _) => infix(fn, xlateCtx, level)
      case MonomorphicFunction(SoQLFunctions.Gt, _) => infix(fn, xlateCtx, level)
      case MonomorphicFunction(SoQLFunctions.Gte, _) => infix(fn, xlateCtx, level)

      case MonomorphicFunction(SoQLFunctions.Lt, _) => infix(fn, xlateCtx, level)
      case MonomorphicFunction(SoQLFunctions.Lte, _) => infix(fn, xlateCtx, level)
      case MonomorphicFunction(SoQLFunctions.Neq, _) => infix(fn, xlateCtx, level)
      case MonomorphicFunction(SoQLFunctions.BangEq, _) => infix(fn, xlateCtx, level)
      case MonomorphicFunction(SoQLFunctions.Concat, _) => infix(fn, xlateCtx, level)
      case _ =>
        throw new NotImplementedException("Expression not implemented " + fn.toString, fn.position)

    }
  }
}

case class ESColumnRef[T](col: ColumnRef[T]) extends ESFilter {
  def toFilter(xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0, canScript: Boolean = false): JValue = {
    col.typ match {
      case x: SoQLNumber.type => JString(col.column.name)
      case x: SoQLText.type => JString(col.column.name)
      case x => JString(col.column.name)
    }
  }

  def toScript(xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0): Tuple2[String, Map[XlateCtx.Value, AnyRef]] = {
    ("doc['%s'].value".format(col.column.name), xlateCtx)
  }
}

case class ESStringLiteral[T](lit: StringLiteral[T]) extends ESFilter {
  def toFilter(xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0, canScript: Boolean = false): JValue = {
    JString(lit.value)
  }

  def toScript(xlateCtx: Map[XlateCtx.Value, AnyRef], level: Int = 0): Tuple2[String, Map[XlateCtx.Value, AnyRef]] = {
    (JString(lit.value).toString, xlateCtx)
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
