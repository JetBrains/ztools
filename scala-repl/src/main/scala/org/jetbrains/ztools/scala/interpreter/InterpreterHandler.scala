package org.jetbrains.ztools.scala.interpreter

import scala.collection.immutable
import scala.tools.nsc.interpreter.IMain

class InterpreterHandler(val interpreter: IMain) {
  val wrapper = new ZtoolsInterpreterWrapper(interpreter)

  def getVariableNames: immutable.Seq[String] =
    interpreter.definedSymbolList.filter { x => x.isGetter }.map(_.name.toString).distinct

  def getInfo(name: String): ScalaVariableInfo = {
    val obj = valueOfTerm(name).orNull
    ScalaVariableInfo(isAccessible = true, isLazy = false, obj, typeOfTerm(obj, name), name, null)
  }

  def valueOfTerm(id: String): Option[Any] = wrapper.valueOfTerm(id)

  def typeOfTerm(obj: Any, id: String): String = obj match {
    case _: Boolean => "Boolean"
    case _: Byte => "Byte"
    case _: Char => "Char"
    case _: Short => "Short"
    case _: Int => "Int"
    case _: Long => "Long"
    case _: Float => "Float"
    case _: Double => "Double"
    case _: String => "String"
    case _: Unit => "Unit"
    case _ => interpreter.typeOfExpression(id, silent = true).toString()
  }
}
