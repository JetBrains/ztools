package org.jetbrains.ztools.scala.interpreter

import scala.collection.immutable
import scala.tools.nsc.interpreter.IMain

class InterpreterHandler(val interpreter: IMain) {
  val wrapper = new ZtoolsInterpreterWrapper(interpreter)

  def getVariableNames: immutable.Seq[String] =
    interpreter.definedSymbolList.filter { x => x.isGetter }.map(_.name.toString).distinct

  def getInfo(name: String, tpe: String): ScalaVariableInfo = {
    val obj = valueOfTerm(name).orNull
    ScalaVariableInfo(isAccessible = true, isLazy = false, obj, tpe, name, null)
  }

  def valueOfTerm(id: String): Option[Any] = wrapper.valueOfTerm(id)
}
