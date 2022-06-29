package org.jetbrains.ztools.scala.handlers.impls

import org.jetbrains.ztools.scala.core.{Loopback, ResNames}
import org.jetbrains.ztools.scala.interpreter.ScalaVariableInfo

import scala.collection.mutable

class SpecialsHandler(limit: Int) extends AbstractTypeHandler {
  override def accept(obj: Any): Boolean = obj.getClass.getCanonicalName != null && obj.getClass.getCanonicalName.startsWith("scala.")

  override def handle(scalaInfo: ScalaVariableInfo, loopback: Loopback, depth: Int): mutable.Map[String, Any] = withJsonObject {
    json =>
      json.put(ResNames.VALUE, scalaInfo.value.toString.take(limit))
  }
}
