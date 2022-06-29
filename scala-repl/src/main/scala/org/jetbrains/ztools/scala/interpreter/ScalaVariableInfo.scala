package org.jetbrains.ztools.scala.interpreter

case class ScalaVariableInfo(isAccessible: Boolean,
                             isLazy: Boolean,
                             value: Any,
                             tpe: String,
                             path: String,
                             ref: String) {
  val name: String = if (path != null)
    path.substring(path.lastIndexOf('.') + 1)
  else
    null
}
