/**
 * Copyright 2020 Jetbrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jetbrains.ztools.scala

import org.codehaus.jettison.json.JSONObject
import org.jetbrains.ztools.core.{Loopback, Names, TrieMap, TypeHandler}
import org.jetbrains.ztools.scala.handlers._

import java.util.function.{Function => JFunction}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.language.implicitConversions

abstract class VariablesViewImpl(val collectionSizeLimit: Int,
                                 val stringSizeLimit: Int,
                                 val blackList: List[String],
                                 val filterUnitResults: Boolean,
                                 val enableProfiling: Boolean,
                                 val depth: Int) extends VariablesView {
  private val ru = scala.reflect.runtime.universe
  private val mirror = ru.runtimeMirror(getClass.getClassLoader)
  private val touched = mutable.Map[String, ScalaVariableInfo]()

  class HandlerWrapper(val handler: TypeHandler) {
    def accept(info: ScalaVariableInfo): Boolean = info.isLazy || handler.accept(info.value)

    def handle(info: ScalaVariableInfo, loopback: Loopback): JSONObject =
      if (info.isLazy) new JSONObject().put(Names.LAZY, true) else handler.handle(info.value, info.path, loopback)
  }

  private val handlerChain = ListBuffer[AbstractTypeHandler](
    new NullHandler(),
    new StringHandler(stringSizeLimit),
    new ArrayHandler(collectionSizeLimit),
    new JavaCollectionHandler(collectionSizeLimit),
    new SeqHandler(collectionSizeLimit),
    new SetHandler(collectionSizeLimit),
    new MapHandler(collectionSizeLimit),
    new ThrowableHandler(),
    new AnyValHandler(),
    new SpecialsHandler(stringSizeLimit)
  ).map(new HandlerWrapper(_))

  val problems: mutable.Map[String, ReflectionProblem] = mutable.Map[String, ReflectionProblem]()

  private case class ScalaVariableInfo(isAccessible: Boolean,
                                       isLazy: Boolean,
                                       value: Any,
                                       tpe: String,
                                       path: String,
                                       ref: String) {
    val name: String = if (path != null) path.substring(path.lastIndexOf('.') + 1) else null
  }

  case class ReflectionProblem(e: Throwable, symbol: String, var count: Int)

  private class ReferenceWrapper(val ref: AnyRef) {
    override def hashCode(): Int = ref.hashCode()

    override def equals(obj: Any): Boolean = {
      obj match {
        case value: ReferenceWrapper =>
          ref.eq(value.ref)
        case _ => false
      }
    }
  }

  private val refMap = mutable.Map[ReferenceWrapper, String]()
  private val refInvMap = new TrieMap[ReferenceWrapper]()

  /**
   * Returns a reference (e.g. valid path) to the object or creates a record in reference maps (and returns null).
   *
   * @param obj  an object we want to find a reference for (can be null)
   * @param path path of the object e.g. myVar.myField.b
   * @return reference path to the object obj. The method returns null if obj is null itself or
   *         obj hasn't been mentioned earlier or in the case of AnyVal object.
   */
  private def getRef(obj: Any, path: String): String = {
    def clearRefIfPathExists(): Unit = {
      if (refInvMap.contains(path)) {
        val tree = refInvMap.subtree(path)
        tree.forEach(refMap.remove(_: ReferenceWrapper))
      }
    }

    obj match {
      case null | _: Unit =>
        clearRefIfPathExists()
        null
      case ref: AnyRef =>
        val wrapper = new ReferenceWrapper(ref)
        if (refMap.contains(wrapper)) {
          if (refInvMap.get(path) != wrapper) clearRefIfPathExists()
          refMap(wrapper)
        } else {
          clearRefIfPathExists()
          refMap(wrapper) = path
          refInvMap.put(path, wrapper)
          null
        }
      case _ => null
    }
  }

  private implicit def toJavaFunction[A, B](f: A => B): JFunction[A, B] = new JFunction[A, B] {
    override def apply(a: A): B = f(a)
  }

  private def getInfo(name: String): ScalaVariableInfo = {
    val obj = valueOfTerm(name).orNull
    ScalaVariableInfo(isAccessible = true, isLazy = false, obj, typeOfTerm(obj, name), name, null)
  }

  def registerTypeHandler(handler: TypeHandler): VariablesView = {
    if (!isHandlerRegistered(handler.getClass)) handlerChain.insert(1, new HandlerWrapper(handler))
    this
  }

  def isHandlerRegistered[T <: TypeHandler](clazz: Class[T]): Boolean = handlerChain.exists(_.handler.getClass == clazz)

  private def toJson(info: ScalaVariableInfo, depth: Int, path: String): JSONObject = {
    object MyAnyHandler extends AbstractTypeHandler {
      override def accept(obj: Any): Boolean = true

      override def handle(obj: Any, id: String, loopback: Loopback): JSONObject = withJsonObject {
        result =>
          if (depth > 0) withJsonObject {
            tree: JSONObject =>
              if (info.value != null)
                listAccessibleProperties(info).foreach { field =>
                  touched(field.path) = field
                  if (field.ref != null && field.ref != field.path) {
                    tree.put(field.name, new JSONObject().put(Names.REF, field.ref))
                  } else {
                    tree.put(field.name, toJson(field, depth - 1, field.path))
                  }
                }
              if (tree.length() != 0)
                result.put(Names.VALUE, tree)
              else
                result.put(Names.VALUE, obj.toString.take(stringSizeLimit))
          } else {
            result.put(Names.VALUE, obj.toString.take(stringSizeLimit))
          }
          result.put(Names.JVM_TYPE, obj.getClass.getCanonicalName)
      }
    }
    val loopback = new Loopback {
      override def pass(obj: Any, id: String): JSONObject = {
        val si = ScalaVariableInfo(isAccessible = true, isLazy = false, obj, null, id, getRef(obj, id))
        toJson(si, depth - 1, id)
      }
    }
    profile {
      (handlerChain.find(_.accept(info)) match {
        case Some(handler) => handler.handle(info, loopback)
        case _ => MyAnyHandler.handle(info.value, path, loopback)
      }).put(Names.TYPE, info.tpe)
    }
  }

  @inline
  def profile(body: => JSONObject): JSONObject = {
    if (enableProfiling) {
      val t = System.nanoTime()
      body.put("time", System.nanoTime() - t)
    } else body
  }

  private def get(instanceMirror: ru.InstanceMirror, symbol: ru.Symbol, path: String): ScalaVariableInfo = {
    if (!problems.contains(path))
      try {
        // is public property
        if (!symbol.isMethod && symbol.isTerm && symbol.asTerm.getter.isPublic) {
          val term = symbol.asTerm
          val f = instanceMirror.reflectField(term)
          val fieldPath = s"$path.${term.name.toString.trim}"
          val value = f.get
          val tpe = term.typeSignature.toString
          return ScalaVariableInfo(isAccessible = tpe != "<notype>", isLazy = term.isLazy, value, tpe, fieldPath, getRef(value, fieldPath))
        }
      } catch {
        case e: Throwable => problems(path) = ReflectionProblem(e, symbol.toString, 1)
      }
    else problems(path).count += 1

    INACCESSIBLE
  }

  private val INACCESSIBLE = ScalaVariableInfo(isAccessible = false, isLazy = false, null, null, null, null)

  private def listAccessibleProperties(info: ScalaVariableInfo): List[ScalaVariableInfo] = {
    val instanceMirror = mirror.reflect(info.value)
    val instanceSymbol = instanceMirror.symbol
    val members = instanceSymbol.toType.members
    members.map {
      symbol =>
        get(instanceMirror, symbol, info.path)
    }.filter(_.isAccessible).toList
  }

  override def toJson: String = toJsonObject.toString(2)

  override def toJsonObject: JSONObject = {
    val result = new JSONObject()
    variables().filter { name => !blackList.contains(name) }.foreach { name =>
      val info = getInfo(name)
      val ref = getRef(info.value, name)
      if (!(filterUnitResults && isUnitResult(info))) {
        touched(info.path) = info
        if (ref != null && ref != info.path) {
          result.put(info.path, new JSONObject().put(Names.REF, ref))
        } else {
          try {
            result.put(info.path, toJson(info, depth, info.path))
          } catch {
            case _: Exception => //maybe we should log exceptions?
          }
        }
      }
    }
    result
  }

  private def isUnitResult(info: ScalaVariableInfo): Boolean =
    info.name.length > 3 && info.name.startsWith("res") && info.name(3).isDigit && info.tpe == "Unit"

  override def toJsonObject(path: String, deep: Int): JSONObject = {
    val result = new JSONObject()
    val obj = touched(path)
    if (obj.ref != null) {
      result.put("path", path)
      result.put(Names.VALUE, new JSONObject().put(Names.REF, obj.ref))
    } else {
      result.put("path", path)
      result.put(Names.VALUE, toJson(obj, depth, path))
    }
    result
  }

  def typeOfTerm(obj: Any, id: String): String = {
    obj match {
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
      case _ => typeOfExpression(id)
    }
  }
}