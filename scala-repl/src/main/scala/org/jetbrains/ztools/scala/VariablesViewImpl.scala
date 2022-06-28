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

import org.apache.commons.lang.exception.ExceptionUtils
import org.jetbrains.ztools.scala.core.{Loopback, Names, TrieMap, TypeHandler}
import org.jetbrains.ztools.scala.handlers._
import org.json4s.jackson.Serialization
import org.json4s.{Formats, NoTypeHints}
import spark.handlers.{DatasetHandler, RDDHandler, SparkContextHandler, SparkSessionHandler}

import java.util.function.{Function => JFunction}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.language.implicitConversions

abstract class VariablesViewImpl(val timeout: Int,
                                 val collectionSizeLimit: Int,
                                 val stringSizeLimit: Int,
                                 val blackList: List[String],
                                 val whiteList: List[String] = null,
                                 val filterUnitResults: Boolean,
                                 val enableProfiling: Boolean,
                                 val depth: Int) extends VariablesView {
  private val ru = scala.reflect.runtime.universe
  private val mirror = ru.runtimeMirror(getClass.getClassLoader)
  private val touched = mutable.Map[String, ScalaVariableInfo]()

  private val startTime = System.currentTimeMillis()

  class HandlerWrapper(val handler: TypeHandler) {
    def accept(info: ScalaVariableInfo): Boolean = info.isLazy || handler.accept(info.value)

    def handle(info: ScalaVariableInfo, loopback: Loopback): mutable.Map[String, Any] =
      if (info.isLazy) {
        val data = mutable.Map[String, Any]()
        data += (Names.LAZY -> true)
        data
      }
      else
        handler.handle(info.value, info.path, loopback)
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
    new SpecialsHandler(stringSizeLimit),
    new DatasetHandler(),
    new RDDHandler(),
    new SparkContextHandler(),
    new SparkSessionHandler()
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

    override def equals(obj: Any): Boolean = obj match {
      case value: ReferenceWrapper =>
        ref.eq(value.ref)
      case _ => false
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
          if (refInvMap.get(path).orNull != wrapper) clearRefIfPathExists()
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

  //noinspection ScalaUnusedSymbol
  private implicit def toJavaFunction[A, B](f: A => B): JFunction[A, B] = new JFunction[A, B] {
    override def apply(a: A): B = f(a)
  }

  private def getInfo(name: String): ScalaVariableInfo = {
    val obj = valueOfTerm(name).orNull
    ScalaVariableInfo(isAccessible = true, isLazy = false, obj, typeOfTerm(obj, name), name, null)
  }
  
  private def toJson(info: ScalaVariableInfo, depth: Int, path: String): mutable.Map[String, Any] = {
    object MyAnyHandler extends AbstractTypeHandler {
      override def accept(obj: Any): Boolean = true

      override def handle(obj: Any, id: String, loopback: Loopback): mutable.Map[String, Any] = withJsonObject {
        result =>
          if (depth > 0) withJsonObject {
            tree: mutable.Map[String, Any] =>
              if (info.value != null)
                listAccessibleProperties(info).foreach { field =>
                  touched(field.path) = field
                  if (field.ref != null && field.ref != field.path) {
                    tree += (field.name -> (mutable.Map[String, Any]() += (Names.REF -> field.ref)))
                  } else {
                    tree += (field.name -> toJson(field, depth - 1, field.path))
                  }
                }
              if (tree.nonEmpty)
                result += (Names.VALUE -> tree)
              else
                result += (Names.VALUE -> obj.toString.take(stringSizeLimit))
          } else {
            result += (Names.VALUE -> obj.toString.take(stringSizeLimit))
          }
          result += (Names.JVM_TYPE -> obj.getClass.getCanonicalName)
      }
    }
    val loopback = new Loopback {
      override def pass(obj: Any, id: String): mutable.Map[String, Any] = {
        val si = ScalaVariableInfo(isAccessible = true, isLazy = false, obj, null, id, getRef(obj, id))
        toJson(si, depth - 1, id)
      }
    }
    profile {
      val res = handlerChain.find(_.accept(info)) match {
        case Some(handler) => handler.handle(info, loopback)
        case _ => MyAnyHandler.handle(info.value, path, loopback)
      }
      if (info.tpe != null)
        res += (Names.TYPE -> info.tpe)
      res
    }
  }

  @inline
  def profile(body: => mutable.Map[String, Any]): mutable.Map[String, Any] = {
    if (enableProfiling) {
      val t = System.nanoTime()
      val newBody: mutable.Map[String, Any] = body
      newBody += ("time" -> (System.nanoTime() - t))
      newBody
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
    else
      problems(path).count += 1

    INACCESSIBLE
  }

  private val INACCESSIBLE = ScalaVariableInfo(isAccessible = false, isLazy = false, null, null, null, null)

  private def listAccessibleProperties(info: ScalaVariableInfo): List[ScalaVariableInfo] = {
    val instanceMirror = mirror.reflect(info.value)
    val instanceSymbol = instanceMirror.symbol
    val members = instanceSymbol.toType.members
    members.map {
      symbol => get(instanceMirror, symbol, info.path)
    }.filter(_.isAccessible).toList
  }

  def toFullJson: String = {
    val errors = problems.map { case (name, refProblem) =>
      f"$name: Reflection error for ${refProblem.symbol} counted ${refProblem.count} times.\n" +
        f"${ExceptionUtils.getMessage(refProblem.e)}\n${ExceptionUtils.getStackTrace(refProblem.e)}"
    }.toList

    implicit val ztoolsFormats: AnyRef with Formats = Serialization.formats(NoTypeHints)
    Serialization.write(
      Map(
        "variables" -> toJsonObject,
        "errors" -> (errors ++ this.errors.toList)
      )
    )
  }

  override def toJson: String = {
    implicit val ztoolsFormats: AnyRef with Formats = Serialization.formats(NoTypeHints)
    Serialization.write(toJsonObject)
  }

  override def toJsonObject: mutable.Map[String, Any] = {
    val result = mutable.Map[String, Any]()
    variables()
      .filter { name => !blackList.contains(name) }
      .filter { name => whiteList == null || whiteList.contains(name) }
      .foreach { name =>
        if (System.currentTimeMillis() - startTime > timeout) {
          errors += s"Variables collect timeout. Exceed ${timeout}ms."
          return result
        }
        try {
          val info = getInfo(name)
          val ref = getRef(info.value, name)
          if (!(filterUnitResults && isUnitResult(info))) {
            touched(info.path) = info
            if (ref != null && ref != info.path) {
              result += (info.path -> (mutable.Map[String, Any]() += (Names.REF -> ref)))
            } else {
              result += info.path -> toJson(info, depth, info.path)
            }
            result
          }
        } catch {
          case t: Throwable => errors +=
            f"${ExceptionUtils.getRootCauseMessage(t)}\n${ExceptionUtils.getStackTrace(ExceptionUtils.getRootCause(t))}"
        }
      }
    result
  }

  private def isUnitResult(info: ScalaVariableInfo): Boolean =
    info.name.length > 3 && info.name.startsWith("res") && info.name(3).isDigit && info.tpe == "Unit"

  override def toJsonObject(path: String, deep: Int): mutable.Map[String, Any] = {
    val result = mutable.Map[String, Any]()
    val obj = touched(path)
    if (obj.ref != null) {
      result += ("path" -> path)
      result += (Names.VALUE -> mutable.Map[String, Any]()) += (Names.REF -> obj.ref)
    } else {
      result += ("path" -> path)
      result += (Names.VALUE -> toJson(obj, depth, path))
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