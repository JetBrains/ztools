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

import scala.reflect.runtime.{universe => ru}
import scala.tools.nsc.interpreter.IMain

class IMainWrapper(val iMain: IMain) {

  import iMain.global._

  import scala.util.{Try => Trying}

  private lazy val importToGlobal = iMain.global mkImporter ru
  private lazy val importToRuntime = ru.internal createImporter iMain.global

  private implicit def importFromRu(sym: ru.Symbol): Symbol = importToGlobal importSymbol sym

  private implicit def importToRu(sym: Symbol): ru.Symbol = importToRuntime importSymbol sym

  def oldValueOf(id: String) = iMain.valueOfTerm(id)

  // see https://github.com/scala/scala/pull/5852/commits/a9424205121f450dea2fe2aa281dd400a579a2b7
  def valueOfTerm(id: String): Option[Any] = exitingTyper {
    def fixClassBasedFullName(fullName: List[String]): List[String] = {
      if (settings.Yreplclassbased.value) {
        val line :: read :: rest = fullName
        line :: read :: "INSTANCE" :: rest
      } else fullName
    }

    def value(fullName: String) = {
      val universe = iMain.runtimeMirror.universe
      import universe.{InstanceMirror, Symbol, TermName}
      val pkg :: rest = fixClassBasedFullName((fullName split '.').toList)
      val top = iMain.runtimeMirror.staticPackage(pkg)

      @annotation.tailrec
      def loop(inst: InstanceMirror, cur: Symbol, path: List[String]): Option[Any] = {
        def mirrored =
          if (inst != null) inst
          else iMain.runtimeMirror reflect (iMain.runtimeMirror reflectModule cur.asModule).instance

        path match {
          case last :: Nil =>
            cur.typeSignature.decls find (x => x.name.toString == last && x.isAccessor) map { m =>
              (mirrored reflectMethod m.asMethod).apply()
            }
          case next :: rest =>
            val s = cur.typeSignature.member(TermName(next))
            val i =
              if (s.isModule) {
                if (inst == null) null
                else iMain.runtimeMirror reflect (inst reflectModule s.asModule).instance
              }
              else if (s.isAccessor) {
                iMain.runtimeMirror reflect (mirrored reflectMethod s.asMethod).apply()
              }
              else {
                assert(false, s.fullName)
                inst
              }
            loop(i, s, rest)
          case Nil => None
        }
      }

      loop(null, top, rest)
    }

    Option(iMain.symbolOfTerm(id)) filter (_.exists) flatMap (s => Trying(value(s.fullName)).toOption.flatten)
  }
}