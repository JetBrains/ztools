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
package org.jetbrains.ztools.scala.handlers.impls

class SetHandler(limit: Int,timeout: Int) extends AbstractCollectionHandler(limit,timeout) {
  override def iterator(obj: Any): Iterator = new Iterator {
    private val it = obj.asInstanceOf[Set[_]].iterator

    override def hasNext: Boolean = it.hasNext

    override def next: Any = it.next()
  }

  override def length(obj: Any): Int = obj.asInstanceOf[Set[_]].size

  override def accept(obj: Any): Boolean = obj.isInstanceOf[Set[_]]
}
