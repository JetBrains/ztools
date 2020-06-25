/**
 * Copyright 2020 Jetbrains
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jetbrains.ztools.scala

import java.util

class JavaCollectionHandler(limit: Int) extends AbstractCollectionHandler(limit) {
  override def accept(obj: Any): Boolean = obj.isInstanceOf[util.Collection[_]]

  override def iterator(obj: Any): Iterator = new Iterator() {
    private val it = obj.asInstanceOf[util.Collection[_]].iterator()

    override def hasNext: Boolean = it.hasNext

    override def next: Any =  it.next()
  }

  override def length(obj: Any): Int = obj.asInstanceOf[util.Collection[_]].size()
}
