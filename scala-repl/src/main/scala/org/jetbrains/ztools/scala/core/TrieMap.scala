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
 *//**
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
package org.jetbrains.ztools.scala.core

import java.util.function.Function
import scala.collection.mutable

object TrieMap {
  class Node[T](var value: Option[T]) {
    private[core] var children: mutable.Map[String, TrieMap.Node[T]] = _

    def put(key: String, node: TrieMap.Node[T]): Option[Node[T]] = {
      if (children == null)
        children = mutable.Map[String, TrieMap.Node[T]]()
      children.put(key, node)
    }

    def del(key: String): Option[Node[T]] = children.remove(key)

    def forEach(func: Function[T, _]): Unit = {
      func.apply(value.get)
      if (children != null) children.foreach(t => t._2.forEach(func))
    }
  }

  def split(key: String): Array[String] = {
    var n = 0
    var j = 0
    for (i <- 0 until key.length) {
      if (key.charAt(i) == '.') n += 1
    }
    val k = new Array[String](n + 1)
    val sb = new mutable.StringBuilder(k.length)
    for (i <- 0 until key.length) {
      val ch = key.charAt(i)
      if (ch == '.') {
        k({
          j += 1;
          j - 1
        }) = sb.toString
        sb.setLength(0)
      }
      else sb.append(ch)
    }
    k(j) = sb.toString
    k
  }
}

class TrieMap[T] {
  private[core] val root = new TrieMap.Node[T](null)

  def subtree(key: Array[String], length: Int): TrieMap.Node[T] = {
    var current = root
    var i = 0
    while ( {
      i < length && current != null
    }) {
      if (current.children == null) return null
      current = current.children.get(key(i)).orNull
      i += 1
    }
    current
  }

  def put(key: Array[String], value: T): Option[TrieMap.Node[T]] = {
    val node = subtree(key, key.length - 1)
    node.put(key(key.length - 1), new TrieMap.Node[T](Option.apply(value)))
  }

  def put(key: String, value: T): Option[TrieMap.Node[T]] = {
    val k = TrieMap.split(key)
    put(k, value)
  }

  def contains(key: String): Boolean = {
    val k = TrieMap.split(key)
    val node = subtree(k, k.length)
    node != null
  }

  def get(key: String): Option[T]= {
    val k = TrieMap.split(key)
    val node = subtree(k, k.length)
    if (node == null) return Option.empty
    node.value
  }

  def subtree(key: String): TrieMap.Node[T] = {
    val k = TrieMap.split(key)
    subtree(k, k.length)
  }
}