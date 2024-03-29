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
package org.jetbrains.ztools.scala

import org.jetbrains.ztools.scala.core.TrieMap
import org.junit.Assert.{assertEquals, assertNull}
import org.junit.Test

class TrieMapTest {
  @Test def testPutGet() = {
    val map = new TrieMap[String]
    map.put("a", "A")
    map.put("a.b", "A.B")
    assertEquals("A", map.get("a").orNull)
    assertEquals("A.B", map.get("a.b").orNull)
    assertNull(map.get("c").orNull)
  }

  @Test def testPutAgain() = {
    val map = new TrieMap[String]
    map.put("a", "A")
    map.put("a.b", "A.B")
    map.put("a.b", "Q")
    map.put("a", "X")
    assertEquals("X", map.get("a").orNull)
    assertNull(map.get("a.b").orNull)
    assertNull(map.get("c").orNull)
  }

  @Test def testSplit() = {
    var s = TrieMap.split("a.b.c")
    assertEquals(3, s.length)
    assertEquals("a", s(0))
    assertEquals("b", s(1))
    assertEquals("c", s(2))
    s = TrieMap.split("abc")
    assertEquals(1, s.length)
    assertEquals("abc", s(0))
  }
}