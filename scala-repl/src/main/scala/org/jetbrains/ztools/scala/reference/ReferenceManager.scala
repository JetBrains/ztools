package org.jetbrains.ztools.scala.reference

import org.jetbrains.ztools.scala.core.TrieMap

import scala.collection.mutable

class ReferenceManager {
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
  def getRef(obj: Any, path: String): String = obj match {
    case null | _: Unit =>
      clearRefIfPathExists(path)
      null
    case ref: AnyRef =>
      val wrapper = new ReferenceWrapper(ref)
      if (refMap.contains(wrapper)) {
        if (refInvMap.get(path).orNull != wrapper) clearRefIfPathExists(path)
        refMap(wrapper)
      } else {
        clearRefIfPathExists(path)
        refMap(wrapper) = path
        refInvMap.put(path, wrapper)
        null
      }
    case _ => null
  }


  private def clearRefIfPathExists(path: String): Unit = {
    if (refInvMap.contains(path)) {
      val tree = refInvMap.subtree(path)
      tree.forEach(refMap.remove(_: ReferenceWrapper))
    }
  }
}