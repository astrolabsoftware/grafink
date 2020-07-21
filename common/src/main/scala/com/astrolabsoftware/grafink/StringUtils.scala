package com.astrolabsoftware.grafink

object StringUtils {

  /**
   * Split a string into tuple, based on first match
   * @param s
   */
  implicit class StringToTuple(s: String) {
    def splitToTuple(regex: String): (String, String) =
      s.split(regex, 2) match {
        case Array(str1, str2) => (str1, str2)
        case Array(str1)       => (str1, "")
      }
  }
}
