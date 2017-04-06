package org.vkhristenko.insightrepo

import java.util.Calendar

package object common {
  //
  // common data formats
  //
  private val month2Num = Map(
    "Jan" -> 0, "Feb" -> 1, "Mar" -> 2, "Apr" -> 3,
    "May" -> 4, "Jun" -> 5, "Jul" -> 6, "Aug" -> 7,
    "Sep" -> 8, "Oct" -> 9, "Nov" -> 10, "Dec" -> 11
  )
  private val num2Month = Map(
    0 -> "Jan", 1 -> "Feb", 2 -> "Mar", 3 -> "Apr",
    4 -> "May", 5 -> "Jun", 6 -> "Jul", 7 -> "Aug",
    8 -> "Sep", 9 -> "Oct", 10 -> "Nov", 11 -> "Dec"
  )
  case class TimeStamp(str: String) {
    val day = str.slice(0, 2).toInt
    val month = month2Num(str.slice(3, 6))
    val year = str.slice(7, 11).toInt
    val hour = str.slice(12, 14).toInt
    val min = str.slice(15, 17).toInt
    val sec = str.slice(18, 20).toInt

    def toSeconds: Long = {
      val calendar = Calendar.getInstance
      calendar.set(year, month, day, hour, min, sec)
      calendar.getTimeInMillis / 1000L
    }

    def -(other: TimeStamp): Long = this.toSeconds - other.toSeconds
    def -(otherSeconds: Long): Long = this.toSeconds - otherSeconds
  }

  object TimeStamp {
    def fromSeconds(secs: Long): TimeStamp = {
      val calendar = Calendar.getInstance
      calendar.setTimeInMillis(secs * 1000L)
      val sDay = "%02d".format(calendar.get(Calendar.DAY_OF_MONTH))
      val sMonth = num2Month(calendar.get(Calendar.MONTH))
      val sYear = calendar.get(Calendar.YEAR).toString
      val sHour = "%02d".format(calendar.get(Calendar.HOUR_OF_DAY))
      val sMinute = "%02d".format(calendar.get(Calendar.MINUTE))
      val sSecond = "%02d".format(calendar.get(Calendar.SECOND))
      TimeStamp(s"$sDay/$sMonth/$sYear:$sHour:$sMinute:$sSecond -0400")
    }
  }

  case class HTTPRequest(str: String) {
    private val isValid: Boolean =
      str.startsWith("GET") || str.startsWith("POST")
    private val splitted = if (!isValid) null else str.split(" ")
    val method = if (splitted == null) "" else splitted(0)
    val resource = if (splitted == null) "" else splitted(1)
    val http = if (splitted == null) "" else {
      if (splitted.size > 2) splitted(2) else ""
    } 
  }
  abstract class Entry;
  case class Event(host: String, timestamp: TimeStamp, request: HTTPRequest,
                   code: Int, bytes: Long) extends Entry{
    lazy val toRawString = 
      s"""${host} - - [${timestamp.str}] "${request.str}" ${code} ${bytes}"""            
  }

  //
  // Parse with RE. Use a RE to extract the ip/timestamp/request
  //
  def parseWithRE(line: String): Event = {
    // split on space first to get bytes and code 
    val lineSplit0 = line.split(" ")
    val bytesString = lineSplit0(lineSplit0.size-1)
    val bytes = if (bytesString == "-") 0 else bytesString.toLong
    val code = lineSplit0(lineSplit0.size-2).toInt

    // restore the string with space
    val rest = lineSplit0.slice(0, lineSplit0.size-2).mkString(" ")
    val re = """(.*?) - - \[(.*?)\] "(.*?)"""".r
    val (host, timeStamp, request) = rest match {
      case re(aaa, bbb, ccc) => (aaa, bbb, ccc)
      case _ => (null, null, null)
    }

    Event(host, TimeStamp(timeStamp), HTTPRequest(request), code, bytes)
  }
}
