package org.vkhristenko.insightrepo.frame

import org.vkhristenko.insightrepo.common._

class TimeFrame(startStamp: TimeStamp) {
  // reference of our sliding time frame
  var timeRef: TimeStamp = startStamp
  // 10 current busiest guys
  var busiest: Array[(String, Long)] = Array.fill[(String, Long)](10)(("", 0L))
  // the 3600 secs sliding window
  var window: Array[Long] = Array.fill[Long](3600)(0L)

  //
  // increment the bins that are within time frame (1h)
  //
  def fill(event: Event): Unit = 
    for (ibin <- 0 until window.length; 
         // if delta time is not less than 0!
         if event.timestamp - (timeRef.toSeconds + ibin.toLong) >= 0L)
      window(ibin) += 1L

  //
  // Register this event within the TimeFrame
  //
  def register(event: Event): Unit = {
    var ibin = 0
    var found = false
    // first identify which bin is the first one within the 3600L secs
    while (ibin < window.length && !found) {
      if (event.timestamp - (timeRef.toSeconds + ibin.toLong) < 3600L) 
        found = true
      else
        ibin += 1
    }

    // if this bin is the first one in the array - just fill
    if (ibin==0) fill(event)
    else { 
      // flush to busiest array first few bins that are no longer in the window
      flush(window.slice(0, ibin))
      // move the window
      window = window.slice(ibin, window.length-1) ++ Array.fill[Long](window.length-ibin)(0L)
      // reassign the time reference
      timeRef = TimeStamp.fromSeconds(timeRef.toSeconds + ibin.toLong)
      // fill the bins
      fill(event)
    }
  }

  //
  // flush - merge with busiest/sort/take top 10
  // since at most we have to sort 3600 + 10, no ordered insert is implemented
  //
  def flush(arr: Array[Long]): Unit = {
    val stamps = for (i <- 0 until busiest.length) yield TimeStamp.fromSeconds(
      timeRef.toSeconds + i.toLong).str
    busiest = (stamps.zip(arr) ++ busiest).sortWith(_._2 > _._2).take(10).toArray
  }
}
