package org.vkhristenko.insightrepo.login

import org.vkhristenko.insightrepo.common._
import java.io._
import scala.collection._

//
// Login Manager - a data structure to keep the record of:
// - who is blocked
// - who has failure login attempts
// - upon block expiration/successful logins - counters/blocks are reset/unset
//
class LoginManager(logFileName: String) {
  // private values
  private val BLOCK_PERIOD = 5L * 60L
  private val FAILURE_PERIOD = 20L
  private val CODE_FAILURE = 401
  private val CODE_SUCCESS = 200

  // output streamer
  val printer = new PrintWriter(new File(logFileName))
  
  // registered hosts that have been blocked for 5 mins
  // TimeStamp shows the time when the block was set
  val hostsBlocked = mutable.HashMap.empty[String, TimeStamp]

  // registered hosts that have failed login attempts
  val hostsWithFailures = mutable.HashMap.empty[String, (TimeStamp, Int)]

  //
  // main interface to outside world
  //
  def register(event: Event): Unit = {
    // evaluate this event if the host making a request is not blocked
    // or the block has expired
    def evaluate: Unit = event.code match {
      case CODE_FAILURE => {
        // already had failures before
        if (hostsWithFailures.contains(event.host)) {
          val failureHostData = hostsWithFailures(event.host)
          if ((event.timestamp - failureHostData._1) > FAILURE_PERIOD)
            // if FAILURE_PERIOD has expired - reset the number of attempts
            // with a new time stamp
            hostsWithFailures(event.host) = (event.timestamp, 1)
          else if (failureHostData._2 == 2) {
            // we are within FAILURE_PERIOD
            // 3rd failure -> block it with this event's time stamp
            hostsBlocked += event.host -> event.timestamp
            // remove this host from failure hosts
            hostsWithFailures -= event.host
          }
          else {
            // within FAILURE_PERIOD but do not yet have 3 failures
            // inc number of failures
            val pair = hostsWithFailures(event.host)
            hostsWithFailures(event.host) = (pair._1, pair._2 + 1)
          }
        }
        else
          // very first failure
          hostsWithFailures += event.host -> (event.timestamp, 1)
      }
      case CODE_SUCCESS => 
        // if this host has failures and event is success - remove from failures
        if (hostsWithFailures.contains(event.host))
          hostsWithFailures -= event.host
      // do nothing for other kinds of requests
      case _ => ()
    }

    // log this event
    def log: Unit = {
      printer.write(event.toRawString + "\n")
    }


    // if this host has been blocked
    if (hostsBlocked.contains(event.host)) {
      if ((event.timestamp - hostsBlocked(event.host)) > BLOCK_PERIOD) {
        // the block has expired - we can remove this host from block and evaluate
        hostsBlocked -= event.host
        // we have to reevaluate this event now!
        evaluate
      }
      else 
        log
    }
    else 
      evaluate
  }
}
