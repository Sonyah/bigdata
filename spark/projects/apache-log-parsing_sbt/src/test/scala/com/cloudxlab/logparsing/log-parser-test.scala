package com.cloudxlab.logparsing

import org.scalatest.FlatSpec

class LogParserSpec extends FlatSpec {

  "extractIP" should "Extract IP Address" in {
    val utils = new Utils
    var line = "121.242.40.10 - - [03/Aug/2015:06:30:52 -0400] \"POST /mod_pagespeed_beacon?url=http%3A%2F%2Fwww.knowbigdata.com%2Fpage%2Fabout-us HTTP/1.1\" 204 206 \"http://www.knowbigdata.com/page/about-us\" \"Mozilla/5.0 (Windows NT 6.3; WOW64; rv:39.0) Gecko/20100101 Firefox/39.0\""
    var ip = utils.extractIP(line)
    assert(ip == "121.242.40.10")
  }

  "toRecord" should "Extract Record with hostname" in {
    val utils = new Utils
    var line = "uplherc.upl.com - - [01/Aug/1995:00:00:10 -0400] \"GET /images/WORLD-logosmall.gif HTTP/1.0\" 304 0"
    var record = utils.toRecord(line)
    assert(record.host == "uplherc.upl.com")
    assert(record.url == "/images/WORLD-logosmall.gif")
    assert(record.httpCode == 304)
  }

  "toRecord" should "Extract Record with ip" in {
    val utils = new Utils
    var line = "121.242.40.10 - - [01/Aug/1995:00:00:10 -0400] \"GET /images/WORLD-logosmall.gif HTTP/1.0\" 304 0"
    var record = utils.toRecord(line)
    assert(record.host == "121.242.40.10")
    assert(record.url == "/images/WORLD-logosmall.gif")
    assert(record.httpCode == 304)
  }

  "CLASSA" should "Return true if class is A" in {
    val utils = new Utils
    assert(utils.isClassA("121.242.40.10 "))
    assert(!utils.isClassA("212.242.40.10 "))
    assert(!utils.isClassA("239.242.40.10 "))
    assert(!utils.isClassA("191.242.40.10 "))
  }

  "containsIP" should "Check if IP exists in the log line" in {
    val utils = new Utils
    var line1 = "121.242.40.10 - - [03/Aug/2015:06:30:52 -0400] \"POST /mod_pagespeed_beacon?url=http%3A%2F%2Fwww.knowbigdata.com%2Fpage%2Fabout-us HTTP/1.1\" 204 206 \"http://www.knowbigdata.com/page/about-us\" \"Mozilla/5.0 (Windows NT 6.3; WOW64; rv:39.0) Gecko/20100101 Firefox/39.0\""

    assert(utils.containsIP(line1))
    
    var line2 = "::1 - - [11/May/2015:06:44:40 -0400] \"OPTIONS * HTTP/1.0\" 200 125 \"-\" \"Apache/2.4.7 (Ubuntu) PHP/5.5.9-1ubuntu4.7 OpenSSL/1.0.1f (internal dummy connection)\""
    assert(!utils.containsIP(line2))
  }
}
