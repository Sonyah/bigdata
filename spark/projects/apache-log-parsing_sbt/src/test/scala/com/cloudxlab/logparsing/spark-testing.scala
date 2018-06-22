package com.cloudxlab.logparsing

import org.scalatest.FunSuite
import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}

class SampleTest extends FunSuite with SharedSparkContext {
    test("Computing top10") {
        var line1 = "121.242.40.10 - - [03/Aug/2015:06:30:52 -0400] \"POST /mod_pagespeed_beacon?url=http%3A%2F%2Fwww.knowbigdata.com%2Fpage%2Fabout-us HTTP/1.1\" 204 206 \"http://www.knowbigdata.com/page/about-us\" \"Mozilla/5.0 (Windows NT 6.3; WOW64; rv:39.0) Gecko/20100101 Firefox/39.0\""
        var line2 = "::1 - - [11/May/2015:06:44:40 -0400] \"OPTIONS * HTTP/1.0\" 200 125 \"-\" \"Apache/2.4.7 (Ubuntu) PHP/5.5.9-1ubuntu4.7 OpenSSL/1.0.1f (internal dummy connection)\""

        val utils = new Utils

        val list = List(line1, line2)
        val rdd = sc.parallelize(list);

        assert(rdd.count === list.length)   

        val records = utils.getTopNIPs(rdd, sc, 10)
        assert(records.length === 1)    
        assert(records(0)._1 == "121.242.40.10")
    }
    test("Should remove IP addresses having 1st Octet Decimal more than 126") {
        var line1 = "121.242.40.10 - - [03/Aug/2015:06:30:52 -0400] \"POST /mod_pagespeed_beacon?url=http%3A%2F%2Fwww.knowbigdata.com%2Fpage%2Fabout-us HTTP/1.1\" 204 206 \"http://www.knowbigdata.com/page/about-us\" \"Mozilla/5.0 (Windows NT 6.3; WOW64; rv:39.0) Gecko/20100101 Firefox/39.0\""
        var line2 = "216.113.160.77 - - [03/Aug/2015:06:30:52 -0400] \"POST /mod_pagespeed_beacon?url=http%3A%2F%2Fwww.knowbigdata.com%2Fpage%2Fabout-us HTTP/1.1\" 204 206 \"http://www.knowbigdata.com/page/about-us\" \"Mozilla/5.0 (Windows NT 6.3; WOW64; rv:39.0) Gecko/20100101 Firefox/39.0\""
        val list = List(line1, line2)
        val rdd = sc.parallelize(list);

        val utils = new Utils
        val records = utils.getTopNIPs(rdd, sc, 10)
        assert(records.length === 1)
        assert(records(0)._1 == "121.242.40.10")
    }

    test("Computing Top 10 urls") {
        var line1 = "kgtyk4.kj.yamagata-u.ac.jp - - [01/Aug/1995:00:00:32 -0400] \"GET /images/NASA-logosmall.gif HTTP/1.0\" 200 786"
        var line2 = "133.43.96.45 - - [01/Aug/1995:00:00:32 -0400] \"GET /images/USA-logosmall.gif HTTP/1.0\" 200 786"
        var line3 = "piweba4y.prodigy.com - - [01/Aug/1995:00:00:32 -0400] \"GET /images/NASA-logosmall.gif HTTP/1.0\" 200 786"

        val list = List(line1, line2, line3)
        val rdd = sc.parallelize(list);

        val utils = new Utils
        val records = utils.getTopNUrls(rdd, sc, 10)
        assert(records.length === 2)
        assert(records(0)._1 == "/images/NASA-logosmall.gif")
        assert(records(0)._2 == 2)
        assert(records(1)._1 == "/images/USA-logosmall.gif")
        assert(records(1)._2 == 1)
    }

    test("Computing Top 5 high time frames") {
        var line1 = "kgtyk4.kj.yamagata-u.ac.jp - - [02/Aug/1995:00:00:32 -0400] \"GET /images/NASA-logosmall.gif HTTP/1.0\" 200 786"
        var line2 = "133.43.96.45 - - [01/Aug/1995:00:00:32 -0400] \"GET /images/USA-logosmall.gif HTTP/1.0\" 200 786"
        var line3 = "piweba4y.prodigy.com - - [01/Aug/1995:00:00:32 -0400] \"GET /images/NASA-logosmall.gif HTTP/1.0\" 200 786"

        val list = List(line1, line2, line3)
        val rdd = sc.parallelize(list);

        val utils = new Utils
        val records = utils.getOrderesNTimestamps(rdd, sc, 5, false)
        assert(records.length === 2)
        assert(records(0)._1 == "01/Aug/1995:00:00:32 -0400")
        assert(records(0)._2 == 2)
        assert(records(1)._1 == "02/Aug/1995:00:00:32 -0400")
        assert(records(1)._2 == 1)
    }

    test("Computing Top 5 low time frames") {
        var line1 = "kgtyk4.kj.yamagata-u.ac.jp - - [02/Aug/1995:00:00:32 -0400] \"GET /images/NASA-logosmall.gif HTTP/1.0\" 200 786"
        var line2 = "133.43.96.45 - - [01/Aug/1995:00:00:32 -0400] \"GET /images/USA-logosmall.gif HTTP/1.0\" 200 786"
        var line3 = "piweba4y.prodigy.com - - [01/Aug/1995:00:00:32 -0400] \"GET /images/NASA-logosmall.gif HTTP/1.0\" 200 786"

        val list = List(line1, line2, line3)
        val rdd = sc.parallelize(list);

        val utils = new Utils
        val records = utils.getOrderesNTimestamps(rdd, sc, 5, true)
        assert(records.length === 2)
        assert(records(1)._1 == "01/Aug/1995:00:00:32 -0400")
        assert(records(1)._2 == 2)
        assert(records(0)._1 == "02/Aug/1995:00:00:32 -0400")
        assert(records(0)._2 == 1)
    }

    test("Computing Http codes") {
        var line1 = "kgtyk4.kj.yamagata-u.ac.jp - - [02/Aug/1995:00:00:32 -0400] \"GET /images/NASA-logosmall.gif HTTP/1.0\" 304 786"
        var line2 = "133.43.96.45 - - [01/Aug/1995:00:00:32 -0400] \"GET /images/USA-logosmall.gif HTTP/1.0\" 200 786"
        var line3 = "piweba4y.prodigy.com - - [01/Aug/1995:00:00:32 -0400] \"GET /images/NASA-logosmall.gif HTTP/1.0\" 404 786"
        var line4 = "piweba4y.prodigy.com - - [01/Aug/1995:00:00:32 -0400] \"GET /images/NASA-logosmall.gif HTTP/1.0\" 200 786"

        val list = List(line1, line2, line3, line4)
        val rdd = sc.parallelize(list);

        val utils = new Utils
        val records = utils.getHttpCodes(rdd, sc)
        assert(records.length === 3)
        assert(records(0)._1 == 200)
        assert(records(0)._2 == 2)
        assert(records(1)._1 == 304)
        assert(records(1)._2 == 1)
        assert(records(2)._1 == 404)
        assert(records(2)._2 == 1)

    }
}
