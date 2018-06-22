package com.cloudxlab.logparsing

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

case class Record(host: String, timeStamp: String, url:String, httpCode:Int) extends Serializable

class Utils extends Serializable {
    val PATTERN = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)(.*)" (\d{3}) (\S+)""".r

    def toRecord(line:String):Record = {
        val res = PATTERN.findFirstMatchIn(line)
        if (res.isEmpty) {
            println("Rejected Line: " + line)
            Record("", "", "",  -1)
        }
        else {
            val m = res.get
            Record(m.group(1), m.group(4), m.group(6), m.group(8).toInt)
        }
    }

    def toValidRecords(lines: RDD[String]): RDD[Record] = {
        lines.map(line => toRecord(line)).filter(record => record.httpCode != -1)
    }

    def getTopNUrls(lines:RDD[String], sc:SparkContext, topN:Int):Array[(String,Int)] = {
        var urls = toValidRecords(lines).map(_.url)
        var urls_tuples = urls.map((_, 1));
        var frequencies = urls_tuples.reduceByKey(_ + _);
        var sortedFrequencies = frequencies.sortBy(x => x._2, false)
        return sortedFrequencies.take(topN)
    }

    def getHttpCodes(lines:RDD[String], sc:SparkContext):Array[(Int,Int)] = {
        var urls = toValidRecords(lines).map(_.httpCode)
        var urls_tuples = urls.map((_, 1));
        var frequencies = urls_tuples.reduceByKey(_ + _);
        var sortedFrequencies = frequencies.sortBy(x => x._1, true)
        return sortedFrequencies.collect()
    }

    def getOrderesNTimestamps(lines:RDD[String], sc:SparkContext, topN:Int, ascending:Boolean):Array[(String,Int)] = {
        var timeStamps = toValidRecords(lines).map(_.timeStamp)
        var timeStamps_tuples = timeStamps.map((_, 1));
        var frequencies = timeStamps_tuples.reduceByKey(_ + _);
        var sortedFrequencies = frequencies.sortBy(x => x._2, ascending)
        return sortedFrequencies.take(topN)
    }

    def containsIP(line:String):Boolean = return line matches "^([0-9\\.]+) .*$"
    //Extract only IP
    def extractIP(line:String):(String) = {
        val pattern = "^([0-9\\.]+) .*$".r
        val pattern(ip:String) = line
        return (ip.toString)
    }

    def isClassA(ip:String):Boolean = {
        ip.split('.')(0).toInt < 127
    }

    def getTopNIPs(accessLogs:RDD[String], sc:SparkContext, topn:Int):Array[(String,Int)] = {
        //Keep only the lines which have IP
        var ipaccesslogs = accessLogs.filter(containsIP)
        var cleanips = ipaccesslogs.map(extractIP(_)).filter(isClassA)
        var ips_tuples = cleanips.map((_,1));
        var frequencies = ips_tuples.reduceByKey(_ + _);
        var sortedfrequencies = frequencies.sortBy(x => x._2, false)
        return sortedfrequencies.take(topn)
    }
}

object EntryPoint {
    def main(args: Array[String]) {
        val utils = new Utils

        // Create a local StreamingContext with batch interval of 10 second
        val conf = new SparkConf().setAppName("WordCount")
        val sc = new SparkContext(conf);
        sc.setLogLevel("WARN")

        var accessLogs = sc.textFile("/data/spark/project/NASA_access_log_Aug95.gz")

        println("===== TOP 10 URLS =====")
        val urls = utils.getTopNIPs(accessLogs, sc, 10)
        for(i <- urls){
            println(i)
        }
        println("===== TOP 5 timestamps with high traffic =====")
        val highTimestamps = utils.getOrderesNTimestamps(accessLogs, sc, 5, false)
        for(i <- highTimestamps){
            println(i)
        }
        println("===== TOP 5 timestamps with low traffic =====")
        val lowTimestamps = utils.getOrderesNTimestamps(accessLogs, sc, 5, true)
        for(i <- lowTimestamps){
            println(i)
        }
        println("===== Http Codes =====")
        val httpCodes = utils.getHttpCodes(accessLogs, sc)
        for(i <- httpCodes){
            println(i)
        }

    }
}
