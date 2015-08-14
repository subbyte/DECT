/*
 *  Distributed Evolving Context Tree (DECT)
 *      -- A Time-inhomogeneous Variable-order Markov Chain Model
 *
 *  Author: Xiaokui Shu
 *  Version: 1.0.0
 *  License: MIT
 *  Email: subx@cs.vt.edu
 *    
 *  Copyright 2015, Yahoo Inc.
 */


import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.HashPartitioner

import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat

import com.typesafe.config.ConfigFactory

import org.rogach.scallop._


class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {
  override def generateActualKey(key: Any, value: Any): Any = 
    NullWritable.get()

  override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String = 
    key.asInstanceOf[String]
}


class ArgsConf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val adonly = opt[Boolean](descr = "Whether or not to process ad-targets only")
  val parallelism = opt[Int](default = Some(64),
    descr = "number of partitions specified in the data importing step")
  val exfilter = opt[Int](default = Some(1),
    descr = "filter out timeseries with less than exfilter number of visits")
  val source = trailArg[String](required = true,
    descr = "Input data, could be \"directory/*\"")
  val output = trailArg[String](required = true,
    descr = "Output Directory, existing directory will be removed")
}


object Dect {

  val BucketSize = ConfigFactory.load().getInt("BucketSize")
  val VGramMaxN = ConfigFactory.load().getInt("VGramMaxN")
  val WinSize = ConfigFactory.load().getInt("WinSize")
  val StartSymbol = ConfigFactory.load().getString("DataSymbol.Start")
  val VGramSpliter:Char = ConfigFactory.load().getString("DataSymbol.VGramSpliter")(0)
  val RecordsPerPartitionOutput = ConfigFactory.load().getInt("Output.RecordsPerPartition")

  def main(args: Array[String]) {
    val argsMap = new ArgsConf(args)
    val conf = new SparkConf()
      .setAppName("Distributed Evolving Context Tree")
      .set("spark.streaming.unpersist", "true")
      .set("spark.yarn.am.memory", "3g")
      .set("spark.yarn.am.cores", "6")
      .set("spark.driver.cores", "6")
    val sc = new SparkContext(conf)
    val hadoopConf = new org.apache.hadoop.conf.Configuration()

    val BucketSizeBC = sc.broadcast(BucketSize)
    val sessions = sc.textFile(argsMap.source(), minPartitions=argsMap.parallelism())
      .map(rawLine => {
        val l = rawLine.trim split '\t'
        ((l(0).toInt/BucketSizeBC.value, l(1)), 1)
      }).reduceByKey(_ + _).persist

    val sessionCNT = sessions.map(s => s._2).reduce(_ + _)

    val sessionTimestamps = sessions.map(r => r._1._1).persist
    val TimeStart = sessionTimestamps.min
    val TimeEnd = sessionTimestamps.max
    sessionTimestamps.unpersist()

    // VGramsCountsInBucket: ((bucket timestamp, context list, target), cnt)
    // all records including ad-targeted ones
    val VGramMaxNBC = sc.broadcast(VGramMaxN)
    val StartSymbolBC = sc.broadcast(StartSymbol)
    val VGramSpliterBC = sc.broadcast(VGramSpliter)
    val VGramsCountsInBucket= sessions.flatMap(line => {
      val items = StartSymbolBC.value :: (line._1._2 split VGramSpliterBC.value).toList
      val l = if (VGramMaxNBC.value < items.length) VGramMaxNBC.value else items.length
      val VGramSpliterSTR = VGramSpliterBC.value.toString
      (for (ssize <- 1 to l) yield items.sliding(ssize).map(g => {
        val gram = g.toSeq
        val target = gram.last
        val context = gram.init mkString VGramSpliterSTR
        ((line._1._1, context, target), line._2)
      })).flatMap(identity)
    }).reduceByKey(_ + _)

    // VGramsCountsInWin: ((win timestamp, context list, target), cnt)
    //   - create fake early win start time, which < TimeStart
    //   - need to take care of when generating timeseries
    val WinSizeBC = sc.broadcast(WinSize)
    val VGramsCountsInWin = VGramsCountsInBucket.flatMap(row => 
      for (r <- 0 until WinSizeBC.value) yield ((row._1._1 - r, row._1._2, row._1._3), row._2)
    ).reduceByKey(_ + _)

    // TCTC: ((timestamp, context), (target, cnt)) in window
    val TCTC = VGramsCountsInWin
      .map(row => ((row._1._1, row._1._2), (row._1._3, row._2)))
      .persist

    // contextCountsInWin: ((timestamp, context), cnt)
    val contextCountsInWin = TCTC.mapValues(v => v._2).reduceByKey(_ + _)

    // probsAll: ((context, target), (timestamp, (prob, cnt)))
    //   - right after join: ((timestamp context), ((target, cnt), cnt))
    val probsAll = TCTC
      .join(contextCountsInWin)
      .map(row => ((row._1._2, row._2._1._1), (row._1._1, (row._2._1._2.toFloat/row._2._2, row._2._1._2))))
      .persist

    // CTabandonedLength: ((context, target), cnt) if cnt > argsMap.exfilter()
    // if count(A.B.C->D) >= count(B.C->D), so the first is definitely pruned if the later is
    val ThresAppearanceBC = sc.broadcast(argsMap.exfilter())
    val CTabandonedLength = probsAll
      .mapValues(v => 1)
      .reduceByKey(_ + _)
      .filter(row => row._2 < ThresAppearanceBC.value)

    // probs: ((context, target), (timestamp, (prob, cnt))
    val probs = probsAll subtractByKey CTabandonedLength

    // TimeSeries: ((context, target), timeseries: ((p1, cnt1), (p2, cnt2), (p3, cnt3), ...))
    val TimeStartBC = sc.broadcast(TimeStart)
    val TimeEndBC = sc.broadcast(TimeEnd)
    val TimeSeries = probs.groupByKey.mapValues(v => {
      val tsMap = v.toMap
      // There are fake win earlier than TimeStart, just ignore them
      for (ts <- TimeStartBC.value to TimeEndBC.value) yield {
        if (tsMap contains ts) tsMap(ts) else (0, 0)
      }
    })

    val TimeSeriesOrderClusters = TimeSeries
      .map(row =>
          ("timeseries." + (
            if (row._1._1.length == 0) 0 else ((row._1._1 count (_ == VGramSpliterBC.value)) + 1)
          ).toString,
          row))
      .persist

    val tsOrderCNT = TimeSeriesOrderClusters
      .map(row => (row._1 split '.')(1).toInt)
      .countByValue
      .toList
      .sorted
    val tsCNT = tsOrderCNT.toMap.values.sum

    val extractedTargetCNT = TimeSeriesOrderClusters.map(row => row._2._1._2).distinct.count

    // store the results
    TimeSeriesOrderClusters
      .partitionBy(new HashPartitioner((tsCNT.toDouble/RecordsPerPartitionOutput).ceil.toInt))
      .saveAsHadoopFile(
        argsMap.output(),
        classOf[String],
        classOf[String],
        classOf[RDDMultipleTextOutputFormat])

    // print statistics
    println("[INFO] Processed: %d sessions, %d stored targets".format(
      sessionCNT,
      extractedTargetCNT))

    println("[INFO] Start time: %d, Batch size: %d seconds, Batch CNT: %d".format(
      TimeStart * BucketSize,
      BucketSize,
      TimeEnd - TimeStart + 1))

    println("[INFO] Stored: %d timeseries (%s).".format(
      tsCNT,
      tsOrderCNT map(t => "%d-order: %d".format(t._1, t._2)) mkString "; "))
  }
}
