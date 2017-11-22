package com.finup.nbsp.adp.HiveUDF

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by john_liu on 2017/11/1.
  */
object TableFlat {
  def main(args: Array[String]): Unit = {

    val Appname = ""
    val master = ""
    //Spark 上下文
    val sparkConf = new SparkConf().setAppName(Appname).setMaster(master)
    val sc = new SparkContext(sparkConf)
    //Hive  上下文
    val hiveContext = new HiveContext(sc)
    val UpperCode = "-1"
    val UpperRate = 1
    val depth = 0
   // println(loopflat(List(UpperCode), UpperRate.toString, 2))
  }
//  @tailrec
//  def loopflat(info:List[String],rate:String,depth:Int,re:List[List[(String,String,String)]]=List.empty):List[List[(String,String,String)]] = {
//    depth match {
//      case 0  => re
//      case _  =>
//        val nextrate        = (rate.toInt+1).toString
//        val childeninfolist = getchildren(nextrate,info)
//        loopflat(childeninfolist.map(_._3),nextrate,depth-1,re.:+(childeninfolist))
//    }
//  }

//  def getchildren(rate:String,parentcode:List[String])(implicit hiveContext: HiveContext):List[(String,String,String)] ={
//    val sql = s"select * from cif_finup_lend.system_regions where parent_code in (${parentcode.map(code => s"'$code'").mkString(",")})"
//    vahiveContext.sql(sql).rdd.map {
//      //      row =>
//      //        val regions_code = row.getAs[String]("regions_code")
//      //        val regions_name = row.getAs[String]("regions_name")
//      //        (rate,regions_name,regions_code)
//      //    }
//      //  }
//    }
//  def generateFamilyTree(info:RDD[List[(String,String)]])(implicit hiveContext: HiveContext):RDD[List[(String,String)]] = {
//
//    // val sql = s"select * from cif_finup_lend.system_regions where parent_code in (${parentcode.map(code => s"'$code'").mkString(",")})"
//    // hiveContext.createDataFrame(info,)
//  }

}
