package com.finup.nbsp.adp.HiveUDF

import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

import scala.annotation.tailrec

/**
  * Created by john_liu on 2017/11/2.
  */
object TablaFlatPendingbyCode4Jiea {



  //尾递归，循环列表第生成code组合
  @tailrec
  def loopcombine(codelist:List[Long],re:List[(Long,Long)]=List.empty): List[(Long,Long)] = {
    codelist match {
      case head :: tail=>loopcombine(tail,re ::: codelist.map(x=>(head,x)))
      case Nil         =>re
    }
  }
  //判断地域的级别
  def ratejudge(code:Long):Long ={

    code match {
      case x if(x%100>0)   =>3
      case x if(x%10000==0)=>1
      case _               =>2
    }
  }

  def main(args: Array[String]): Unit = {

//    val option = new Options
//    option.addOption("resulttable",true,"result table")
//    option.addOption("sourcetable",true,"source table")
//    val parser = new DefaultParser()
//    val c1     = parser.parse(option,args)
    val Appname = "TablaFlatPendingbyCode"
    val master = "local"
    //结果表名
    val resultTableName = "develop.jiea_system_region_flat"
      //Option(c1.getOptionValue("resulttable")).getOrElse{println("lack of result table name");sys.exit(1)}
    //临时表名
    val tempTableName   = "lsw_test"
    //数据源表名
    val sourceTableName = "cif_jiea.system_region"
      //Option(c1.getOptionValue("sourcetable")).getOrElse{println("lack of source table name");sys.exit(1)}
    //Spark 上下文
    val sparkConf = new SparkConf().setAppName(Appname)//.setMaster(master)
    val sc = new SparkContext(sparkConf)
    //Hive  上下文
    val hiveContext = new HiveContext(sc)
    val sql = s"select * from ${sourceTableName}"
    //全量数据拉取
    val fulldata   = hiveContext.sql(sql)
   // val fulldata    = hiveContext.read.parquet("hdfs://localhost:8020/user/hive/warehouse/cif_finup_lend.db/system_region/*.parquet")
    //只保留code 和 名字
    val fulldataAsRDD = fulldata.map{
      row =>
        val code =row.getAs[Long]("region_code")
        val name =row.getAs[String]("region_name")
        (code,name)
    }
    //val metadata   = fulldata.columns.map(StructField(_,DataTypes.StringType))
    //新表列的元数据
    val metadata      = Array("region_code","region_rate","ancestor_region_code","ancestor_region_rate").map(StructField(_,DataTypes.LongType))
    //过滤出叶子数据
    val leafdataAsRDD = fulldataAsRDD.filter(_._1%100>0).repartition(1)
    //列举所有情况
    val kankan        = leafdataAsRDD.flatMap{
    info =>
      val code     = info._1
      val codelist = List(code,code-code%100,(code/10000)*10000)
      //尾递归生成code组合
      loopcombine(codelist)
    }
      //去重
      .distinct()
      //排序
      .sortBy{
      x => x
    }.map{
      //为每个code加上rate
      info=>Row.fromSeq(Seq(info._1,ratejudge(info._1),info._2,ratejudge(info._2)))
    }.cache
    println(kankan.map{
      x =>
       // test
       // println(x)
        1
    }.reduce(_+_))
    hiveContext.createDataFrame(kankan,StructType(metadata))
      .registerTempTable(tempTableName)

//    //test
//    hiveContext.read.parquet("hdfs://localhost:8020/temp/1106/*").rdd.map{
//      x =>
//        println(x)
//        1
//    }.reduce(_+_)
//      primeTable.registerTempTable(s"$tempTableName")
    hiveContext.sql(s"drop table if exists $resultTableName ").show()
    hiveContext.sql(s"Create table $resultTableName stored as parquet as select a.region_code,b.region_name,a.region_rate,a.ancestor_region_code ,a.ancestor_region_rate ,c.region_name as ancestor_region_name FROM $tempTableName a INNER JOIN $sourceTableName b ON a.region_code = b.region_code INNER JOIN $sourceTableName c On a.ancestor_region_code = c.region_code")//.write.parquet("hdfs://localhost:8020/temp/fuckee")


  }

}
