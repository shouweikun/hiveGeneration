package com.finup.nbsp.adp.HiveUDF.utils

import java.io.File

import scala.sys.process._

/**
  * Created by john_liu on 2017/11/7.
  */
object uploadFileToHDFS {
  def main(args: Array[String]): Unit = {
//
    val filePath = "/Users/john_liu/IdeaProjects/hiveGeneration/src/main/resources/temp"
    val pathToBePut = "hdfs://192.168.176.62:8020/user/hive/warehouse/develop.db/system_regions_flat"
//    val conf = new Configuration
//    val fs = FileSystem.get(conf)
//    val fpath = new Path(filePath)
//    fs.mkdirs(new Path("hdfs://localhost:8020/user/hive/warehouse/cif_finup_lend.db/system_regions"));
//    fs.listStatus(fpath).filter(_.isFile).map{
//      file =>
//        val path = file.getPath
//        fs.copyFromLocalFile(path,new Path(s"$pathToBePut/${path.getName}"))
//    }
//    fs.close()
    val file = new File(filePath)
//    s"hadoop fs -rm -r hdfs://192.168.176.62:8020/user/hive/warehouse/lsw.db/system_regions_flat" !;
   s"hdfs dfs -mkdir -p hdfs://192.168.176.62:8020/user/hive/warehouse/develop.db/system_regions_flat" !;
    file.listFiles.map{
      thefile =>
        val command = s"hdfs dfs -put ${thefile.getAbsolutePath} ${pathToBePut}/${thefile.getName}"
        println(command)
        println(command !)
    }
  }
}
