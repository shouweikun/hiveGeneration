package com.finup.nbsp.adp.HiveUDF.utils

import java.io.FileOutputStream
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils

/**
  * Created by john_liu on 2017/11/7.
  */
object copyFileFromHDFS {
  def main(args: Array[String]): Unit = {
    val dest = "hdfs://localhost:8020/temp/fuck"
    val local = "/Users/john_liu/IdeaProjects/hiveGeneration/src/main/resources/temp"
    val conf = new Configuration
    //conf.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
    val fs = FileSystem.get(new URI("hdfs://localhost:8020"), conf)
    val fileStatus = fs.listStatus(new Path(dest))
    fileStatus.filter(_.isFile).par.map{
      file =>
        val path = file.getPath
        IOUtils.copyBytes(fs.open(path), new FileOutputStream(s"$local/${path.getName}"), 4096, true)
    }
    fs.close()
  }
}
