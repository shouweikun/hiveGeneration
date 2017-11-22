package com.finup.nbsp.adp.HiveUDF.UDF

import org.apache.hadoop.hive.ql.exec.UDF
import com.finup.nbsp.adp.HiveUDF.utils.HiveUtil

/**
  * Created by john_liu on 2017/10/29.
  */
class trackingback extends UDF{
 implicit val hiveUtil = new HiveUtil("","","")

 def evaluate(childcode: Int,ancestorcode :Int): Boolean = {
  //int b=Integer.parseInt(a);
    val acncestorRate = ratejudge(ancestorcode)
    val childcodeRate = ratejudge(childcode)
    val flag = if(childcodeRate<acncestorRate)false else {
     ancestorcode match {
      case 1 => if(ancestorcode/10000 == childcode/10000 ) true else false
      case 2 => if(ancestorcode/100 == childcode/100) true else false
      case 3 => if(ancestorcode == childcode) true else false
     }

    }
     flag
 }

 def evalute(childname:String,ancestorname:String):Boolean = {
   hiveUtil.excuteSql(s"select regions_code from cif_finup_lend.system_region where regions_name = '$ancestorname'")
  false
 }
//  def filterGeneration(parent:String,depth:Int)(re:Set[String]=Set.empty)(implicit hiveUtil: HiveUtil) = {
//     depth match {
//       case 0
//     }
//    val sql = s"select * from tablename where parent_id = \"$parent\""
//    hiveUtil.selectsql(sql).asScala.map{
//          //todo
//      row => filterGeneration(row.get("thefield").toString,depth-1)
//    }
//  }

 def ratejudge(code:Int):Int ={

  code match {
   case x if(x%100>0)   =>3
   case x if(x%10000==0)=>1
   case _               =>2
  }
 }

}
