package com.finup.nbsp.adp.HiveUDF.UDF

import org.apache.hadoop.hive.ql.exec.UDF
import org.apache.hadoop.io.Text

/**
  * Created by john_liu on 2017/11/22.
  */
class rmBracket extends UDF
  {
  val leftBrackets  = Set('(','（','（').map(_.toByte)
  val rightBrackets = Set(')','）','）').map(_.toByte)
  def evaluate(text:Text):Text = {

   val byteIndexArray = (0 until text.getLength).map(x=>(x,text.getBytes()(x)))
   val theLeftBkts    = (byteIndexArray).filter(x=>leftBrackets.contains(x._2)).map(_._1).sortWith((x1,x2)=> x1<=x2)
   val theRightBkts   = (byteIndexArray).filter(x=>rightBrackets.contains(x._2)).map(_._1).sortWith((x1,x2)=> x1<=x2)


    def theFilterSet = {
      (theLeftBkts zip theRightBkts)
        .foldLeft((0 until text.getLength).toSet){
          (set,upandDown)=>
            val up   = upandDown._2
            val down = upandDown._1
            set.filter(x=>x<down||x>up)
        }
    }

    (theLeftBkts.length-theRightBkts.length) match {
      case _         =>{
       val set         = theFilterSet
       val newByteArray= byteIndexArray
                         .filter(x=> set.contains(x._1))
                         .map(_._2).toArray
        println(new  String(newByteArray,"UTF-8"))
        new Text(newByteArray)
      }
//    case 0         =>{
//         theFilterSet
//        }
//    case diff if(diff>0) =>{
//          //todo
//        }
//      case diff if(diff>0) =>{
//        //todo
//      }

      }


    }
  }


