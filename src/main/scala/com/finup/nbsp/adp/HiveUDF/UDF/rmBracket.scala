package com.finup.nbsp.adp.HiveUDF.UDF

import org.apache.hadoop.hive.ql.exec.UDF
import org.apache.hadoop.io.Text
import scala.util.control._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by john_liu on 2017/11/22.
  */
class rmBracket extends UDF {
  val leftBrackets = Set('(', '（', '（').map(_.toByte)
  val rightBrackets = Set(')', '）', '）').map(_.toByte)
  val brackets = leftBrackets ++ rightBrackets

  def evaluate(text: Text): Text = {

    // 所有字符构成的数组
    val allByteArray= (0 until text.getLength)
      .map(x => (x, text.getBytes()(x)))
    //筛选出所有括号
    val theBrackets = allByteArray
      .filter(x => brackets.contains(x._2))
    //初始化stack
    val stack    = new mutable.Stack[(Int, Byte)]
    //记录被排除区间
    val lb       = ListBuffer.empty[(Int, Int)]
    //break是为了当遇到异常右括号无法入栈匹配时，break循环，直接进入判定
    val loop     = new Breaks
    loop.breakable{
      //利用栈匹配括号，被排除的区间记录在lb中
    for (bracket <- theBrackets) {
      if (leftBrackets.contains(bracket._2)) {
        stack.push(bracket);
      } else {
        if(stack.isEmpty){
          stack.push(bracket)
          loop.break()
        }else{
        val left = stack.pop()
        val right = bracket
        val closedInterval = (left._1, right._1)
        lb.append(closedInterval)
        }
      }
    }
    }
    //
    val filterSet = lb
      .toList
      .foldLeft((0 until text.getLength).toSet)((set,downUp)=>set.filter(x=>x<downUp._1||x>downUp._2))

    val filterCondition = if(stack.isEmpty){
      filterSet
    }else{
      if(leftBrackets.contains(stack.head._2)){
        filterSet.filter(x=>x<stack.last._1)
      }else{
        filterSet.filter(x=>x>stack.head._1)
      }
    }
    val newText = if(filterCondition.isEmpty)new Text("") else{
      println(new String(allByteArray.filter(x=>filterCondition(x._1)).map(_._2).toArray))
      new Text(allByteArray.filter(x=>filterCondition(x._1)).map(_._2).toArray)
    }
    newText
  }

}
