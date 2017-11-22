import com.finup.nbsp.adp.HiveUDF.UDF.rmBracket
import org.apache.hadoop.io.Text

/**
  * Created by john_liu on 2017/11/22.
  */
object TestrmBracket {
  def main(args: Array[String]): Unit = {
  val test = List("(dfdf)(df)d",
      "1(23(4)6)",
      "11(111)2222(22)23",
      "1234567",
      "dfdfde(r)g",
      "d(favfwdfwgq(3r(gqw(fqwqwvqw","sdgfs)dbs"
    )
   val udf =  new rmBracket()
    test.map(x=>udf.evaluate(new Text(x)))
  }
}
