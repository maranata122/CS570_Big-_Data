import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.storage.StorageLevel
object NetworkWordCount {
   def main(args: Array[String]) {
      val updateFunc = (values: Seq[Int], state: Option[Int]) => {
         val currentCount = values.foldLeft(0)(_ + _)
         val previousCount = state.getOrElse(0)
         Some(currentCount + previousCount)
      }
      val ssc = new StreamingContext("local[2]", "NetworkWordCount", Seconds(1))
      ssc.checkpoint(".")
      val lines = ssc.socketTextStream("127.0.0.1", 9999)
      val words = lines.flatMap(_.split(" "))
      val pairs = words.map(word => (word, 1))
      val stateWordCounts = pairs.updateStateByKey[Int](updateFunc)
      stateWordCounts.print()
      ssc.start()
      ssc.awaitTermination()
   }
}
