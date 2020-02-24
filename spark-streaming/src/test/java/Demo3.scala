import java.util

object Demo3 {
    
    
//    import scala.collection.JavaConversions._
    def main(args: Array[String]): Unit = {
        val list = new util.ArrayList[Int]()
        list.add(100)
        list.add(200)
        list.add(300)
//        list += 1000

//        for (ele <- list) {   // list.foreach(......)
//            println(ele)
//        }
       
       for(i  <- 0 until list.size()){
           println(list.get(i))
       }
    }
}
