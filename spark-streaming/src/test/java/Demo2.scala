import org.json4s.jackson.JsonMethods

/**
  * Author atguigu
  * Date 2020/2/24 16:32
  */
object Demo2 {
    def main(args: Array[String]): Unit = {
        import org.json4s.JsonDSL._
        val list1 = List(("aa", "a"), ("aa", "b"))   // []
        val s: String = JsonMethods.compact(JsonMethods.render(list1))
        println(s)
        
    }
}
