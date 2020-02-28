import org.json4s.JValue
import org.json4s.jackson.{JsonMethods, Serialization}

/**
  * Author atguigu
  * Date 2020/2/28 13:50
  */
case class User(age: Int, name: String)
object Json4VDemo {
    def main(args: Array[String]): Unit = {
        import org.json4s.JsonDSL._
        import org.json4s.DefaultFormats
        val list = List(("a", 96), ("b", 97), ("c", 98))
        /*val j1: JValue = JsonMethods.render(list)
        println(JsonMethods.compact(j1))*/
        
//        val user = User(10, "lisi")
//        val r: String = Serialization.write(user)(DefaultFormats)
//        val r = Serialization.write(list)(DefaultFormats)
        implicit  val d = org.json4s.DefaultFormats
        val r = Serialization.write(list)
        val r1 = Serialization.write(list)
        println(r)
    }
}
/*
把scala中的数据结构, 转换成json字符串, 使用json4s

1.scala的集合 ->json字符串
    List, Map, Set ->json
    val j1: JValue = JsonMethods.render(list)
        println(JsonMethods.compact(j1))
        
    scala的集合很有限, 可以提前写好隐式转换把这些集合的父类转成jvalue

2. 样例类->json字符串
    Serialization.write(user)(DefaultFormats)
    只要是scala的数据结构, 都可以使用这种方式做json的序列化



把java中的数据结构, 转换成json字符串, 使用fastjson(ali)/gson(google)/jackson
    在scala中解析(从字符串->对象)问题不大, 如果序列化把对象->字符串容易出问题

 */