/**
  * Author atguigu
  * Date 2020/2/22 15:47
  */
object EitherTest {
    def main(args: Array[String]): Unit = {
        // Option 语义: 值可能有也可能没有  避免空针   Some None
        
        // Either 语义: 可能有两种值, 表示错误或者正确 Left Right
        val e1: Either[String, Int] = Right(10)
        val e2: Either[String, Int] = Left("ab")
        
        e2 match {
            case Right(a) =>
                println(a)
            case Left(a) =>
                println("error: " + a)
        }
    }
}
