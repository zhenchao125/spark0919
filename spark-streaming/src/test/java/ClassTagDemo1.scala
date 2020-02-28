import scala.reflect.ClassTag

/**
  * Author atguigu
  * Date 2020/2/28 15:58
  */
object ClassTagDemo1 {
    def main(args: Array[String]): Unit = {
        //        val arr: Array[Int] = newArr[Int](10)
        //        println(arr.mkString(", "))
        
        //        val arr =  newArr[User](10)
        //        println(arr)
        
        /*println(max(10, 3, 5))
        val arr = List(10, 20, 5)
    
        println(max(arr: _*))*/
        
        
        val arr = Array(10, 20, 30)
        /* arr.foreach(x => println(x) */
         arr.foreach(println)  //传递现有的函数
         arr.foreach(println(_))  //
        
        
        
        // 部分应用函数
        // 在已有的函数的基础上,得到一个新的函数, 这个函数只算平方  (理论基础是闭包)
        /*val s: Double => Double = math.pow(_, 2)
        println(s(10))
        println(s(20))*/
        
        /*val p: Any => Unit = println(_)
        p(10)*/
        println(a_++)
        
        
    }
    def ++ = 10
    // scala不支持把运算符和字符混合起来命名
//    def a++ = 20
    //
    def a_++ = 30
    
    
    def max(args: Int*) = args.max
    
    // 这段代码是否可以正常运行?
    // new int[10]
    //    def newArr[T](len: Int) = new Array[T](len)
    
    // 泛型的上下文(隐式值和隐式参数的简化):  要存在一个隐式值 , 类型必须是 ClassTag[T]
    //    def newArr[T: ClassTag](len: Int) = new Array[T](len)
    
    def newArr[T](len: Int)(implicit ct: ClassTag[T]) = new Array[T](len)
    
    //    def newList[T]() = new util.ArrayList[T]()
}

/*
1. ClassTag
    他是用来保存泛型到运行时
    泛型默认只存在于编译时, 编译完之后, class中是没有泛型(泛型擦除)
2. _  9种用法
    1. 导包的通配符
    2. 元组的前缀
            c._1 c._2
            
    3. 作为隐式参数
        map((_, 1))
    4. 给属性设置默认值
        var a: Int = 0
        var a: Int = _
    5. 模式匹配的时候用于通配符
        case _ => 不会用到匹配到的值
        case a =>  可以使用匹配到那个值
    6. 方法转换成函数
        foo(f _)
    7. 处理异常的时候, 也可以用通配符
    8. 用于展开集合(分解)
           println(max(arr: _*))
    9. 用于部分应用函数
            math.pow(_, 3)
            
    10. 用于运算符和字符的混合定义
        a_:
        a_++
    
    
    
 */