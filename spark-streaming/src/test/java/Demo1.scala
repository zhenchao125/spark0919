import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * Author atguigu
  * Date 2020/2/24 8:34
  */
object Demo1 {
    def main(args: Array[String]): Unit = {
//        val set = Set(10, 20)
//        val set1: Set[Int] = set + 100
//        println(set)
//        println(set1)
        
//        val arr = Array(10, 20)
        
//        val arr1: Array[Int] = arr :+ 100
//        arr +: 100  // 100.+:(arr)
//        val arr1: Array[Int] = 100 +: arr  // arr.+:(100)


//        println(arr.mkString(", "))
//        println(arr1.mkString(", "))
        
        
//        val set1 = Set(1,2,3)
//        val set2 = Set(10,20,30)
//        val set3: Set[Int] = set1 ++ set2
//        val set3: Set[Int] = set1 ++: set2  // set2.++:(set1)
//        println(set1)
//        println(set2)
//        println(set3)
        
//        val arr1 = ListBuffer(30, 50, 70, 60, 10, 20)
//        val arr2 = ListBuffer(3, 5, 7, 6, 1, 2)
//        val arr3: Array[Int] = arr1 ++ arr2
//        val arr3: Array[Int] = arr1 ++: arr2 //
//        val arr3 = arr1 ++: arr2 //
//        println(arr1.mkString(", "))
//        println(arr2.mkString(", "))
//        println(arr3.mkString(", "))
        
        
//        var arr1 = Array(30, 50, 70, 60, 10, 20)
//        arr1 :+= 100

//        println(arr1)
        
//        var arr1: ArrayBuffer[Any] = ArrayBuffer(10, 20)
//        val arr2: ArrayBuffer[Any] = ArrayBuffer(100, 200)
        
//        arr1 :+= 100
//        100 +=: arr1
//        arr1 ++= arr2
//        println(arr1)
        
        val list1 = List(30, 50, 70, 60, 10, 20)
        val list2 = List(3, 5, 7, 6, 1, 2)
//        val list2: List[Int] = list1 :+ 100
//        val list2: List[Int] = 100::list1
        
        val list3: List[Int] = list1 ++ list2  // list1.++(list2)
        val list4: List[Int] = list1 ::: list2  // list2.:::(list1)
        println(list1)
        println(list2)
        println(list3)
        println(list4)
    
        println(list1.foldLeft(0)(_ + _))
    
        println((0 /: list1) (_ + _))
        println((list1 :\ 0) (_ + _))  // 右折叠
        
    }
}
/*

scala: 运算符中, 前置 +1 -1 中置 1 + 2 后置  5!
运算符的结合性:
    左结合
        1 + 2
    
    
    右结合
        a = 20
        运算符以 : 结尾, 都是右结合的
        
   
+
:+
+:
    向集合中添加元素
    有冒号一般用于那些有索引的集合
    
    

++
++:
:++
    合并集合中的元素


+=
:+=
++=
    相比签名多了一个赋值的作用
    1. 如果是可变的集合, 则向老的集合中添加元素, 会修改原来的集合
    2. 如果不可变集合,则生成一个变化的新集合之后, 赋值给原来的变量

List专用:
    :: 添加元素
    ::: 合并List

:\  右折叠
/:  左折叠  foldLeft


 */
