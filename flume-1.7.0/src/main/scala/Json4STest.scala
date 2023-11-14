import org.json4s.jackson.{JsonMethods, Serialization}


/**
 * @Auther: wxf
 * @Date: 2023/11/14 16:22:10
 * @Description: Json4STest
 * @Version 1.0.0
 */
object Json4STest {
  def main(args: Array[String]): Unit = {

    //数据格式：user:{"name": "zs","age": 10}
    val json: String = "{\"name\": \"zs\",\"age\": 10}"

    //导隐式函数
    //    implicit val f: DefaultFormats.type = org.json4s.DefaultFormats

    /** ************************* json字符串->对象 ***************************************/
    val mf: Manifest[User] = new Manifest[User] {
      override def runtimeClass: Class[User] = classOf[User]
    }
    val user: User = JsonMethods.parse(json).extract[User](org.json4s.DefaultFormats, mf)
    println(user)

    /** ************************* 对象->json字符串 ***************************************/
    val userJStr: String = Serialization.write(user)(org.json4s.DefaultFormats)
    println(userJStr)

  }
}

case class User(name: String, age: Int)
