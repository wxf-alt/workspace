import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.junit.Test;

/**
 * @Auther: wxf
 * @Date: 2022/12/2 16:07:44
 * @Description: TestFastJson
 * @Version 1.0.0
 */
public class TestFastJson {

    @Test // 创建 JsonObject
    public void testFastJsonCreateObject() {
        // 创建 JsonObject 对象
        JSONObject jsonObject = new JSONObject();
        // 添加属性
        jsonObject.put("name", "jack");
        jsonObject.put("age", 18);
        jsonObject.put("gender", "male");

        // {"gender":"male","name":"jack","age":18}
        System.out.println(jsonObject.toString());
        // jack
        System.out.println(jsonObject.get("name"));
    }

    @Test // Json字符串转换为Object
    public void testFastJsonStringToObject() {
        String str = "{\"gender\":\"male\",\"name\":\"jack\",\"age\":18}";
        Person person = JSON.parseObject(str, Person.class);
        System.out.println(person);
    }


    @Test // 测试 JsonArray
    public void testFastJsonArray() {
        // 创建 JsonObject 对象
        JSONObject jsonObject = new JSONObject();
        // 添加属性
        jsonObject.put("name", "jack");
        jsonObject.put("age", 18);
        jsonObject.put("gender", "male");

        JSONObject jsonObject2 = new JSONObject();
        // 添加属性
        jsonObject2.put("name", "jack");
        jsonObject2.put("age", 19);
        jsonObject2.put("gender", "male");

        JSONArray jsonArray = new JSONArray();
        jsonArray.add(jsonObject);
        jsonArray.add(jsonObject2);

        System.out.println(jsonArray);


    }

}