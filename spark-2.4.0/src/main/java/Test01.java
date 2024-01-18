/**
 * @Auther: wxf
 * @Date: 2023/12/4 10:34:11
 * @Description: Test01
 * @Version 1.0.0
 */
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class Test01 {
    public static void main(String[] args) {

        // 创建集合对象
        List<User> list = new ArrayList<>();
        // 对象赋值
        User user1 = new User("张三",25,"男");
        User user2 = new User("小李",38,"男");
        User user3 = new User("小红",40,"女");
        // 添加到集合中
        list.add(user1);
        list.add(user2);
        list.add(user3);

        System.out.println("集合的所有信息： " + list);
        System.out.println("");


        // 单一条件过滤：根据对象中的一个条件过滤（过滤名字不为张三的所有人）
        List<User> collect = list.stream()
                .filter(user -> !"张三".equals(user.getName()))
                .collect(Collectors.toList());

        System.out.println("单一条件过滤后：" + collect);
        System.out.println("");


        // 多条件过滤：根据对象中的多个属性过滤（过滤，只剩年龄在40岁以上，并且性别为女的用户）
        List<User> collect2 = list.stream()
                .filter(user -> user.getAge() >= 40 && "女".equals(user.getSex()))
                .collect(Collectors.toList());

        System.out.println("多条件过滤后：" + collect2);
    }

}