/**
 * @Auther: wxf
 * @Date: 2023/12/4 10:34:44
 * @Description: User
 * @Version 1.0.0
 */
public class User {
    private String name;
    private Integer age;
    private String sex;

    public User(String name, Integer age, String sex) {
        this.name = name;
        this.age = age;
        this.sex = sex;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    @Override
    public String toString() {
        String message = String.format("[USER：【name=%s】,【age=%d】，【sex=%s】]", name, age, sex);
        return message;
    }
}