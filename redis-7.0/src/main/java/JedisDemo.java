import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * @Auther: wxf
 * @Date: 2023/11/3 15:49:59
 * @Description: JedisDemo
 * @Version 1.0.0
 */
public class JedisDemo {
    public static void main(String[] args) {

        // jdk1.7 新特性 语法糖 -> 会自动关闭 try() 中创建的对象。 对象必须实现 Closeable 或者 AuToCloseable
        try (Jedis jedis = new Jedis("nn1.hadoop", 6379)) {
            // 密码
            jedis.auth("111111");

            // lua 脚本操作
            jedis.eval("redis.call('set' 'k1', 'v1') return redis.call('get' 'k1')");
            jedis.eval("return redis.call('set' KEY[1] ARGV[1], 'set' KEY[2] ARGV[2])", 2, "k1", "k2", "lua1", "lua2");

            // 操作
            System.out.println(jedis.keys("*"));

            jedis.set("k1", "v1");
            jedis.lpush("l1", "1", "2", "3");
            jedis.sadd("set1", "s1", "s2");
            jedis.zadd("z1", 10D, "z1");
            jedis.hset("h1", "name", "h1");
        } catch (Exception e) {
            e.printStackTrace();
        }

//        // 创建客户端
//        Jedis jedis = new Jedis("nn1.hadoop", 6379);
//
//        // 密码
//        jedis.auth("111111");
//
//        // 操作
//        System.out.println(jedis.keys("*"));
//
//        jedis.set("k1", "v1");
//        jedis.lpush("l1", "1", "2", "3");
//        jedis.sadd("set1", "s1", "s2");
//        jedis.zadd("z1", 10D, "z1");
//        jedis.hset("h1", "name", "h1");
//
//        jedis.close();

    }

}