import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

import java.time.Duration;
import java.util.ArrayList;

/**
 * @Auther: wxf
 * @Date: 2023/11/3 15:56:52
 * @Description: LettuceDemo
 * @Version 1.0.0
 */
public class LettuceDemo {
    public static void main(String[] args) {

        // 构建 RedisRUI
        RedisURI uri = RedisURI.builder()
                .withHost("nn1.hadoop")
                .withPort(6379)
                .withAuthentication("default", "111111")
                .build();

        // 创建客户端
        RedisClient redisClient = RedisClient.create(uri);
        StatefulRedisConnection<String, String> conn = redisClient.connect();

        // 通过 conn 创建 操作的 command
        RedisCommands<String, String> commands = conn.sync();

        // 封装操作
        System.out.println(commands.keys("*"));

        commands.set("k1", "v1");
        commands.expire("k1", Duration.ofSeconds(10L));
        commands.lpush("l1", "1", "2", "3");
        commands.sadd("set1", "s1", "s2");
        commands.zadd("z1", 10D, "z1");
        commands.hset("h1", "name", "h1");

        String[] arr = {"k1", "k2"};
        commands.eval("return redis.call('set' KEY[1] ARGV[1], 'set' KEY[2] ARGV[2])", ScriptOutputType.BOOLEAN, arr, "lua1", "lua2");

        // 关闭资源
        conn.close();
        redisClient.shutdown();
    }
}