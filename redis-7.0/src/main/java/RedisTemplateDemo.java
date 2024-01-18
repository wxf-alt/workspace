import org.springframework.data.geo.*;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * @Auther: wxf
 * @Date: 2023/11/9 17:12:45
 * @Description: RedisTemplateDemo
 * @Version 1.0.0
 */
public class RedisTemplateDemo {

    public static void main(String[] args) {

        RedisTemplate template = new RedisTemplate();
        // TODO 编译报错
//        RedisStandaloneConfiguration standaloneConfig = new RedisStandaloneConfiguration("nn1.hadoop", 6379);
//        JedisConnectionFactory fac = new JedisConnectionFactory(standaloneConfig);
//        template.setConnectionFactory(fac);
//        template.afterPropertiesSet();

        template.opsForValue().set("hello", "world");
        Object hello = template.opsForValue().get("hello");
        System.out.println(hello);

        try {
            TimeUnit.SECONDS.sleep(1L); // 休眠
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // 获取两个坐标点 之间 距离
        template.opsForGeo().distance("", "", "", RedisGeoCommands.DistanceUnit.KILOMETERS);

        // 返回 10.0，10.0 半径内 10KM 距离内的10条数据
//        Circle within = new Circle(10.0, 10.0, Metrics.KILOMETERS.getMultiplier());
        Circle within = new Circle(new Point(10.0, 10.0), new Distance(10, RedisGeoCommands.DistanceUnit.KILOMETERS));
        RedisGeoCommands.GeoRadiusCommandArgs arg = RedisGeoCommands.GeoRadiusCommandArgs.newGeoRadiusArgs().includeDistance().includeCoordinates().sortAscending().limit(10);
        GeoResults geoResults = template.opsForGeo().radius("", within, arg);

        Distance distance = new Distance(6378.137, Metrics.NEUTRAL);

        String luaScript = "redis.call('set' 'k1', 'v1') return redis.call('get' 'k1')";
        template.execute(new DefaultRedisScript(luaScript, Long.class), Arrays.asList("key"), "value");

    }
}