package factory;

import org.redisson.Redisson;
import org.redisson.RedissonMultiLock;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

/**
 * @Auther: wxf
 * @Date: 2023/11/20 19:30:03
 * @Description: RedisSonTest  分布式锁-案例
 * @Version 1.0.0
 */
public class RedisSonTest {
    public static void main(String[] args) {

        // 单例-分布式锁-案例
        SingleRedisSonFunction();

        //多分布式锁-案例
        ClusterRedisSonFunction();

    }

    public static void ClusterRedisSonFunction() {
        Config config1 = new Config();
        config1.useSingleServer().setAddress("redis://nn1.hadoop:6379").setDatabase(0).setPassword("111111");
        RedissonClient redissonClient1 = Redisson.create(config1);

        Config config2 = new Config();
        config2.useSingleServer().setAddress("redis://nn1.hadoop:6379").setDatabase(0).setPassword("111111");
        RedissonClient redissonClient2 = Redisson.create(config2);

        Config config3 = new Config();
        config3.useSingleServer().setAddress("redis://nn1.hadoop:6379").setDatabase(0).setPassword("111111");
        RedissonClient redissonClient3 = Redisson.create(config3);

        RLock lock1 = redissonClient1.getLock("lock");
        RLock lock2 = redissonClient2.getLock("lock");
        RLock lock3 = redissonClient3.getLock("lock");

        // 创建 多重锁
        RedissonMultiLock multiLock = new RedissonMultiLock(lock1, lock2, lock3);

        multiLock.lock();
        try {
            System.out.println("进行业务处理");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (multiLock.isLocked() && multiLock.isHeldByCurrentThread()) {
                multiLock.unlock();
            }
        }
    }

    public static void SingleRedisSonFunction() {
        Config config = new Config();
        // 设置 单例机器
        config.useSingleServer().setAddress("redis://nn1.hadoop:6379").setDatabase(0).setPassword("111111");
        // 创建 RedisSon 分布式锁
        RedissonClient redissonClient = Redisson.create(config);

        // 创建锁
        RLock rLock = redissonClient.getLock("myLock");
        rLock.lock();
        // 业务处理
        try {
            System.out.println("进行业务处理");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // 如果当前锁正在被持有，并且是当前线程的锁  才可以删除
            if (rLock.isLocked() && rLock.isHeldByCurrentThread()) {
                rLock.unlock();
            }
        }
    }


}