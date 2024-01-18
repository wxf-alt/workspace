package factory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.UUID;
import java.util.concurrent.locks.Lock;

/**
 * @Auther: wxf
 * @Date: 2023/11/16 19:19:36
 * @Description: DistributedLockFactory  分布式锁工厂
 * @Version 1.0.0
 */
public class DistributedLockFactory {
    private StringRedisTemplate stringRedisTemplate;
    private String lockName;
    private String uuid;

    public DistributedLockFactory() {
        this.uuid = UUID.fromString(lockName).toString();
    }

    public Lock getDistributedLock(String lockType) {
        if (lockType == null) {
            return null;
        }
        if (lockType.equalsIgnoreCase("REDIS")) {
            this.lockName = "zzyyRedisLock";
            return new RedisDistributedLock(stringRedisTemplate, lockName, uuid);
        } else if (lockType.equalsIgnoreCase("ZOOKEEPER")) {
            this.lockName = "zzyyZookeeperLockNode";
            //TODO zookeeper版本的分布式锁
            return null;
        } else if (lockType.equalsIgnoreCase("MYSQL")) {
            //TODO MYSQL版本的分布式锁
            return null;
        }

        return null;
    }
}