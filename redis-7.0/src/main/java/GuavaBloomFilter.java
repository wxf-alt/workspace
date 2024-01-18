import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

/**
 * @Auther: wxf
 * @Date: 2023/11/15 09:33:27
 * @Description: GuavaBloomFilter
 * @Version 1.0.0
 */
public class GuavaBloomFilter {

    public static void main(String[] args) {

        BloomFilter<Integer> bloomFilter = BloomFilter.create(Funnels.integerFunnel(), 1, 0.03);

        // 是否存在
        System.out.println(bloomFilter.mightContain(10));
        // 添加元素
        bloomFilter.put(10);
        // 是否存在
        System.out.println(bloomFilter.mightContain(10));

    }

}