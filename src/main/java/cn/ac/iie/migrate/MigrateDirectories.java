package cn.ac.iie.migrate;

import cn.ac.iie.utils.SortedList;
import com.google.common.collect.Lists;

import java.util.Collection;
import java.util.List;

/**
 * 数据迁移目标目录列表
 *
 * @author Xiang
 * @date 2016-09-22 18:53
 */
public class MigrateDirectories {
    private SortedList<MigrateDirectory> migrateDirectories = new SortedList<>(new MigrateDirectoryComparator<>());
    private final Object syncObj = new Object();

    public MigrateDirectories() {
    }

    private MigrateDirectories(Collection<MigrateDirectory> migrateDirectories){
        if (migrateDirectories != null) {
            this.migrateDirectories.addAll(migrateDirectories);
        }
    }

    /**
     * 给出迁移目标目录列表，获取MigrateDirectories实例
     * @param migratePaths 迁移目标目录列表
     * @return 返回MigrateDirectories实例
     */
    public static MigrateDirectories instance(Collection<String> migratePaths){
        List<MigrateDirectory> migrateDirectoryList = Lists.newArrayList();
        migratePaths.forEach(path -> migrateDirectoryList.add(new MigrateDirectory(path)));
        return new MigrateDirectories(migrateDirectoryList);
    }

    /**
     * 从迁移目标目录列表中获取一个剩余空间最大的目录<br/>
     * 若目前列表为空，则等待1秒后重试直到达到重试次数或取得一个正确的目标目录
     * @param attempt 尝试次数
     * @param maxAttempt 最大尝试次数，最大尝试次数过大可能<br/>
     *                   抛出StackOverFlowException异常，<br/>
     *                   因此此处推荐最大尝试次数不要超过10次<br/>
     * @return 返回目前列表中剩余空间最大的目录
     */
    public MigrateDirectory poll(int attempt, int maxAttempt) throws InterruptedException {
        MigrateDirectory e;
        // 由于各个任务是并行执行，因此对migrateDirectories的访问必须保证是线程安全的，
        // 因此此处使用synchronized确保目标目录的取出和放回是同步的
        // 调用pollFirst方法是因为列表中的目录是按剩余空间大小从大到小排序的，
        // 因此取出第一个即为剩余空间最大的目标目录
        synchronized (syncObj) {
            e = migrateDirectories.pollFirst();
        }
        // 此处利用递归来达到重试的目的
        if(e == null && attempt < maxAttempt){
            Thread.sleep(1000);
            return poll(attempt+1, maxAttempt);
        }
        return e;
    }

    /**
     * 向列表中放入一个目录<br/>
     * 该方法应该在初始化或目录放回时调用<br/>
     * 该方法与取出目录是同步的
     * @param e 需要放入的目录
     * @return 返回放入结果，若成功放入则放回true，否则返回false
     */
    public synchronized boolean add(MigrateDirectory e){
        // 由于各个任务是并行执行，因此对migrateDirectories的访问必须保证是线程安全的，
        // 因此此处使用synchronized确保目标目录的取出和放回是同步的
        synchronized (syncObj) {
            return migrateDirectories.add(e);
        }
    }

    /**
     * 添加特定字符串集合中的所有元素，
     * 字符串首先需要转化为{@link MigrateDirectory}对象
     * @param migratePaths 字符串路径集合
     * @return 返回是否添加成功，只要有一条添加成功则返回true；
     * 否则，返回false
     */
    public boolean addAllString(Collection<String> migratePaths){
        boolean result = false;
        synchronized (syncObj) {
            for (String migratePath : migratePaths) {
                MigrateDirectory directory = new MigrateDirectory(migratePath);
                if (directory.exists())
                    result |= migrateDirectories.add(directory);
            }
        }
        return result;
    }

    /**
     * 获取目前列表中的目录个数
     * @return 返回目录个数
     */
    public int dirCount(){
        int count;
        synchronized (syncObj) {
            count = migrateDirectories.size();
        }
        return count;
    }

}
