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

    public MigrateDirectories(Collection<MigrateDirectory> migrateDirectories){
        if (migrateDirectories != null) {
            this.migrateDirectories.addAll(migrateDirectories);
        }
    }

    public static MigrateDirectories instance(Collection<String> migratePaths){
        List<MigrateDirectory> migrateDirectoryList = Lists.newArrayList();
        migratePaths.forEach(path -> migrateDirectoryList.add(new MigrateDirectory(path)));
        return new MigrateDirectories(migrateDirectoryList);
    }

    public MigrateDirectory poll(int attempt, int maxAttempt) throws InterruptedException {
        MigrateDirectory e;
        synchronized (syncObj) {
            e = migrateDirectories.pollFirst();
        }
        if(e == null && attempt < maxAttempt){
            Thread.sleep(1000);
            return poll(attempt+1, maxAttempt);
        }
        return e;
    }

    public synchronized boolean add(MigrateDirectory e){
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

}
