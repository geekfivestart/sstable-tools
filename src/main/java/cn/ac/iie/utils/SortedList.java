package cn.ac.iie.utils;


import java.util.*;

/**
 * 排序的list
 *
 * @author Xiang
 * @date 2016-09-22 19:34
 */
public class SortedList<T> extends LinkedList <T> implements RandomAccess{
    /**
     * 排序比较器.
     */
    private Comparator<? super T> comparator = null;


    public SortedList() {
    }


    public SortedList(Comparator<? super T> comparator) {
        this.comparator = comparator;
    }
    /**
     * 向列表添加一个实体. 插入时使用定义好的比较器对数据进行排序。
     *
     * @param paramT 需要添加的实体
     */
    @Override
    public boolean add(T paramT) {
        if(comparator == null){
            return false;
        }
        int insertionPoint = Collections.binarySearch(this, paramT, comparator);
        super.add((insertionPoint > -1) ? insertionPoint : (-insertionPoint) - 1, paramT);
        return true;
    }
    /**
     * 将一个特定的集合中的元素全部添加进列表.
     * 每个元素将使用定义好的比较器寻找其在列表中的正确位置
     *
     * @param paramCollection 需要添加的集合
     */
    @Override
    public boolean addAll(Collection<? extends T> paramCollection) {
        boolean result = false;
        if (paramCollection.size() > 4) {
            result = super.addAll(paramCollection);
            Collections.sort(this, comparator);
        }
        else {
            for (T paramT:paramCollection) {
                result |= add(paramT);
            }
        }
        return result;
    }
    /**
     * 检查列表中是否包含某一特定元素
     *
     * @param paramT 需要检查的元素
     * @return 如果包含该元素则返回true;
     * 否则，返回false。
     */
    public boolean containsElement(T paramT) {
        return (Collections.binarySearch(this, paramT, comparator) > -1);
    }
}
