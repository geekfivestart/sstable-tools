package cn.ac.iie.cassandra;

/**
 * 不存在该table异常
 *
 * @author Xiang
 * @date 2016-09-20 18:54
 */
public class NoSuchTableException extends Exception {

    public NoSuchTableException(String message){
        super(message);
    }

    public NoSuchTableException(String message, Throwable cause){
        super(message, cause);
    }
}
