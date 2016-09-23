package cn.ac.iie.cassandra;

/**
 * 不存在该keyspace异常
 *
 * @author Xiang
 * @date 2016-09-20 18:54
 */
public class NoSuchKeyspaceException extends Exception {

    public NoSuchKeyspaceException(String message){
        super(message);
    }

    public NoSuchKeyspaceException(String message, Throwable cause){
        super(message, cause);
    }
}
