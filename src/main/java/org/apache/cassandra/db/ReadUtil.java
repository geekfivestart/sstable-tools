package org.apache.cassandra.db;

/**
 * Created by zhangjc on 2018-1-24.
 */
public class ReadUtil {
    public static ReadExecutionController controller(ReadCommand cmd){
        return ReadExecutionController.forCommand(cmd);
    }
}
