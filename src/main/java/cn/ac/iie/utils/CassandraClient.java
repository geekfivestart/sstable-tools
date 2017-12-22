package cn.ac.iie.utils;

import com.datastax.driver.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

/**
 * Created by zhangjc on 2017/12/14.
 */
public class CassandraClient {
    private static Logger LOG = LoggerFactory.getLogger(CassandraClient.class);
    private static String connectIP = null;
    private static int port;
    private static String userName = null;
    private static String password = null;
    private static Cluster cluster;
    private static Session session;

    public CassandraClient(String connectIP, int port, String userName, String password) {
        CassandraClient.connectIP = connectIP;
        CassandraClient.port = port;
        CassandraClient.userName = userName;
        CassandraClient.password = password;
    }

    public void ConnectCassandra() {
        QueryOptions queryoptions = new QueryOptions().setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);
        cluster = Cluster.builder().addContactPoint(connectIP).withPort(port).withQueryOptions(queryoptions)
                .withCredentials(userName, password).build();
        session = cluster.connect();
        LOG.info("Connect the Cassandra is OK!");
    }

    public Iterator<Row> queryResult(String query) {
        ResultSet res = session.execute(query);
        Iterator<Row> rows = res.iterator();

        return rows;
    }

    public void execute(String sql){
        session.execute(sql);
    }

    public void close(){
        session.close();
        cluster.close();
    }
}
