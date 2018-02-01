package cn.ac.iie.utils;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;


/**
 * Util class for {@link Token}
 *
 * @author Xiang
 * @date 2016-12-13 13:12
 */
public class TokenUtil {
    private static final Logger LOG = LoggerFactory.getLogger(TokenUtil.class);
    /**
     * token range and endpoints map each keyspace owns
     */
    private static final Map<String, ConcurrentHashMap<Range<Token>, List<InetAddress>>> KS_TOKEN_ENDPOINTS = new ConcurrentHashMap<>();
    /**
     * data centers each keyspace owns
     */
    private static final Map<String, List<String>> KS_DC = new ConcurrentHashMap<>();

    private static final Map<String, Set<String>> KS_DC_SET = new ConcurrentHashMap<>();
    /**
     * endpoints each dc owns
     */
    private static final Map<String, Set<InetAddress>> DC_ENDPOINTS = new ConcurrentHashMap<>();
    /**
     * token range and endpoints map each dc owns
     */
    private static final Map<String, ConcurrentHashMap<Range<Token>, List<InetAddress>>> DC_TOKEN_ENDPOINTS = new ConcurrentHashMap<>();
    /**
     * token to endpoint map sorted by token
     */
    private static final Map<Token, InetAddress> SORTED_TOKEN_TO_ENDPOINT = new LinkedHashMap<>();
    /**
     * token ranges each endpoint owns
     */
    private static final Map<String, Map<InetAddress, List<Range<Token>>>> DC_ENDPOINT_PRIMARY_RANGES = new ConcurrentHashMap<>();
    private static final Object OBJ = new Object();

    /**
     * map for <ks,token range list> for local C* point,
     */
    public static final Map<String,List<Range<Token>>> LOCAL_TK_RANGE_KS=new ConcurrentHashMap<>();

    /**
     * Get token range of specific token in the token ring of the cassandra cluster.
     * We cache the token range for each endpoint of local dc which the specific keyspace is contained.
     * @param token the token vale.
     * @return the token range.
     */
    public static Range<Token> rangeOf(Token token, Keyspace keyspace){
        mayInit(keyspace);
        Map<Range<Token>, List<InetAddress>> map = KS_TOKEN_ENDPOINTS.get(keyspace.getName());
        Range<Token> range = map.entrySet()
                .parallelStream().filter(e -> e.getKey().contains(token))
                .map(Map.Entry::getKey).findFirst().orElse(null);
        LOG.debug("got token range: {}", range);
        return range;

    }

    /**
     * Get token range of specific token, keyspace and endpoint in the token range of the cassandra cluster.
     * We cache the primary token range of each endpoint of specific dc.Thus we can filter the dc which contains
     * the endpoint then map the token range which contains the token.
     * @param token the token value
     * @param keyspace the specific keyspace
     * @param endpoint the specific endpoint address
     * @return the token range
     */
    public static Range<Token> range4SpecEndpoint(Token token, Keyspace keyspace, InetAddress endpoint) {
        mayInit(keyspace);
        Range<Token> range=null;
        try {
            range = DC_ENDPOINT_PRIMARY_RANGES.entrySet()
                    .parallelStream()
                    .filter(e -> e.getValue().containsKey(endpoint))
                    .map(e -> e.getValue().get(endpoint).parallelStream()
                            .filter(r -> r.contains(token))
                            .findFirst().orElse(null))
                    .findFirst().orElse(null);
        }catch(Exception ex){
            LOG.warn("Failed to find Range for {} on endpoint {}, usgin min/max range {}/{}",
                    token.getTokenValue(),endpoint.getHostAddress(),Long.MIN_VALUE,Long.MAX_VALUE);
            range=new Range<Token>(new Murmur3Partitioner.LongToken(Long.MIN_VALUE),
                    new Murmur3Partitioner.LongToken(Long.MAX_VALUE));
        }
        return range;
    }

    /**
     * Initialize token range and endpoint data for keyspace.
     * If the data is initialized for the keyspace, skip it.
     * @param keyspace the keyspace.
     */
    public static void mayInit(Keyspace keyspace) {
        if(!KS_TOKEN_ENDPOINTS.containsKey(keyspace.getName())) {
            synchronized (OBJ) {
                if(!KS_TOKEN_ENDPOINTS.containsKey(keyspace.getName())) {
                    while (!StorageService.instance.isInitialized()) {
                        waitSomeSeconds(1);
                    }
                    getTokenToEndpointMap();
                    // local dc
                    String dc = DatabaseDescriptor.getEndpointSnitch().getDatacenter(FBUtilities.getBroadcastAddress());
                    String primaryDc = primaryDc(keyspace);
                    if(primaryDc == null) {
                        Map<Range<Token>, List<InetAddress>> map = StorageService.instance
                                .getRangeToAddressMap(keyspace.getName());
                        DC_ENDPOINTS.put(dc, new HashSet<>(SORTED_TOKEN_TO_ENDPOINT.values()));
                        KS_TOKEN_ENDPOINTS.put(keyspace.getName(), new ConcurrentHashMap<>(map));
                    } else {
                        if(!KS_DC.get(keyspace.getName()).contains(dc)) {
                            dc = primaryDc;
                        }
                        KS_DC.get(keyspace.getName()).forEach(e-> DC_ENDPOINTS.put(e, addressesInDc(keyspace, e)));
                        allKsDcRangeAddr(keyspace);
                        Map<Range<Token>, List<InetAddress>> map = new HashMap<>();
                        map.putAll(DC_TOKEN_ENDPOINTS.get(keyspace.getName()+"_"+dc));
                        if(map.isEmpty()) {
                            map.putAll(StorageService.instance.getRangeToAddressMap(keyspace.getName()));
                        }
                        KS_TOKEN_ENDPOINTS.put(keyspace.getName(), new ConcurrentHashMap<>(map));
                    }
                    LOG.info("Initialed range endpoints for ks[{}]: {}",
                            keyspace.getName(),
                            KS_TOKEN_ENDPOINTS.get(keyspace.getName()));
                    List<Range<Token>> list=new ArrayList<>();
                    InetAddress local=FBUtilities.getBroadcastAddress();
                    KS_TOKEN_ENDPOINTS.get(keyspace.getName()).entrySet().forEach(en->{
                        for(InetAddress inet:en.getValue()){
                            if(inet.equals(local)){
                                list.add(en.getKey());
                                break;
                            }
                        }
                    });
                    LOCAL_TK_RANGE_KS.put(keyspace.getName(),list);
                    LOG.info("local tk range for ks [{}]",keyspace.getName(),list);
                }
            }
        }
    }

    /**
     * Get the live endpoints for specific keyspace and token ranges they owns.
     * @param keyspace the keyspace
     * @return return the endpoints and token ranges map
     */
    public static Map<InetAddress, List<Range<Token>>> getKsLiveEndpointToken(Keyspace keyspace) {
        mayInit(keyspace);
        String keyspaceName = keyspace.getName();
        // search local data center first, if there are no replication of the keyspace in local data center,
        // we search the primary data center.
        String dc = DatabaseDescriptor.getEndpointSnitch().getDatacenter(FBUtilities.getBroadcastAddress());
        if(!KS_DC.get(keyspaceName).contains(dc)) {
            dc = primaryDc(keyspace);
        }
        return getKsLiveEndpointTokenForDc(keyspaceName, dc);
    }

    public static Map<InetAddress, List<Range<Token>>> getKsLiveEndpointTokenForAllDc(Keyspace keyspace) {
        mayInit(keyspace);
        final String ksName = keyspace.getName();
        List<String> dcList = KS_DC.get(ksName);
        if(dcList == null) {
            return new HashMap<>();
        }
        final Map<InetAddress, List<Range<Token>>> map = new HashMap<>();
        dcList.forEach(dc->map.putAll(getKsLiveEndpointTokenForDc(ksName, dc)));
        return Collections.unmodifiableMap(map);
    }

    private static Map<InetAddress, List<Range<Token>>> getKsLiveEndpointTokenForDc(String keyspaceName, String dc) {
        ConcurrentHashMap<Range<Token>, List<InetAddress>> rangeEndpoints = DC_TOKEN_ENDPOINTS.get(keyspaceName+"_"+dc);
        Set<InetAddress> addrs = DC_ENDPOINTS.get(dc);
        // filter the node in the data center from the cached endpoints
        Map<InetAddress, List<Range<Token>>> map = DC_ENDPOINT_PRIMARY_RANGES.get(dc).entrySet().stream()
                .filter(e->addrs.contains(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        Map<InetAddress, List<Range<Token>>> live = new HashMap<>();
        Map<InetAddress, List<Range<Token>>> dead = new HashMap<>();
        // Detect node status(live or dead) of each node.
        // We must do this, if there was some nodes in dead status,
        // we should search other replication for full correct result.
        map.forEach((addr,ranges)->{
            if(FailureDetector.instance.isAlive(addr)) {
                // For live endpoints, the token ranges is primary, thus we don't need add them to range list.
                // At SE, we search all primary fist, then search the additional token ranges.
                live.put(addr, new ArrayList<>());
            } else {
                dead.put(addr, ranges);
            }
        });
        //for dead endpoint, we get the token ranges it owns, then find other live endpoints where these token ranges
        // located.
        dead.forEach((dn,ranges)->
                ranges.forEach(r->{
                    for (InetAddress endpoint : rangeEndpoints.get(r)) {
                        if(FailureDetector.instance.isAlive(endpoint)) {
                            if(!live.containsKey(endpoint)) {
                                live.put(endpoint, new ArrayList<>());
                            }
                            live.get(endpoint).add(r);
                            break;
                        }
                    }
                }));
        return Collections.unmodifiableMap(live);
    }

    /**
     * Get the primary dc in specific keyspace which is the first element of the sorted DC list or
     * which has maximum replication factor.
     * @param keyspace the specific keyspace
     * @return return the primary dc name if exists, otherwise return null.
     */
    private static String primaryDc(Keyspace keyspace) {
        AbstractReplicationStrategy strategy = keyspace.getReplicationStrategy();
        if(strategy instanceof NetworkTopologyStrategy) {
            NetworkTopologyStrategy ntStrategy = (NetworkTopologyStrategy) strategy;
            List<String> dcList = new ArrayList<>(ntStrategy.getDatacenters());
            dcList.sort(String.CASE_INSENSITIVE_ORDER);
            int maxFactor = 0;
            String finalDc = null;
            for (String dc : dcList) {
                if(ntStrategy.getReplicationFactor(dc) > maxFactor) {
                    maxFactor = ntStrategy.getReplicationFactor(dc);
                    finalDc = dc;
                }
            }
            KS_DC.put(keyspace.getName(), dcList);
            return finalDc;
        } else {
            List<String> dcList = new ArrayList<>();
            dcList.add(DatabaseDescriptor.getEndpointSnitch().getDatacenter(FBUtilities.getBroadcastAddress()));
            KS_DC.put(keyspace.getName(), dcList);
            return null;
        }
    }

    public static boolean ksHasDataInlocal(Keyspace keyspace) {
        String localDc = DatabaseDescriptor.getEndpointSnitch().getDatacenter(FBUtilities.getBroadcastAddress());
        if(KS_DC_SET.containsKey(keyspace.getName())) {
            return KS_DC_SET.get(keyspace.getName()).contains(localDc);
        }
        AbstractReplicationStrategy strategy = keyspace.getReplicationStrategy();
        if(strategy instanceof NetworkTopologyStrategy) {
            NetworkTopologyStrategy ntStrategy = (NetworkTopologyStrategy) strategy;
            Set<String> dcSet = ntStrategy.getDatacenters();
            KS_DC_SET.put(keyspace.getName(), dcSet);
            return dcSet.contains(localDc);
        } else {
            KS_DC_SET.put(keyspace.getName(), new HashSet<String>(){{
                add(localDc);
            }});
            return true;
        }
    }

    private static Set<InetAddress> addressesInDc(Keyspace keyspace, String dc) {
        if(DC_ENDPOINTS.containsKey(dc)) {
            return DC_ENDPOINTS.get(dc);
        }
        assert keyspace.getReplicationStrategy() instanceof NetworkTopologyStrategy:
                "Invalid replication strategy class: "+keyspace.getReplicationStrategy().getClass().getName();

        String cql = "select count(*) from peers where data_center = '"+dc+"' ALLOW FILTERING";
        Long peersCountInDc = getCount(cql);
        LOG.info("I know there are {} endpoints in dc[{}] except me", peersCountInDc, dc);
        int i = 0;
        while (true) {
            NetworkTopologyStrategy strategy = (NetworkTopologyStrategy) keyspace.getReplicationStrategy();
            Field field;
            try {
                field = AbstractReplicationStrategy.class.getDeclaredField("tokenMetadata");
                field.setAccessible(true);
                TokenMetadata tokenMetadata = (TokenMetadata) field.get(strategy);
                Set<InetAddress> addresses = tokenMetadata.cloneOnlyTokenMap().getTopology()
                        .getDatacenterEndpoints().get(dc).parallelStream()
                        .collect(Collectors.toSet());
                Long requiredEndpoints = peersCountInDc;
                if(addresses.contains(FBUtilities.getBroadcastAddress())) {
                    requiredEndpoints++;
                }
                if(addresses.size() == requiredEndpoints) {
                    return addresses;
                } else {
                    LOG.info("Can't get enough addr for dc[{}], required:{}, got:{}",
                            dc, requiredEndpoints, addresses.size());
                }
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            }
            waitSomeSeconds(1);
            if(i++ > 100) {
                break;
            }
        }
        return new HashSet<>();
    }

    /**
     * Get token range and endpoints map for specefic dc in specific keyspace.
     * @param keyspace the keyspace
     * @param dc the dc
     * @return return token range and endpoints map
     */
    private static Map<Range<Token>, List<InetAddress>> dcRangeAddr(Keyspace keyspace, String dc) {
        if(!DC_ENDPOINTS.containsKey(dc)) {
            DC_ENDPOINTS.put(dc, addressesInDc(keyspace, dc));
        }
        if(!DC_ENDPOINT_PRIMARY_RANGES.containsKey(dc)) {
            DC_ENDPOINT_PRIMARY_RANGES.put(dc, new ConcurrentHashMap<>());
        }
        final Map<InetAddress, List<Range<Token>>> ENDPOINT_PRIMARY_RANGES = DC_ENDPOINT_PRIMARY_RANGES.get(dc);
        Map<Range<Token>, List<InetAddress>> map = new HashMap<>();
        try {
            Set<InetAddress> addresses = DC_ENDPOINTS.get(dc);
            List<Range<Token>> ranges = new ArrayList<>();
            List<Token> tokens = new ArrayList<>();
            // filter the token which locate the dc from full token owned by the cluster.
            SORTED_TOKEN_TO_ENDPOINT.forEach((t,i)->{
                if(addresses.contains(i)) {
                    tokens.add(t);
                }
            });
            if(!tokens.isEmpty()) {
                // generate token ranges for each endpoint
                Token last = tokens.get(tokens.size() - 1);
                for (Token token : tokens) {
                    Range<Token> range = new Range<>(last, token);
                    ranges.add(range);
                    InetAddress endpoint = SORTED_TOKEN_TO_ENDPOINT.get(token);
                    if (!ENDPOINT_PRIMARY_RANGES.containsKey(endpoint)) {
                        ENDPOINT_PRIMARY_RANGES.put(endpoint, new ArrayList<>());
                    }
                    ENDPOINT_PRIMARY_RANGES.get(endpoint).add(range);
                    last = token;
                }
            }
            ranges.forEach(r-> map.put(r,
                    StorageService.instance.getNaturalEndpoints(keyspace.getName(), r.right)
                            .parallelStream()
                            .filter(addresses::contains)  // this filter is needed, we cached the endpoints in this dc only
                            .collect(Collectors.toList())));

        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
        return map;
    }

    /**
     * Get token range and endpoints map for all data centers in specific keyspace
     * @param keyspace the keyspace
     */
    private static void allKsDcRangeAddr(Keyspace keyspace) {
        List<String> dcList = KS_DC.get(keyspace.getName());
        dcList.forEach(dc-> DC_TOKEN_ENDPOINTS.put(keyspace.getName()+"_"+dc,
                new ConcurrentHashMap<>(dcRangeAddr(keyspace, dc))));
    }

    /**
     * Get full token to endpoint map of the cluster.
     * @return return the map
     */
    private static Map<Token, InetAddress> getTokenToEndpointMap() {
        if(SORTED_TOKEN_TO_ENDPOINT.isEmpty()) {
            Map<Token, InetAddress> map = normalTokenToEndpoint();
            List<Token> sortedToken = new ArrayList<>(map.keySet());
            Collections.sort(sortedToken);
            sortedToken.forEach(t -> SORTED_TOKEN_TO_ENDPOINT.put(t, map.get(t)));
        }
        return SORTED_TOKEN_TO_ENDPOINT;
    }

    private static Map<Token, InetAddress> normalTokenToEndpoint() {
        String cql = "select count(*) from system.peers";
        Long peersCount = getCount(cql);

        LOG.info("I know there are {} endpoints in this cluster.", peersCount+1);
        while (true) {
            Map<Token, InetAddress> map = StorageService.instance.getTokenMetadata()
                    .getNormalAndBootstrappingTokenToEndpointMap();
            int endpointCount = map.values().stream()
                    .map(InetAddress::getHostAddress)
                    .collect(Collectors.toSet()).size();
            if (peersCount != endpointCount - 1) {
                LOG.warn("can't read all endpoints, please wait 1s");
            } else {
                LOG.info("got normal range endpoints: {}", map);
                return map;
            }
            waitSomeSeconds(1);
        }
    }

    private static long getCount(String cql) {
        while (true) {
            try {
                UntypedResultSet rs = executeQuery(cql);
                if (rs == null || rs.isEmpty()) {
                    LOG.info("can't get peers count, please wait 1s");
                } else {
                    return rs.one().getLong("count");
                }
            } catch (Throwable ex) {
                LOG.error(ex.getMessage(), ex);
            }
            waitSomeSeconds(1);
        }
    }

    private static void waitSomeSeconds(long seconds) {
        try {
            Thread.sleep(seconds*1000);
        } catch (InterruptedException e) {
            LOG.error(e.getMessage(), e);
        }
    }

    public static UntypedResultSet executeQuery(String cql) {
        LOG.debug("executing cql: {}", cql);
        return QueryProcessor.execute(cql,
                ConsistencyLevel.ONE,
                new QueryState(ClientState.forInternalCalls()));
    }
}

