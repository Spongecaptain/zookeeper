/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server.quorum;

import static org.apache.zookeeper.common.NetUtils.formatInetAddr;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.UnresolvedAddressException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.net.ssl.SSLSocket;
import org.apache.zookeeper.common.NetUtils;
import org.apache.zookeeper.common.X509Exception;
import org.apache.zookeeper.server.ExitCode;
import org.apache.zookeeper.server.ZooKeeperThread;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.apache.zookeeper.server.quorum.auth.QuorumAuthLearner;
import org.apache.zookeeper.server.quorum.auth.QuorumAuthServer;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.apache.zookeeper.server.util.ConfigUtils;
import org.apache.zookeeper.util.CircularBlockingQueue;
import org.apache.zookeeper.util.ServiceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class implements a connection manager for leader election using TCP. It
 * maintains one connection for every pair of servers. The tricky part is to
 * guarantee that there is exactly one connection for every pair of servers that
 * are operating correctly and that can communicate over the network.
 *
 * If two servers try to start a connection concurrently, then the connection
 * manager uses a very simple tie-breaking mechanism to decide which connection
 * to drop based on the IP addressed of the two parties.
 *
 * For every peer, the manager maintains a queue of messages to send. If the
 * connection to any particular peer drops, then the sender thread puts the
 * message back on the list. As this implementation currently uses a queue
 * implementation to maintain messages to send to another peer, we add the
 * message to the tail of the queue, thus changing the order of messages.
 * Although this is not a problem for the leader election, it could be a problem
 * when consolidating peer communication. This is to be verified, though.
 *
 */

/**
 * QuorumCnxManager 类使用 TCP 协议来负责集群各台服务器之间的底层 Leader 选举下的网络层通信
 * 它为每一对服务器之间维护了一对连接（如果集群中有 3 个服务器，那么此类的实例就需要维护 2 个连接，如果有 5 个服务器，那么显然就需要维护 4 对连接）
 * 最棘手的部分在于这个类需要为每一对服务器之间仅仅维护一对连接，这是这个类的难点，不过为什么会有这个问题？
 * 主要在于一对服务器中的任意服务器都可以发起连接，虽然每一个服务器用于 Leader 选举的监听端口是固定不变的，
 * 但是连接发起方的 Socket 地址是没有写死，由操作系统分配一个没有用过的端口(说了这么多，总之一对 Socket 连接是一个四元组，有一个不同即不同)
 * 为了保证这一特性，QuorumCnxManager 使用了一个简单的策略，在下面具体方法的注释上会有提到
 *
 * ZooKeeeper 每一个主机都在 QuorumCnxManager 中为其他主机（Peer）维护了一个队列来发送消息（Message）。
 * 如果与某一个 Peer 的连接中断，那么会将待发送的 Message 放回到队列中。因为 QuorumCnxManager 类使用 queue 来
 * 维护与存放发送给其他 peer 的 message，因此我们将消息加入到队列的尾部，这会改变 message 的顺序。
 * 为什么会改变顺序？
 * 因为如果要确保顺序，应当将回放的 message 放到队列头部，而不是尾部。从理论上将，回放的 message 比所有队列中的 message 有着更高的发送优先级。
 * 这并不影响 Leader Election，不过可能会影响连接的保活，但这有待验证。
 */
public class QuorumCnxManager {

    private static final Logger LOG = LoggerFactory.getLogger(QuorumCnxManager.class);

    /*
     * Maximum capacity of thread queues
     */
    static final int RECV_CAPACITY = 100;
    // Initialized to 1 to prevent sending
    // stale notifications to peers
    static final int SEND_CAPACITY = 1;

    static final int PACKETMAXSIZE = 1024 * 512;

    /*
     * Negative counter for observer server ids.
     */

    private AtomicLong observerCounter = new AtomicLong(-1);

    /*
     * Protocol identifier used among peers (must be a negative number for backward compatibility reasons)
     */
    // the following protocol version was sent in every connection initiation message since ZOOKEEPER-107 released in 3.5.0
    public static final long PROTOCOL_VERSION_V1 = -65536L;
    // ZOOKEEPER-3188 introduced multiple addresses in the connection initiation message, released in 3.6.0
    public static final long PROTOCOL_VERSION_V2 = -65535L;

    /*
     * Max buffer size to be read from the network.
     */
    public static final int maxBuffer = 2048;

    /*
     * Connection time out value in milliseconds
     */

    private int cnxTO = 5000;

    final QuorumPeer self;//指点

    /*
     * Local IP address
     */
    final long mySid;
    final int socketTimeout;
    final Map<Long, QuorumPeer.QuorumServer> view;//视图，Key myid,Value 参与竞选的其他服务器对应在当前主机堆内的 QuorumPeer.QuorumServer 实例
    final boolean listenOnAllIPs;
    private ThreadPoolExecutor connectionExecutor;
    private final Set<Long> inprogressConnections = Collections.synchronizedSet(new HashSet<>());
    private QuorumAuthServer authServer;
    private QuorumAuthLearner authLearner;
    private boolean quorumSaslAuthEnabled;
    /*
     * Counter to count connection processing threads.
     */
    private AtomicInteger connectionThreadCnt = new AtomicInteger(0);

    /**
     * Mapping from Peer to Thread number
     * 下面的若干字段是 QuorumCnxManager 类的核心，假设集群中有 n 个节点参与 Leader Election，那么其维护了如下实例：
     * n-1 条异步线程 SendWorker，每一个异步线程对应一个阻塞队列 (queueSendMap的value)，每一个线程负责消费阻塞队列上的消息
     * n-1 条异步线程 RecvWorker，所有异步异步线程仅仅对应一个阻塞队列 recvQueue，一起负责向此队列添加元素
     * n-1 个阻塞队列(ConcurrentHashMap<Long, BlockingQueue<ByteBuffer>> queueSendMap 的 value)，用于存储待发送的消息（每一个异步线程一个）
     * n-1 个 ByteBuffer 用于存储发给其余参与竞选节点的最后一个消息(存储于 lastMessageSent 字段中)
     * 1   个阻塞队列 BlockingQueue<Message> recvQueue，用于存储当前主机接收到的消息（这与发送相比截然不同，不管集群中哪一个节点发来的消息，都统一集中存放在一个队列中）
     * 我们通过 myid，即 serverId 来进行映射，找到当前主机与某一个参与竞选的其他节点的：异步线程、阻塞队列、最后一个消息。
     * 注意事项：并没有由 RecvWorker 作为键值组成的 Map，这是因为 SendWorker 与 RecvWorker 总是一一对应，我们通过
     * myid 来查找 SendWorker 实例，再通过其类内部的 `RecvWorker recvWorker;` 字段来找到 RecvWorker 实例的引用
     */
    final ConcurrentHashMap<Long, SendWorker> senderWorkerMap;// 每台服务器对应的 SendWorker(Worker 为一个异步线程)
    final ConcurrentHashMap<Long, BlockingQueue<ByteBuffer>> queueSendMap;// 需要发送给各个服务器的消息队列
    // 发送给每台服务器最近的消息，我猜测这是 ZooKeeper 用于在应用层保持心跳包，因为在 LeaderElection 过程中重复发送同一数据是没有意义的，只有心跳作用
    final ConcurrentHashMap<Long, ByteBuffer> lastMessageSent;


    /*
     * Reception queue
     */
    public final BlockingQueue<Message> recvQueue;// 本台服务器接收到的消息

    /*
     * Shutdown flag
     */

    volatile boolean shutdown = false;

    /*
     * Listener thread
     */
    public final Listener listener;

    /*
     * Counter to count worker threads
     */
    private AtomicInteger threadCnt = new AtomicInteger(0);

    /*
     * Socket options for TCP keepalive
     */
    private final boolean tcpKeepAlive = Boolean.getBoolean("zookeeper.tcpKeepAlive");


    /*
     * Socket factory, allowing the injection of custom socket implementations for testing
     */
    static final Supplier<Socket> DEFAULT_SOCKET_FACTORY = () -> new Socket();
    private static Supplier<Socket> SOCKET_FACTORY = DEFAULT_SOCKET_FACTORY;
    static void setSocketFactory(Supplier<Socket> factory) {
        SOCKET_FACTORY = factory;
    }


    public static class Message {

        Message(ByteBuffer buffer, long sid) {
            this.buffer = buffer;
            this.sid = sid;
        }

        ByteBuffer buffer;
        long sid;

    }

    /*
     * This class parses the initial identification sent out by peers with their
     * sid & hostname.
     */
    public static class InitialMessage {

        public Long sid;
        public List<InetSocketAddress> electionAddr;

        InitialMessage(Long sid, List<InetSocketAddress> addresses) {
            this.sid = sid;
            this.electionAddr = addresses;
        }

        @SuppressWarnings("serial")
        public static class InitialMessageException extends Exception {

            InitialMessageException(String message, Object... args) {
                super(String.format(message, args));
            }

        }

        public static InitialMessage parse(Long protocolVersion, DataInputStream din) throws InitialMessageException, IOException {
            Long sid;

            if (protocolVersion != PROTOCOL_VERSION_V1 && protocolVersion != PROTOCOL_VERSION_V2) {
                throw new InitialMessageException("Got unrecognized protocol version %s", protocolVersion);
            }

            sid = din.readLong();

            int remaining = din.readInt();
            if (remaining <= 0 || remaining > maxBuffer) {
                throw new InitialMessageException("Unreasonable buffer length: %s", remaining);
            }

            byte[] b = new byte[remaining];
            int num_read = din.read(b);

            if (num_read != remaining) {
                throw new InitialMessageException("Read only %s bytes out of %s sent by server %s", num_read, remaining, sid);
            }

            // in PROTOCOL_VERSION_V1 we expect to get a single address here represented as a 'host:port' string
            // in PROTOCOL_VERSION_V2 we expect to get multiple addresses like: 'host1:port1|host2:port2|...'
            String[] addressStrings = new String(b).split("\\|");
            List<InetSocketAddress> addresses = new ArrayList<>(addressStrings.length);
            for (String addr : addressStrings) {

                String[] host_port;
                try {
                    host_port = ConfigUtils.getHostAndPort(addr);
                } catch (ConfigException e) {
                    throw new InitialMessageException("Badly formed address: %s", addr);
                }

                if (host_port.length != 2) {
                    throw new InitialMessageException("Badly formed address: %s", addr);
                }

                int port;
                try {
                    port = Integer.parseInt(host_port[1]);
                } catch (NumberFormatException e) {
                    throw new InitialMessageException("Bad port number: %s", host_port[1]);
                } catch (ArrayIndexOutOfBoundsException e) {
                    throw new InitialMessageException("No port number in: %s", addr);
                }
                if (!isWildcardAddress(host_port[0])) {
                    addresses.add(new InetSocketAddress(host_port[0], port));
                }
            }

            return new InitialMessage(sid, addresses);
        }

        /**
         * Returns true if the specified hostname is a wildcard address,
         * like 0.0.0.0 for IPv4 or :: for IPv6
         *
         * (the function is package-private to be visible for testing)
         */
        static boolean isWildcardAddress(final String hostname) {
            try {
                return InetAddress.getByName(hostname).isAnyLocalAddress();
            } catch (UnknownHostException e) {
                // if we can not resolve, it can not be a wildcard address
                return false;
            }
        }

        @Override
        public String toString() {
            return "InitialMessage{sid=" + sid + ", electionAddr=" + electionAddr + '}';
        }
    }

    public QuorumCnxManager(QuorumPeer self, final long mySid, Map<Long, QuorumPeer.QuorumServer> view,
        QuorumAuthServer authServer, QuorumAuthLearner authLearner, int socketTimeout, boolean listenOnAllIPs,
        int quorumCnxnThreadsSize, boolean quorumSaslAuthEnabled) {

        this.recvQueue = new CircularBlockingQueue<>(RECV_CAPACITY);
        this.queueSendMap = new ConcurrentHashMap<>();
        this.senderWorkerMap = new ConcurrentHashMap<>();
        this.lastMessageSent = new ConcurrentHashMap<>();

        String cnxToValue = System.getProperty("zookeeper.cnxTimeout");
        if (cnxToValue != null) {
            this.cnxTO = Integer.parseInt(cnxToValue);
        }

        this.self = self;

        this.mySid = mySid;
        this.socketTimeout = socketTimeout;
        this.view = view;
        this.listenOnAllIPs = listenOnAllIPs;
        this.authServer = authServer;
        this.authLearner = authLearner;
        this.quorumSaslAuthEnabled = quorumSaslAuthEnabled;

        initializeConnectionExecutor(mySid, quorumCnxnThreadsSize);

        // Starts listener thread that waits for connection requests
        listener = new Listener();
        listener.setName("QuorumPeerListener");
    }

    // we always use the Connection Executor during connection initiation (to handle connection
    // timeouts), and optionally use it during receiving connections (as the Quorum SASL authentication
    // can take extra time)
    private void initializeConnectionExecutor(final long mySid, final int quorumCnxnThreadsSize) {
        final AtomicInteger threadIndex = new AtomicInteger(1);
        SecurityManager s = System.getSecurityManager();
        final ThreadGroup group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();

        final ThreadFactory daemonThFactory = runnable -> new Thread(group, runnable,
            String.format("QuorumConnectionThread-[myid=%d]-%d", mySid, threadIndex.getAndIncrement()));

        this.connectionExecutor = new ThreadPoolExecutor(3, quorumCnxnThreadsSize, 60, TimeUnit.SECONDS,
                                                         new SynchronousQueue<>(), daemonThFactory);
        this.connectionExecutor.allowCoreThreadTimeOut(true);
    }

    /**
     * Invokes initiateConnection for testing purposes
     *
     * @param sid
     */
    public void testInitiateConnection(long sid) {
        LOG.debug("Opening channel to server {}", sid);
        initiateConnection(self.getVotingView().get(sid).electionAddr, sid);
    }

    /**
     * First we create the socket, perform SSL handshake and authentication if needed.
     * Then we perform the initiation protocol.
     * If this server has initiated the connection, then it gives up on the
     * connection if it loses challenge. Otherwise, it keeps the connection.
     */
    /**
     *
     * @param electionAddr 要与本机建立 Socket 连接的地址
     * @param sid 要与本机建立 Socket 连接的服务器 id（myid/sid/serverId 都是同一个概念）
     *            建立连接连接主要分为  步：
     *            1.创建 Socket
     *            2.使用 SSL 握手以及验证（如果需要，可以不这么使用）
     *            3.执行初始化协议
     */
    public void initiateConnection(final MultipleAddresses electionAddr, final Long sid) {
        Socket sock = null;
        try {
            LOG.debug("Opening channel to server {}", sid);
            if (self.isSslQuorum()) {
                sock = self.getX509Util().createSSLSocket();
            } else {
                sock = SOCKET_FACTORY.get();
            }
            setSockOpts(sock);
            sock.connect(electionAddr.getReachableOrOne(), cnxTO);
            if (sock instanceof SSLSocket) {
                SSLSocket sslSock = (SSLSocket) sock;
                sslSock.startHandshake();
                LOG.info("SSL handshake complete with {} - {} - {}",
                         sslSock.getRemoteSocketAddress(),
                         sslSock.getSession().getProtocol(),
                         sslSock.getSession().getCipherSuite());
            }

            LOG.debug("Connected to server {} using election address: {}:{}",
                      sid, sock.getInetAddress(), sock.getPort());
        } catch (X509Exception e) {
            LOG.warn("Cannot open secure channel to {} at election address {}", sid, electionAddr, e);
            closeSocket(sock);
            return;
        } catch (UnresolvedAddressException | IOException e) {
            LOG.warn("Cannot open channel to {} at election address {}", sid, electionAddr, e);
            closeSocket(sock);
            return;
        }

        try {
            startConnection(sock, sid);
        } catch (IOException e) {
            LOG.error(
              "Exception while connecting, id: {}, addr: {}, closing learner connection",
              sid,
              sock.getRemoteSocketAddress(),
              e);
            closeSocket(sock);
        }
    }

    /**
     * Server will initiate the connection request to its peer server
     * asynchronously via separate connection thread.
     */
    public boolean initiateConnectionAsync(final MultipleAddresses electionAddr, final Long sid) {
        if (!inprogressConnections.add(sid)) {
            // simply return as there is a connection request to
            // server 'sid' already in progress.
            LOG.debug("Connection request to server id: {} is already in progress, so skipping this request", sid);
            return true;
        }
        try {
            connectionExecutor.execute(new QuorumConnectionReqThread(electionAddr, sid));
            connectionThreadCnt.incrementAndGet();
        } catch (Throwable e) {
            // Imp: Safer side catching all type of exceptions and remove 'sid'
            // from inprogress connections. This is to avoid blocking further
            // connection requests from this 'sid' in case of errors.
            inprogressConnections.remove(sid);
            LOG.error("Exception while submitting quorum connection request", e);
            return false;
        }
        return true;
    }

    /**
     * Thread to send connection request to peer server.
     */
    private class QuorumConnectionReqThread extends ZooKeeperThread {
        final MultipleAddresses electionAddr;
        final Long sid;
        QuorumConnectionReqThread(final MultipleAddresses electionAddr, final Long sid) {
            super("QuorumConnectionReqThread-" + sid);
            this.electionAddr = electionAddr;
            this.sid = sid;
        }

        @Override
        public void run() {
            try {
                initiateConnection(electionAddr, sid);
            } finally {
                inprogressConnections.remove(sid);
            }
        }

    }
    //用于与集群中某一个具体的节点建立 Socket 连接，这里的 sid 就是 myid
    private boolean startConnection(Socket sock, Long sid) throws IOException {
        DataOutputStream dout = null;
        DataInputStream din = null;
        LOG.debug("startConnection (myId:{} --> sid:{})", self.getId(), sid);
        try {
            // Use BufferedOutputStream to reduce the number of IP packets. This is
            // important for x-DC scenarios.
            BufferedOutputStream buf = new BufferedOutputStream(sock.getOutputStream());
            dout = new DataOutputStream(buf);

            // Sending id and challenge

            // First sending the protocol version (in other words - message type).
            // For backward compatibility reasons we stick to the old protocol version, unless the MultiAddress
            // feature is enabled. During rolling upgrade, we must make sure that all the servers can
            // understand the protocol version we use to avoid multiple partitions. see ZOOKEEPER-3720
            long protocolVersion = self.isMultiAddressEnabled() ? PROTOCOL_VERSION_V2 : PROTOCOL_VERSION_V1;
            dout.writeLong(protocolVersion);
            dout.writeLong(self.getId());

            // now we send our election address. For the new protocol version, we can send multiple addresses.
            Collection<InetSocketAddress> addressesToSend = protocolVersion == PROTOCOL_VERSION_V2
                    ? self.getElectionAddress().getAllAddresses()
                    : Arrays.asList(self.getElectionAddress().getOne());

            String addr = addressesToSend.stream()
                    .map(NetUtils::formatInetAddr).collect(Collectors.joining("|"));
            byte[] addr_bytes = addr.getBytes();
            dout.writeInt(addr_bytes.length);
            dout.write(addr_bytes);
            dout.flush();

            din = new DataInputStream(new BufferedInputStream(sock.getInputStream()));
        } catch (IOException e) {
            LOG.warn("Ignoring exception reading or writing challenge: ", e);
            closeSocket(sock);
            return false;
        }

        // authenticate learner
        QuorumPeer.QuorumServer qps = self.getVotingView().get(sid);
        if (qps != null) {
            // TODO - investigate why reconfig makes qps null.
            authLearner.authenticate(sock, qps.hostname);
        }

        // If lost the challenge, then drop the new connection
        // Socket 虽然能够双向连接，但是 ZooKeeper 为了确保两个服务器之间最多存在一条有效的用于 Leader 选举的 Socket 连接
        // 如果发现对方服务器的 sid 大于本级服务器 id，那么就会主动断开 Socket 连接
        // 换言之，最终只有 sid 较大方，主动发起的 Socket 连接最终才会被持久化，否则很快就会被关闭
        // 关于此，很容易带来疑惑：为什么两个服务器之间 Socket 可以建立两个？Socket 作为一个四元组不会重复吗？
        // 事实上不会重复，这是因为只有 ServerSocket 才会绑定（监听）端口，而 new Socket(host,port); 操作并不会占用 ServerSocket 的端口
        // 而是会被操作系统随机分配一个不冲突的端口，因此最终两台主机之间可以建立多个个 Socket 连接
        if (sid > self.getId()) {
            LOG.info("Have smaller server identifier, so dropping the connection: (myId:{} --> sid:{})", self.getId(), sid);
            closeSocket(sock);
            // Otherwise proceed with the connection
        } else {
            LOG.debug("Have larger server identifier, so keeping the connection: (myId:{} --> sid:{})", self.getId(), sid);
            //这里表示 Socket 连接已经建立成功，然后开始初始化 SendWorker 与 RecvWorker 并启动
            SendWorker sw = new SendWorker(sock, sid);
            RecvWorker rw = new RecvWorker(sock, din, sid, sw);
            sw.setRecv(rw);

            SendWorker vsw = senderWorkerMap.get(sid);

            if (vsw != null) {
                vsw.finish();
            }

            senderWorkerMap.put(sid, sw);

            queueSendMap.putIfAbsent(sid, new CircularBlockingQueue<>(SEND_CAPACITY));

            sw.start();
            rw.start();

            return true;

        }
        return false;
    }

    /**
     * If this server receives a connection request, then it gives up on the new
     * connection if it wins. Notice that it checks whether it has a connection
     * to this server already or not. If it does, then it sends the smallest
     * possible long value to lose the challenge.
     *
     */
    public void receiveConnection(final Socket sock) {
        DataInputStream din = null;
        try {
            din = new DataInputStream(new BufferedInputStream(sock.getInputStream()));

            LOG.debug("Sync handling of connection request received from: {}", sock.getRemoteSocketAddress());
            handleConnection(sock, din);
        } catch (IOException e) {
            LOG.error("Exception handling connection, addr: {}, closing server connection", sock.getRemoteSocketAddress());
            LOG.debug("Exception details: ", e);
            closeSocket(sock);
        }
    }

    /**
     * Server receives a connection request and handles it asynchronously via
     * separate thread.
     */
    public void receiveConnectionAsync(final Socket sock) {
        try {
            LOG.debug("Async handling of connection request received from: {}", sock.getRemoteSocketAddress());
            connectionExecutor.execute(new QuorumConnectionReceiverThread(sock));
            connectionThreadCnt.incrementAndGet();
        } catch (Throwable e) {
            LOG.error("Exception handling connection, addr: {}, closing server connection", sock.getRemoteSocketAddress());
            LOG.debug("Exception details: ", e);
            closeSocket(sock);
        }
    }

    /**
     * Thread to receive connection request from peer server.
     */
    private class QuorumConnectionReceiverThread extends ZooKeeperThread {

        private final Socket sock;
        QuorumConnectionReceiverThread(final Socket sock) {
            super("QuorumConnectionReceiverThread-" + sock.getRemoteSocketAddress());
            this.sock = sock;
        }

        @Override
        public void run() {
            receiveConnection(sock);
        }

    }

    private void handleConnection(Socket sock, DataInputStream din) throws IOException {
        Long sid = null, protocolVersion = null;
        MultipleAddresses electionAddr = null;

        try {
            protocolVersion = din.readLong();
            if (protocolVersion >= 0) { // this is a server id and not a protocol version
                sid = protocolVersion;
            } else {
                try {
                    InitialMessage init = InitialMessage.parse(protocolVersion, din);
                    sid = init.sid;
                    if (!init.electionAddr.isEmpty()) {
                        electionAddr = new MultipleAddresses(init.electionAddr,
                                Duration.ofMillis(self.getMultiAddressReachabilityCheckTimeoutMs()));
                    }
                    LOG.debug("Initial message parsed by {}: {}", self.getId(), init.toString());
                } catch (InitialMessage.InitialMessageException ex) {
                    LOG.error("Initial message parsing error!", ex);
                    closeSocket(sock);
                    return;
                }
            }

            if (sid == QuorumPeer.OBSERVER_ID) {
                /*
                 * Choose identifier at random. We need a value to identify
                 * the connection.
                 */
                sid = observerCounter.getAndDecrement();
                LOG.info("Setting arbitrary identifier to observer: {}", sid);
            }
        } catch (IOException e) {
            LOG.warn("Exception reading or writing challenge", e);
            closeSocket(sock);
            return;
        }

        // do authenticating learner
        authServer.authenticate(sock, din);
        //If wins the challenge, then close the new connection.
        if (sid < self.getId()) {
            /*
             * This replica might still believe that the connection to sid is
             * up, so we have to shut down the workers before trying to open a
             * new connection.
             */
            SendWorker sw = senderWorkerMap.get(sid);
            if (sw != null) {
                sw.finish();
            }

            /*
             * Now we start a new connection
             */
            LOG.debug("Create new connection to server: {}", sid);
            closeSocket(sock);

            if (electionAddr != null) {
                connectOne(sid, electionAddr);
            } else {
                connectOne(sid);
            }

        } else if (sid == self.getId()) {
            // we saw this case in ZOOKEEPER-2164
            LOG.warn("We got a connection request from a server with our own ID. "
                     + "This should be either a configuration error, or a bug.");
        } else { // Otherwise start worker threads to receive data.
            SendWorker sw = new SendWorker(sock, sid);
            RecvWorker rw = new RecvWorker(sock, din, sid, sw);
            sw.setRecv(rw);

            SendWorker vsw = senderWorkerMap.get(sid);

            if (vsw != null) {
                vsw.finish();
            }

            senderWorkerMap.put(sid, sw);

            queueSendMap.putIfAbsent(sid, new CircularBlockingQueue<>(SEND_CAPACITY));

            sw.start();
            rw.start();
        }
    }

    /**
     * Processes invoke this message to queue a message to send. Currently,
     * only leader election uses it.
     */

    /**
     * 将消息发送给
     * @param sid 主机的 myid，即 serverId（可以是当前主机自身）
     * @param b 要发送的数据，显然消息传递基于 NIO 实现
     */
    public void toSend(Long sid, ByteBuffer b) {
        /*
         * If sending message to myself, then simply enqueue it (loopback).
         */
        //如果消息是给自己发送的，那么当然不需要经过网络，直接将消息进行封装后添加存放本主机的接收消息队列中即可
        if (this.mySid == sid) {
            b.position(0);
            addToRecvQueue(new Message(b.duplicate(), sid));
            /*
             * Otherwise send to the corresponding thread to send.
             */
        } else {
            /*
             * Start a new connection if doesn't have one already.
             */
            //如果消息是给其他主机发送的，那么就将消息进行封装后加入到阻塞队列中
            //注意事项：CurrentHashMap.computeIfAbsent() 方法首先判断缓存MAP中是否存在指定key的值，
            //如果不存在，会自动调用 mappingFunction(key) 计算 key 的 value，然后将 key = value 放入到缓存 Map。
            //否则，就直接取出 value,总之不管怎么样，我们都能拿到此队列
            BlockingQueue<ByteBuffer> bq = queueSendMap.computeIfAbsent(sid, serverId -> new CircularBlockingQueue<>(SEND_CAPACITY));
            addToSendQueue(bq, b);
            //下面这个方法是一个异步连接的方法，如果没有连接，则会建立连接
            connectOne(sid);
        }
    }

    /**
     * Try to establish a connection to server with id sid using its electionAddr.
     * The function will return quickly and the connection will be established asynchronously.
     *
     * VisibleForTesting.
     *
     *  @param sid  server id
     *  @return boolean success indication
     */
    synchronized boolean connectOne(long sid, MultipleAddresses electionAddr) {
        if (senderWorkerMap.get(sid) != null) {
            LOG.debug("There is a connection already for server {}", sid);
            if (self.isMultiAddressEnabled() && electionAddr.size() > 1 && self.isMultiAddressReachabilityCheckEnabled()) {
                // since ZOOKEEPER-3188 we can use multiple election addresses to reach a server. It is possible, that the
                // one we are using is already dead and we need to clean-up, so when we will create a new connection
                // then we will choose an other one, which is actually reachable
                senderWorkerMap.get(sid).asyncValidateIfSocketIsStillReachable();
            }
            return true;
        }

        // we are doing connection initiation always asynchronously, since it is possible that
        // the socket connection timeouts or the SSL handshake takes too long and don't want
        // to keep the rest of the connections to wait
        return initiateConnectionAsync(electionAddr, sid);
    }

    /**
     * Try to establish a connection to server with id sid.
     * The function will return quickly and the connection will be established asynchronously.
     * 重点：这个方法是一个异步的过程，本主机将异步地与入口参数 sid 对应的主机建立连接
     * 方法返回时，连接不一定已经建立
     *  @param sid  server id
     *
     */
    synchronized void connectOne(long sid) {
        //是否已经注册好了一个 Socket 连接
        if (senderWorkerMap.get(sid) != null) {
            LOG.debug("There is a connection already for server {}", sid);
            if (self.isMultiAddressEnabled() && self.isMultiAddressReachabilityCheckEnabled()) {
                // since ZOOKEEPER-3188 we can use multiple election addresses to reach a server. It is possible, that the
                // one we are using is already dead and we need to clean-up, so when we will create a new connection
                // then we will choose an other one, which is actually reachable
                //异步地检测 Socket 是否依然可达
                senderWorkerMap.get(sid).asyncValidateIfSocketIsStillReachable();
            }
            return;
        }
        synchronized (self.QV_LOCK) {
            boolean knownId = false;
            // Resolve hostname for the remote server before attempting to
            // connect in case the underlying ip address has changed.
            self.recreateSocketAddresses(sid);
            Map<Long, QuorumPeer.QuorumServer> lastCommittedView = self.getView();
            QuorumVerifier lastSeenQV = self.getLastSeenQuorumVerifier();
            Map<Long, QuorumPeer.QuorumServer> lastProposedView = lastSeenQV.getAllMembers();
            if (lastCommittedView.containsKey(sid)) {
                knownId = true;
                LOG.debug("Server {} knows {} already, it is in the lastCommittedView", self.getId(), sid);
                if (connectOne(sid, lastCommittedView.get(sid).electionAddr)) {
                    return;
                }
            }
            if (lastSeenQV != null
                && lastProposedView.containsKey(sid)
                && (!knownId
                    || (lastProposedView.get(sid).electionAddr != lastCommittedView.get(sid).electionAddr))) {
                knownId = true;
                LOG.debug("Server {} knows {} already, it is in the lastProposedView", self.getId(), sid);

                if (connectOne(sid, lastProposedView.get(sid).electionAddr)) {
                    return;
                }
            }
            if (!knownId) {
                LOG.warn("Invalid server id: {} ", sid);
            }
        }
    }

    /**
     * Try to establish a connection with each server if one
     * doesn't exist.
     */

    //connectAll() 方法的执行逻辑非常简单，就是进行遍历，与参与竞选的其他所有节点建立连接，通过遍历 connectOne(sid) 实现
    //注意事项：由于 connectOne() 方法是一个异步的连接方法，因此这个方法也将是异步返回，即在返回时不见得本机已经与其他所有
    //参与竞选的主机建立好了 Socket 连接
    public void connectAll() {
        long sid;
        for (Enumeration<Long> en = queueSendMap.keys(); en.hasMoreElements(); ) {
            sid = en.nextElement();// sid 为服务器 id 的语义
            connectOne(sid);
        }
    }

    /**
     * Check if all queues are empty, indicating that all messages have been delivered.
     */
    boolean haveDelivered() {
        for (BlockingQueue<ByteBuffer> queue : queueSendMap.values()) {
            final int queueSize = queue.size();
            LOG.debug("Queue size: {}", queueSize);
            if (queueSize == 0) {
                return true;
            }
        }

        return false;
    }

    /**
     * Flag that it is time to wrap up all activities and interrupt the listener.
     */
    public void halt() {
        shutdown = true;
        LOG.debug("Halting listener");
        listener.halt();

        // Wait for the listener to terminate.
        try {
            listener.join();
        } catch (InterruptedException ex) {
            LOG.warn("Got interrupted before joining the listener", ex);
        }
        softHalt();

        // clear data structures used for auth
        if (connectionExecutor != null) {
            connectionExecutor.shutdown();
        }
        inprogressConnections.clear();
        resetConnectionThreadCount();
    }

    /**
     * A soft halt simply finishes workers.
     */
    public void softHalt() {
        for (SendWorker sw : senderWorkerMap.values()) {
            LOG.debug("Server {} is soft-halting sender towards: {}", self.getId(), sw);
            sw.finish();
        }
    }

    /**
     * Helper method to set socket options.
     *
     * @param sock
     *            Reference to socket
     */
    private void setSockOpts(Socket sock) throws SocketException {
        sock.setTcpNoDelay(true);
        sock.setKeepAlive(tcpKeepAlive);
        sock.setSoTimeout(this.socketTimeout);
    }

    /**
     * Helper method to close a socket.
     *
     * @param sock
     *            Reference to socket
     */
    private void closeSocket(Socket sock) {
        if (sock == null) {
            return;
        }

        try {
            sock.close();
        } catch (IOException ie) {
            LOG.error("Exception while closing", ie);
        }
    }

    /**
     * Return number of worker threads
     */
    public long getThreadCount() {
        return threadCnt.get();
    }

    /**
     * Return number of connection processing threads.
     */
    public long getConnectionThreadCount() {
        return connectionThreadCnt.get();
    }

    /**
     * Reset the value of connection processing threads count to zero.
     */
    private void resetConnectionThreadCount() {
        connectionThreadCnt.set(0);
    }

    /**
     * Thread to listen on some ports
     * Listener 类是一个线程类，其用于监听参与选举的所有其他节点的 Socket 消息
     */
    public class Listener extends ZooKeeperThread {

        private static final String ELECTION_PORT_BIND_RETRY = "zookeeper.electionPortBindRetry";
        private static final int DEFAULT_PORT_BIND_MAX_RETRY = 3;

        private final int portBindMaxRetry;
        private Runnable socketBindErrorHandler = () -> ServiceUtils.requestSystemExit(ExitCode.UNABLE_TO_BIND_QUORUM_PORT.getValue());
        private List<ListenerHandler> listenerHandlers;
        private final AtomicBoolean socketException;


        public Listener() {
            // During startup of thread, thread name will be overridden to
            // specific election address
            super("ListenerThread");

            socketException = new AtomicBoolean(false);

            // maximum retry count while trying to bind to election port
            // see ZOOKEEPER-3320 for more details
            final Integer maxRetry = Integer.getInteger(ELECTION_PORT_BIND_RETRY,
                    DEFAULT_PORT_BIND_MAX_RETRY);
            if (maxRetry >= 0) {
                LOG.info("Election port bind maximum retries is {}", maxRetry == 0 ? "infinite" : maxRetry);
                portBindMaxRetry = maxRetry;
            } else {
                LOG.info(
                  "'{}' contains invalid value: {}(must be >= 0). Use default value of {} instead.",
                  ELECTION_PORT_BIND_RETRY,
                  maxRetry,
                  DEFAULT_PORT_BIND_MAX_RETRY);
                portBindMaxRetry = DEFAULT_PORT_BIND_MAX_RETRY;
            }
        }

        /**
         * Change socket bind error handler. Used for testing.
         */
        void setSocketBindErrorHandler(Runnable errorHandler) {
            this.socketBindErrorHandler = errorHandler;
        }

        @Override
        public void run() {
            if (!shutdown) {
                LOG.debug("Listener thread started, myId: {}", self.getId());
                Set<InetSocketAddress> addresses;

                if (self.getQuorumListenOnAllIPs()) {
                    addresses = self.getElectionAddress().getWildcardAddresses();
                } else {
                    addresses = self.getElectionAddress().getAllAddresses();
                }

                CountDownLatch latch = new CountDownLatch(addresses.size());
                listenerHandlers = addresses.stream().map(address ->
                                new ListenerHandler(address, self.shouldUsePortUnification(), self.isSslQuorum(), latch))
                        .collect(Collectors.toList());

                ExecutorService executor = Executors.newFixedThreadPool(addresses.size());
                listenerHandlers.forEach(executor::submit);

                try {
                    latch.await();
                } catch (InterruptedException ie) {
                    LOG.error("Interrupted while sleeping. Ignoring exception", ie);
                } finally {
                    // Clean up for shutdown.
                    for (ListenerHandler handler : listenerHandlers) {
                        try {
                            handler.close();
                        } catch (IOException ie) {
                            // Don't log an error for shutdown.
                            LOG.debug("Error closing server socket", ie);
                        }
                    }
                }
            }

            LOG.info("Leaving listener");
            if (!shutdown) {
                LOG.error(
                  "As I'm leaving the listener thread, I won't be able to participate in leader election any longer: {}",
                  self.getElectionAddress().getAllAddresses().stream()
                    .map(NetUtils::formatInetAddr)
                    .collect(Collectors.joining("|")));
                if (socketException.get()) {
                    // After leaving listener thread, the host cannot join the quorum anymore,
                    // this is a severe error that we cannot recover from, so we need to exit
                    socketBindErrorHandler.run();
                }
            }
        }

        /**
         * Halts this listener thread.
         */
        void halt() {
            LOG.debug("Halt called: Trying to close listeners");
            if (listenerHandlers != null) {
                LOG.debug("Closing listener: {}", QuorumCnxManager.this.mySid);
                for (ListenerHandler handler : listenerHandlers) {
                    try {
                        handler.close();
                    } catch (IOException e) {
                        LOG.warn("Exception when shutting down listener: ", e);
                    }
                }
            }
        }

        class ListenerHandler implements Runnable, Closeable {
            //可以见得，ZooKeeper 对于来自客户端的请求，采用 NIO 的方式来处理，对于 Leader 选举（因为线程数量有限），因此使用 OIO 的方式来处理
            private ServerSocket serverSocket;
            private InetSocketAddress address;
            private boolean portUnification;
            private boolean sslQuorum;
            private CountDownLatch latch;
            //指的一提的是，只有应用层需要进行 leader 选举时，回会需要这个在传输层建立的类。
            ListenerHandler(InetSocketAddress address, boolean portUnification, boolean sslQuorum,
                            CountDownLatch latch) {
                this.address = address;
                this.portUnification = portUnification;
                this.sslQuorum = sslQuorum;
                this.latch = latch;
            }

            /**
             * Sleeps on acceptConnections().
             */
            @Override
            public void run() {
                try {
                    Thread.currentThread().setName("ListenerHandler-" + address);
                    acceptConnections();
                    try {
                        close();
                    } catch (IOException e) {
                        LOG.warn("Exception when shutting down listener: ", e);
                    }
                } catch (Exception e) {
                    // Output of unexpected exception, should never happen
                    LOG.error("Unexpected error ", e);
                } finally {
                    latch.countDown();
                }
            }

            @Override
            public synchronized void close() throws IOException {
                if (serverSocket != null && !serverSocket.isClosed()) {
                    LOG.debug("Trying to close listeners: {}", serverSocket);
                    serverSocket.close();
                }
            }

            /**
             * Sleeps on accept().
             */
            private void acceptConnections() {
                int numRetries = 0;
                Socket client = null;

                while ((!shutdown) && (portBindMaxRetry == 0 || numRetries < portBindMaxRetry)) {
                    try {
                        serverSocket = createNewServerSocket();
                        LOG.info("{} is accepting connections now, my election bind port: {}", QuorumCnxManager.this.mySid, address.toString());
                        while (!shutdown) {
                            try {
                                client = serverSocket.accept();
                                setSockOpts(client);
                                LOG.info("Received connection request from {}", client.getRemoteSocketAddress());
                                // Receive and handle the connection request
                                // asynchronously if the quorum sasl authentication is
                                // enabled. This is required because sasl server
                                // authentication process may take few seconds to finish,
                                // this may delay next peer connection requests.
                                if (quorumSaslAuthEnabled) {
                                    receiveConnectionAsync(client);
                                } else {
                                    receiveConnection(client);
                                }
                                numRetries = 0;
                            } catch (SocketTimeoutException e) {
                                LOG.warn("The socket is listening for the election accepted "
                                        + "and it timed out unexpectedly, but will retry."
                                        + "see ZOOKEEPER-2836");
                            }
                        }
                    } catch (IOException e) {
                        if (shutdown) {
                            break;
                        }

                        LOG.error("Exception while listening", e);

                        if (e instanceof SocketException) {
                            socketException.set(true);
                        }

                        numRetries++;
                        try {
                            close();
                            Thread.sleep(1000);
                        } catch (IOException ie) {
                            LOG.error("Error closing server socket", ie);
                        } catch (InterruptedException ie) {
                            LOG.error("Interrupted while sleeping. Ignoring exception", ie);
                        }
                        closeSocket(client);
                    }
                }
                if (!shutdown) {
                    LOG.error(
                      "Leaving listener thread for address {} after {} errors. Use {} property to increase retry count.",
                      formatInetAddr(address),
                      numRetries,
                      ELECTION_PORT_BIND_RETRY);
                }
            }

            private ServerSocket createNewServerSocket() throws IOException {
                ServerSocket socket;

                if (portUnification) {
                    LOG.info("Creating TLS-enabled quorum server socket");
                    socket = new UnifiedServerSocket(self.getX509Util(), true);
                } else if (sslQuorum) {
                    LOG.info("Creating TLS-only quorum server socket");
                    socket = new UnifiedServerSocket(self.getX509Util(), false);
                } else {
                    socket = new ServerSocket();
                }

                socket.setReuseAddress(true);
                socket.bind(address);

                return socket;
            }
        }

    }

    /**
     * Thread to send messages. Instance waits on a queue, and send a message as
     * soon as there is one available. If connection breaks, then opens a new
     * one.
     */
    class SendWorker extends ZooKeeperThread {

        Long sid;
        Socket sock;
        RecvWorker recvWorker;
        volatile boolean running = true;
        DataOutputStream dout;
        AtomicBoolean ongoingAsyncValidation = new AtomicBoolean(false);

        /**
         * An instance of this thread receives messages to send
         * through a queue and sends them to the server sid.
         *
         * @param sock
         *            Socket to remote peer
         * @param sid
         *            Server identifier of remote peer
         */
        SendWorker(Socket sock, Long sid) {
            super("SendWorker:" + sid);
            this.sid = sid;
            this.sock = sock;
            recvWorker = null;
            try {
                dout = new DataOutputStream(sock.getOutputStream());
            } catch (IOException e) {
                LOG.error("Unable to access socket output stream", e);
                closeSocket(sock);
                running = false;
            }
            LOG.debug("Address of remote peer: {}", this.sid);
        }

        synchronized void setRecv(RecvWorker recvWorker) {
            this.recvWorker = recvWorker;
        }

        /**
         * Returns RecvWorker that pairs up with this SendWorker.
         *
         * @return RecvWorker
         */

        synchronized RecvWorker getRecvWorker() {
            return recvWorker;
        }

        /**
         * 释放相关资源，包括:
         * - Socket
         * - 线程资源：SendWorker 以及 RecvWorker
         * @return
         */
        synchronized boolean finish() {
            LOG.debug("Calling SendWorker.finish for {}", sid);
            //防止关闭两次
            if (!running) {
                /*
                 * Avoids running finish() twice.
                 */
                return running;
            }

            running = false;//标志位
            closeSocket(sock);//关闭网络资源 Socket

            this.interrupt();//中断 SendWorker 的工作，即让其跳出其 while 循环
            if (recvWorker != null) {
                recvWorker.finish();
            }

            LOG.debug("Removing entry from senderWorkerMap sid={}", sid);

            senderWorkerMap.remove(sid, this);//注销在 QuorumCnxManager 中的此 SendWorker 线程
            threadCnt.decrementAndGet();//自减
            return running;//返回关闭的结果
        }

        /**
         * 这个方法是 JDK Socket 阻塞式的数据发送
         * 不过借用了 NIO 中的 ByteBuffer 作为序列化字节数据当做存储容器，不要被误导了
         * @param b
         * @throws IOException
         */
        synchronized void send(ByteBuffer b) throws IOException {
            byte[] msgBytes = new byte[b.capacity()];
            try {
                b.position(0);
                b.get(msgBytes);
            } catch (BufferUnderflowException be) {
                LOG.error("BufferUnderflowException ", be);
                return;
            }
            dout.writeInt(b.capacity());
            dout.write(b.array());
            dout.flush();
        }

        /**
         * 下面是 QuorumCnxManager.SendWorker 线程内部的工作逻辑
         */
        @Override
        public void run() {
            threadCnt.incrementAndGet();//计数器自加一下
            try {
                /**
                 * If there is nothing in the queue to send, then we
                 * send the lastMessage to ensure that the last message
                 * was received by the peer. The message could be dropped
                 * in case self or the peer shutdown their connection
                 * (and exit the thread) prior to reading/processing
                 * the last message. Duplicate messages are handled correctly
                 * by the peer.
                 *
                 * If the send queue is non-empty, then we have a recent
                 * message than that stored in lastMessage. To avoid sending
                 * stale message, we should send the message in the send queue.
                 */
                //根据服务器的 myid 取出 SendWorker 对应的阻塞队列，如果为空说明还没有注册，或者被删除了
                BlockingQueue<ByteBuffer> bq = queueSendMap.get(sid);
                //如果 bq（阻塞队列）为空或者 bq 内没有任何待发送的元素，那么就从 lastMessageSent 队列中取出一个 ByteBuffer 元素
                if (bq == null || isSendQueueEmpty(bq)) {
                    ByteBuffer b = lastMessageSent.get(sid);
                    //如果 ByteBuffer 不为空，那么发送，否则不做任何事
                    if (b != null) {
                        LOG.debug("Attempting to send lastMessage to sid={}", sid);
                        send(b);
                    }
                }
            } catch (IOException e) {
                LOG.error("Failed to send last message. Shutting down thread.", e);
                this.finish();
            }
            LOG.debug("SendWorker thread started towards {}. myId: {}", sid, QuorumCnxManager.this.mySid);

            try {
                //这里是 SendWorker 的 Main Loop（与 run() 方法最开始执行逻辑不同，这里会循环执行，前者只会执行一次）
                while (running && !shutdown && sock != null) {

                    ByteBuffer b = null;
                    try {
                        //这里得到 SendWorker 对应的阻塞队列 bq
                        BlockingQueue<ByteBuffer> bq = queueSendMap.get(sid);
                        /**
                         * 我们可以认为超时时间主要用于控制 ZooKeeper 各台服务器之间为了 Leader 选举心跳包的发送间隔
                         * 因此我们可以认为 ZooKeeper 集群处于稳定状态，即不需要选举，那么 ZooKeeper 集群的用于选举的 Socket 连接
                         * 通过 1 秒作为时间间隔进行发送心跳包（之前发过的重复数据）
                         */
                        //在队列不为空的情况下，根据超时时间 1 秒从阻塞队列中取出元素 ByteBuffer 实例
                        if (bq != null) {
                            b = pollSendQueue(bq, 1000, TimeUnit.MILLISECONDS);
                        } else {
                            LOG.error("No queue of incoming messages for server {}", sid);
                            break;
                        }
                        //如果取出的 ByteBuffer 还是空，那么从 lastMessageSent 中取出一个 ByteBuffer
                        /**
                         * 那么继续为空怎么办？
                         * 实际上不会，因为 ZooKeeper 集群中的节点在启动之初的第一件事就是发送选举用的投票消息
                         * 因此早在一开始 lastMessageSent 就有元素的。
                         * 而阻塞队列中的元素一旦被被消费掉，就不存在了，因此可能遇到取出元素返回 null 的情况
                         */
                        if (b != null) {
                            lastMessageSent.put(sid, b);
                            //通过 JDK NIO 发送序列化了的消息
                            send(b);
                        }
                    } catch (InterruptedException e) {
                        LOG.warn("Interrupted while waiting for message on queue", e);
                    }
                }
            } catch (Exception e) {
                LOG.warn(
                    "Exception when using channel: for id {} my id = {}",
                    sid ,
                    QuorumCnxManager.this.mySid,
                    e);
            }
            //如果发送异常，while 循环会终止，那么会调用此方法来完成相关资源的释放
            this.finish();

            LOG.warn("Send worker leaving thread id {} my id = {}", sid, self.getId());
        }

        //异步检测 Socket 是否存活的线程
        public void asyncValidateIfSocketIsStillReachable() {
            if (ongoingAsyncValidation.compareAndSet(false, true)) {
                //构造一个新异步线程实例后，启动
                new Thread(() -> {
                    LOG.debug("validate if destination address is reachable for sid {}", sid);
                    //如果 Socket 不为 null 才进行 isReachable() 进行测试，否则什么都不做
                    if (sock != null) {
                        InetAddress address = sock.getInetAddress();
                        try {
                            //如果 Socket 依然可达（存活），超时时间设置为 500 ms
                            if (address.isReachable(500)) {
                                LOG.debug("destination address {} is reachable for sid {}", address.toString(), sid);
                                ongoingAsyncValidation.set(false);
                                return;
                            }
                            //如果连接抛出异常（例如超时），那么在 catch 中什么都不做（不过会打印出一条日志）
                        } catch (NullPointerException | IOException ignored) {
                        }
                        LOG.warn(
                          "destination address {} not reachable anymore, shutting down the SendWorker for sid {}",
                          address.toString(),
                          sid);
                        //如果连接超时等异常（连接失效或超时），那么运行这里 SendWorker.finish() 方法来释放相关资源，最终关闭此线程
                        this.finish();
                    }
                }).start();
            } else {
                LOG.debug("validation of destination address for sid {} is skipped (it is already running)", sid);
            }
        }

    }

    /**
     * Thread to receive messages. Instance waits on a socket read. If the
     * channel breaks, then removes itself from the pool of receivers.
     */
    class RecvWorker extends ZooKeeperThread {

        Long sid;
        Socket sock;
        volatile boolean running = true;
        final DataInputStream din;
        final SendWorker sw;

        RecvWorker(Socket sock, DataInputStream din, Long sid, SendWorker sw) {
            super("RecvWorker:" + sid);
            this.sid = sid;
            this.sock = sock;
            this.sw = sw;
            this.din = din;
            try {
                // OK to wait until socket disconnects while reading.
                sock.setSoTimeout(0);
            } catch (IOException e) {
                LOG.error("Error while accessing socket for {}", sid, e);
                closeSocket(sock);
                running = false;
            }
        }

        /**
         * Shuts down this worker
         *
         * @return boolean  Value of variable running
         */
        synchronized boolean finish() {
            LOG.debug("RecvWorker.finish called. sid: {}. myId: {}", sid, QuorumCnxManager.this.mySid);
            if (!running) {
                /*
                 * Avoids running finish() twice.
                 */
                return running;
            }
            running = false;

            this.interrupt();
            threadCnt.decrementAndGet();
            return running;
        }

        @Override
        public void run() {
            threadCnt.incrementAndGet();//自加
            try {
                LOG.debug("RecvWorker thread towards {} started. myId: {}", sid, QuorumCnxManager.this.mySid);
                //这里是 RecvWorker 的 Main Loop
                while (running && !shutdown && sock != null) {
                    /**
                     * Reads the first int to determine the length of the
                     * message
                     */
                    int length = din.readInt();//得到消息的字节长度
                    if (length <= 0 || length > PACKETMAXSIZE) {
                        throw new IOException("Received packet with invalid packet: " + length);
                    }
                    /**
                     * Allocates a new ByteBuffer to receive the message
                     */
                    final byte[] msgArray = new byte[length];
                    //因为 ZooKeeper 的 LeaderElection 过程采用的是非 NIO 而是 JDK Sockset 实现，因此在没有数据时此方法会阻塞
                    din.readFully(msgArray, 0, length);
                    //将读到的消息封装为 Message 实例加入 QuorumCnxManager.recvQueue 队列中
                    addToRecvQueue(new Message(ByteBuffer.wrap(msgArray), sid));
                }
            } catch (Exception e) {
                LOG.warn(
                    "Connection broken for id {}, my id = {}",
                    sid,
                    QuorumCnxManager.this.mySid,
                    e);
            } finally {
                LOG.warn("Interrupting SendWorker thread from RecvWorker. sid: {}. myId: {}", sid, QuorumCnxManager.this.mySid);
                sw.finish();
                closeSocket(sock);
            }
        }

    }

    /**
     * Inserts an element in the provided {@link BlockingQueue}. This method
     * assumes that if the Queue is full, an element from the head of the Queue is
     * removed and the new item is inserted at the tail of the queue. This is done
     * to prevent a thread from blocking while inserting an element in the queue.
     *
     * @param queue Reference to the Queue
     * @param buffer Reference to the buffer to be inserted in the queue
     */
    private void addToSendQueue(final BlockingQueue<ByteBuffer> queue,
        final ByteBuffer buffer) {
        final boolean success = queue.offer(buffer);
        if (!success) {
          throw new RuntimeException("Could not insert into receive queue");
        }
    }

    /**
     * Returns true if queue is empty.
     * @param queue
     *          Reference to the queue
     * @return
     *      true if the specified queue is empty
     */
    private boolean isSendQueueEmpty(final BlockingQueue<ByteBuffer> queue) {
        return queue.isEmpty();
    }

    /**
     * Retrieves and removes buffer at the head of this queue,
     * waiting up to the specified wait time if necessary for an element to
     * become available.
     *
     * {@link BlockingQueue#poll(long, java.util.concurrent.TimeUnit)}
     */
    private ByteBuffer pollSendQueue(final BlockingQueue<ByteBuffer> queue,
          final long timeout, final TimeUnit unit) throws InterruptedException {
       return queue.poll(timeout, unit);
    }

    /**
     * Inserts an element in the {@link #recvQueue}. If the Queue is full, this
     * methods removes an element from the head of the Queue and then inserts the
     * element at the tail of the queue.
     * 注意 recvQueue 队列是有默认大小的，默认为 100 大小，如果此队列已经满（通常情况下不会），但是如果已经慢了
     * 那么加入新的元素时会将队列头的元素移除，并将当前元素加入队列中
     * 对于选举过程中发生一个消息被移除并不严重，因为即使是上一轮因为消息丢失而无法选举，那么进行下一轮选举就好了。
     * @param msg Reference to the message to be inserted in the queue
     */
    public void addToRecvQueue(final Message msg) {
      final boolean success = this.recvQueue.offer(msg);
      if (!success) {
          throw new RuntimeException("Could not insert into receive queue");
      }
    }

    /**
     * Retrieves and removes a message at the head of this queue,
     * waiting up to the specified wait time if necessary for an element to
     * become available.
     *
     * {@link BlockingQueue#poll(long, java.util.concurrent.TimeUnit)}
     */
    public Message pollRecvQueue(final long timeout, final TimeUnit unit)
       throws InterruptedException {
       return this.recvQueue.poll(timeout, unit);
    }

    public boolean connectedToPeer(long peerSid) {
        return senderWorkerMap.get(peerSid) != null;
    }

    public boolean isReconfigEnabled() {
        return self.isReconfigEnabled();
    }

}
