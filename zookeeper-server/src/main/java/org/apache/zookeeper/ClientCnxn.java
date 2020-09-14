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

package org.apache.zookeeper;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import javax.security.auth.login.LoginException;
import javax.security.sasl.SaslException;
import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.AsyncCallback.ACLCallback;
import org.apache.zookeeper.AsyncCallback.AllChildrenNumberCallback;
import org.apache.zookeeper.AsyncCallback.Children2Callback;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.Create2Callback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.EphemeralsCallback;
import org.apache.zookeeper.AsyncCallback.MultiCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.OpResult.ErrorResult;
import org.apache.zookeeper.Watcher.Event;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.ZooKeeper.WatchRegistration;
import org.apache.zookeeper.client.HostProvider;
import org.apache.zookeeper.client.ZKClientConfig;
import org.apache.zookeeper.client.ZooKeeperSaslClient;
import org.apache.zookeeper.common.Time;
import org.apache.zookeeper.proto.AuthPacket;
import org.apache.zookeeper.proto.ConnectRequest;
import org.apache.zookeeper.proto.Create2Response;
import org.apache.zookeeper.proto.CreateResponse;
import org.apache.zookeeper.proto.ExistsResponse;
import org.apache.zookeeper.proto.GetACLResponse;
import org.apache.zookeeper.proto.GetAllChildrenNumberResponse;
import org.apache.zookeeper.proto.GetChildren2Response;
import org.apache.zookeeper.proto.GetChildrenResponse;
import org.apache.zookeeper.proto.GetDataResponse;
import org.apache.zookeeper.proto.GetEphemeralsResponse;
import org.apache.zookeeper.proto.GetSASLRequest;
import org.apache.zookeeper.proto.ReplyHeader;
import org.apache.zookeeper.proto.RequestHeader;
import org.apache.zookeeper.proto.SetACLResponse;
import org.apache.zookeeper.proto.SetDataResponse;
import org.apache.zookeeper.proto.SetWatches;
import org.apache.zookeeper.proto.SetWatches2;
import org.apache.zookeeper.proto.WatcherEvent;
import org.apache.zookeeper.server.ByteBufferInputStream;
import org.apache.zookeeper.server.ZooKeeperThread;
import org.apache.zookeeper.server.ZooTrace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * This class manages the socket i/o for the client. ClientCnxn maintains a list
 * of available servers to connect to and "transparently" switches servers it is
 * connected to as needed.
 *
 */
@SuppressFBWarnings({"EI_EXPOSE_REP", "EI_EXPOSE_REP2"})
public class ClientCnxn {

    private static final Logger LOG = LoggerFactory.getLogger(ClientCnxn.class);

    /* ZOOKEEPER-706: If a session has a large number of watches set then
     * attempting to re-establish those watches after a connection loss may
     * fail due to the SetWatches request exceeding the server's configured
     * jute.maxBuffer value. To avoid this we instead split the watch
     * re-establishement across multiple SetWatches calls. This constant
     * controls the size of each call. It is set to 128kB to be conservative
     * with respect to the server's 1MB default for jute.maxBuffer.
     */
    private static final int SET_WATCHES_MAX_LENGTH = 128 * 1024;

    /* predefined xid's values recognized as special by the server */
    // -1 means notification(WATCHER_EVENT)
    public static final int NOTIFICATION_XID = -1;
    // -2 is the xid for pings
    public static final int PING_XID = -2;
    // -4 is the xid for AuthPacket
    public static final int AUTHPACKET_XID = -4;
    // -8 is the xid for setWatch
    public static final int SET_WATCHES_XID = -8;

    static class AuthData {

        AuthData(String scheme, byte[] data) {
            this.scheme = scheme;
            this.data = data;
        }

        String scheme;

        byte[] data;

    }

    private final CopyOnWriteArraySet<AuthData> authInfo = new CopyOnWriteArraySet<AuthData>();

    /**
     * These are the packets that have been sent and are waiting for a response.
     */
    private final Queue<Packet> pendingQueue = new ArrayDeque<>();

    /**
     * These are the packets that need to be sent.
     */
    private final LinkedBlockingDeque<Packet> outgoingQueue = new LinkedBlockingDeque<Packet>();

    private int connectTimeout;

    /**
     * The timeout in ms the client negotiated with the server. This is the
     * "real" timeout, not the timeout request by the client (which may have
     * been increased/decreased by the server which applies bounds to this
     * value.
     */
    private volatile int negotiatedSessionTimeout;

    private int readTimeout;

    private final int sessionTimeout;

    private final ZKWatchManager watchManager;

    private long sessionId;

    private byte[] sessionPasswd;

    /**
     * If true, the connection is allowed to go to r-o mode. This field's value
     * is sent, besides other data, during session creation handshake. If the
     * server on the other side of the wire is partitioned it'll accept
     * read-only clients only.
     */
    private boolean readOnly;

    final String chrootPath;

    final SendThread sendThread;

    final EventThread eventThread;

    /**
     * Set to true when close is called. Latches the connection such that we
     * don't attempt to re-connect to the server if in the middle of closing the
     * connection (client sends session disconnect to server as part of close
     * operation)
     */
    private volatile boolean closing = false;

    /**
     * A set of ZooKeeper hosts this client could connect to.
     */
    private final HostProvider hostProvider;

    /**
     * Is set to true when a connection to a r/w server is established for the
     * first time; never changed afterwards.
     * <p>
     * Is used to handle situations when client without sessionId connects to a
     * read-only server. Such client receives "fake" sessionId from read-only
     * server, but this sessionId is invalid for other servers. So when such
     * client finds a r/w server, it sends 0 instead of fake sessionId during
     * connection handshake and establishes new, valid session.
     * <p>
     * If this field is false (which implies we haven't seen r/w server before)
     * then non-zero sessionId is fake, otherwise it is valid.
     */
    volatile boolean seenRwServerBefore = false;

    public ZooKeeperSaslClient zooKeeperSaslClient;

    private final ZKClientConfig clientConfig;
    /**
     * If any request's response in not received in configured requestTimeout
     * then it is assumed that the response packet is lost.
     */
    private long requestTimeout;

    ZKWatchManager getWatcherManager() {
        return watchManager;
    }

    public long getSessionId() {
        return sessionId;
    }

    public byte[] getSessionPasswd() {
        return sessionPasswd;
    }

    public int getSessionTimeout() {
        return negotiatedSessionTimeout;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        SocketAddress local = sendThread.getClientCnxnSocket().getLocalSocketAddress();
        SocketAddress remote = sendThread.getClientCnxnSocket().getRemoteSocketAddress();
        sb.append("sessionid:0x").append(Long.toHexString(getSessionId()))
          .append(" local:").append(local)
          .append(" remoteserver:").append(remote)
          .append(" lastZxid:").append(lastZxid)
          .append(" xid:").append(xid)
          .append(" sent:").append(sendThread.getClientCnxnSocket().getSentCount())
          .append(" recv:").append(sendThread.getClientCnxnSocket().getRecvCount())
          .append(" queuedpkts:").append(outgoingQueue.size())
          .append(" pendingresp:").append(pendingQueue.size())
          .append(" queuedevents:").append(eventThread.waitingEvents.size());

        return sb.toString();
    }

    /**
     * This class allows us to pass the headers and the relevant records around.
     */
    static class Packet {

        RequestHeader requestHeader;

        ReplyHeader replyHeader;

        Record request;

        Record response;

        ByteBuffer bb;

        /** Client's view of the path (may differ due to chroot) **/
        String clientPath;
        /** Servers's view of the path (may differ due to chroot) **/
        String serverPath;

        boolean finished;

        AsyncCallback cb;

        Object ctx;

        WatchRegistration watchRegistration;

        public boolean readOnly;

        WatchDeregistration watchDeregistration;

        /** Convenience ctor */
        Packet(
            RequestHeader requestHeader,
            ReplyHeader replyHeader,
            Record request,
            Record response,
            WatchRegistration watchRegistration) {
            this(requestHeader, replyHeader, request, response, watchRegistration, false);
        }

        Packet(
            RequestHeader requestHeader,
            ReplyHeader replyHeader,
            Record request,
            Record response,
            WatchRegistration watchRegistration,
            boolean readOnly) {

            this.requestHeader = requestHeader;
            this.replyHeader = replyHeader;
            this.request = request;
            this.response = response;
            this.readOnly = readOnly;
            this.watchRegistration = watchRegistration;
        }

        public void createBB() {
            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                BinaryOutputArchive boa = BinaryOutputArchive.getArchive(baos);
                boa.writeInt(-1, "len"); // We'll fill this in later
                if (requestHeader != null) {
                    requestHeader.serialize(boa, "header");
                }
                if (request instanceof ConnectRequest) {
                    request.serialize(boa, "connect");
                    // append "am-I-allowed-to-be-readonly" flag
                    boa.writeBool(readOnly, "readOnly");
                } else if (request != null) {
                    request.serialize(boa, "request");
                }
                baos.close();
                this.bb = ByteBuffer.wrap(baos.toByteArray());
                this.bb.putInt(this.bb.capacity() - 4);
                this.bb.rewind();
            } catch (IOException e) {
                LOG.warn("Unexpected exception", e);
            }
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();

            sb.append("clientPath:" + clientPath);
            sb.append(" serverPath:" + serverPath);
            sb.append(" finished:" + finished);

            sb.append(" header:: " + requestHeader);
            sb.append(" replyHeader:: " + replyHeader);
            sb.append(" request:: " + request);
            sb.append(" response:: " + response);

            // jute toString is horrible, remove unnecessary newlines
            return sb.toString().replaceAll("\r*\n+", " ");
        }

    }

    /**
     * Creates a connection object. The actual network connect doesn't get
     * established until needed. The start() instance method must be called
     * subsequent to construction.
     *
     * @param chrootPath the chroot of this client. Should be removed from this Class in ZOOKEEPER-838
     * @param hostProvider the list of ZooKeeper servers to connect to
     * @param sessionTimeout the timeout for connections.
     * @param clientConfig the client configuration.
     * @param defaultWatcher default watcher for this connection
     * @param clientCnxnSocket the socket implementation used (e.g. NIO/Netty)
     * @param canBeReadOnly whether the connection is allowed to go to read-only mode in case of partitioning
     */
    public ClientCnxn(
        String chrootPath,
        HostProvider hostProvider,
        int sessionTimeout,
        ZKClientConfig clientConfig,
        Watcher defaultWatcher,
        ClientCnxnSocket clientCnxnSocket,
        boolean canBeReadOnly
    ) throws IOException {
        this(
            chrootPath,
            hostProvider,
            sessionTimeout,
            clientConfig,
            defaultWatcher,
            clientCnxnSocket,
            0,
            new byte[16],
            canBeReadOnly);
    }

    /**
     * Creates a connection object. The actual network connect doesn't get
     * established until needed. The start() instance method must be called
     * subsequent to construction.
     *
     * @param chrootPath the chroot of this client. Should be removed from this Class in ZOOKEEPER-838
     * @param hostProvider the list of ZooKeeper servers to connect to
     * @param sessionTimeout the timeout for connections.
     * @param clientConfig the client configuration.
     * @param defaultWatcher default watcher for this connection
     * @param clientCnxnSocket the socket implementation used (e.g. NIO/Netty)
     * @param sessionId session id if re-establishing session
     * @param sessionPasswd session passwd if re-establishing session
     * @param canBeReadOnly whether the connection is allowed to go to read-only mode in case of partitioning
     * @throws IOException in cases of broken network
     */
    public ClientCnxn(
        String chrootPath,
        HostProvider hostProvider,
        int sessionTimeout,
        ZKClientConfig clientConfig,
        Watcher defaultWatcher,
        ClientCnxnSocket clientCnxnSocket,
        long sessionId,
        byte[] sessionPasswd,
        boolean canBeReadOnly
    ) throws IOException {
        //1. 字段初始化
        this.chrootPath = chrootPath;
        this.hostProvider = hostProvider;
        this.sessionTimeout = sessionTimeout;
        this.clientConfig = clientConfig;
        this.sessionId = sessionId;
        this.sessionPasswd = sessionPasswd;
        this.readOnly = canBeReadOnly;

        this.watchManager = new ZKWatchManager(
                clientConfig.getBoolean(ZKClientConfig.DISABLE_AUTO_WATCH_RESET),
                defaultWatcher);

        this.connectTimeout = sessionTimeout / hostProvider.size();//一个连接的超时时间等于 Session 超时时间除以有多少个 host
        this.readTimeout = sessionTimeout * 2 / 3;//readTimeout 也是通过 Session Timeout 来计算的
        //2. 初始化下面两个线程
        this.sendThread = new SendThread(clientCnxnSocket);
        this.eventThread = new EventThread();
        initRequestTimeout();
    }

    public void start() {
        //可见 ClientCnxn 本身不是线程，其之所以能够暴露一个 start() 方法的原因在于其内部封装了两个线程实例
        //start() 方法即用于启动这两个线程
        //下面我们应当查看这两个线程实例的 run() 方法
        sendThread.start();
        eventThread.start();
    }

    private Object eventOfDeath = new Object();

    private static class WatcherSetEventPair {

        private final Set<Watcher> watchers;
        private final WatchedEvent event;

        public WatcherSetEventPair(Set<Watcher> watchers, WatchedEvent event) {
            this.watchers = watchers;
            this.event = event;
        }

    }

    /**
     * Guard against creating "-EventThread-EventThread-EventThread-..." thread
     * names when ZooKeeper object is being created from within a watcher.
     * See ZOOKEEPER-795 for details.
     */
    private static String makeThreadName(String suffix) {
        String name = Thread.currentThread().getName().replaceAll("-EventThread", "");
        return name + suffix;
    }

    /**
     * Tests that current thread is the main event loop.
     * This method is useful only for tests inside ZooKeeper project
     * it is not a public API intended for use by external applications.
     * @return true if Thread.currentThread() is an EventThread.
     */
    public static boolean isInEventThread() {
        return Thread.currentThread() instanceof EventThread;
    }

    class EventThread extends ZooKeeperThread {

        private final LinkedBlockingQueue<Object> waitingEvents = new LinkedBlockingQueue<Object>();

        /** This is really the queued session state until the event
         * thread actually processes the event and hands it to the watcher.
         * But for all intents and purposes this is the state.
         */
        private volatile KeeperState sessionState = KeeperState.Disconnected;

        private volatile boolean wasKilled = false;
        private volatile boolean isRunning = false;

        EventThread() {
            super(makeThreadName("-EventThread"));
            setDaemon(true);
        }

        public void queueEvent(WatchedEvent event) {
            queueEvent(event, null);
        }

        private void queueEvent(WatchedEvent event, Set<Watcher> materializedWatchers) {
            if (event.getType() == EventType.None && sessionState == event.getState()) {
                return;
            }
            sessionState = event.getState();
            final Set<Watcher> watchers;
            //注意，在 ClientCnxn.SendThread.readResponse() 方法中 materializedWatchers 为 null
            if (materializedWatchers == null) {
                // materialize the watchers based on the event
                //这里向 ZKWatchManager 实例索要能处理此 Event 的 Watcher 实例（多个，以 Set 容器保存）
                watchers = watchManager.materialize(event.getState(), event.getType(), event.getPath());
            } else {
                watchers = new HashSet<>(materializedWatchers);
            }
            //WatcherSetEventPair 类实例的作用就在于封装 Watcher 以及 Event，前者是事件处理器（处理器可以有多种），后者是事件
            WatcherSetEventPair pair = new WatcherSetEventPair(watchers, event);
            // queue the pair (watch set & event) for later processing
            waitingEvents.add(pair);
        }

        public void queueCallback(AsyncCallback cb, int rc, String path, Object ctx) {
            waitingEvents.add(new LocalCallback(cb, rc, path, ctx));
        }

        @SuppressFBWarnings("JLM_JSR166_UTILCONCURRENT_MONITORENTER")
        public void queuePacket(Packet packet) {
            if (wasKilled) {
                synchronized (waitingEvents) {
                    if (isRunning) {
                        waitingEvents.add(packet);
                    } else {
                        processEvent(packet);
                    }
                }
            } else {
                waitingEvents.add(packet);
            }
        }

        public void queueEventOfDeath() {
            waitingEvents.add(eventOfDeath);
        }

        @Override
        @SuppressFBWarnings("JLM_JSR166_UTILCONCURRENT_MONITORENTER")
        public void run() {
            try {
                isRunning = true;
                while (true) {
                    Object event = waitingEvents.take();//从队列中取出事件
                    if (event == eventOfDeath) {//事件是否已经被取消了
                        wasKilled = true;
                    } else {
                        processEvent(event);//处理事件
                    }
                    if (wasKilled) {
                        synchronized (waitingEvents) {
                            if (waitingEvents.isEmpty()) {
                                isRunning = false;
                                break;
                            }
                        }
                    }
                }
            } catch (InterruptedException e) {
                LOG.error("Event thread exiting due to interruption", e);
            }

            LOG.info("EventThread shut down for session: 0x{}", Long.toHexString(getSessionId()));
        }

        /**
         * 处理事件的第一步就是判断事件类型，事件类型有：WatcherSetEventPair、LocalCallback、StatCallback ...
         * 利用各种 if else 语句来实现最终的逻辑确实有点 low，使用设计模式可以漂亮一点
         * 我们这里的关注重点是 WatcherSetEventPair，其包含了 Watchers 用于处理事件，以及事件本身 Event 实例
         * @param event
         */
        private void processEvent(Object event) {
            try {
                //WatcherSetEventPair 这是我们的关注重点
                if (event instanceof WatcherSetEventPair) {
                    // each watcher will process the event
                    WatcherSetEventPair pair = (WatcherSetEventPair) event;
                    //这里的处理逻辑非常简单，就是遍历 Set<Watcher> 容器，每一个 Watcher 都处理一次 Event
                    for (Watcher watcher : pair.watchers) {
                        try {
                            //来源于 Watcher 接口的 process(WatchedEvent event) 方法
                            watcher.process(pair.event);
                        } catch (Throwable t) {
                            LOG.error("Error while calling watcher.", t);
                        }
                    }
                } else if (event instanceof LocalCallback) {
                    LocalCallback lcb = (LocalCallback) event;
                    if (lcb.cb instanceof StatCallback) {
                        ((StatCallback) lcb.cb).processResult(lcb.rc, lcb.path, lcb.ctx, null);
                    } else if (lcb.cb instanceof DataCallback) {
                        ((DataCallback) lcb.cb).processResult(lcb.rc, lcb.path, lcb.ctx, null, null);
                    } else if (lcb.cb instanceof ACLCallback) {
                        ((ACLCallback) lcb.cb).processResult(lcb.rc, lcb.path, lcb.ctx, null, null);
                    } else if (lcb.cb instanceof ChildrenCallback) {
                        ((ChildrenCallback) lcb.cb).processResult(lcb.rc, lcb.path, lcb.ctx, null);
                    } else if (lcb.cb instanceof Children2Callback) {
                        ((Children2Callback) lcb.cb).processResult(lcb.rc, lcb.path, lcb.ctx, null, null);
                    } else if (lcb.cb instanceof StringCallback) {
                        ((StringCallback) lcb.cb).processResult(lcb.rc, lcb.path, lcb.ctx, null);
                    } else if (lcb.cb instanceof AsyncCallback.EphemeralsCallback) {
                        ((AsyncCallback.EphemeralsCallback) lcb.cb).processResult(lcb.rc, lcb.ctx, null);
                    } else if (lcb.cb instanceof AsyncCallback.AllChildrenNumberCallback) {
                        ((AsyncCallback.AllChildrenNumberCallback) lcb.cb).processResult(lcb.rc, lcb.path, lcb.ctx, -1);
                    } else if (lcb.cb instanceof AsyncCallback.MultiCallback) {
                        ((AsyncCallback.MultiCallback) lcb.cb).processResult(lcb.rc, lcb.path, lcb.ctx, Collections.emptyList());
                    } else {
                        ((VoidCallback) lcb.cb).processResult(lcb.rc, lcb.path, lcb.ctx);
                    }
                } else {
                    Packet p = (Packet) event;
                    int rc = 0;
                    String clientPath = p.clientPath;
                    if (p.replyHeader.getErr() != 0) {
                        rc = p.replyHeader.getErr();
                    }
                    if (p.cb == null) {
                        LOG.warn("Somehow a null cb got to EventThread!");
                    } else if (p.response instanceof ExistsResponse
                               || p.response instanceof SetDataResponse
                               || p.response instanceof SetACLResponse) {
                        StatCallback cb = (StatCallback) p.cb;
                        if (rc == 0) {
                            if (p.response instanceof ExistsResponse) {
                                cb.processResult(rc, clientPath, p.ctx, ((ExistsResponse) p.response).getStat());
                            } else if (p.response instanceof SetDataResponse) {
                                cb.processResult(rc, clientPath, p.ctx, ((SetDataResponse) p.response).getStat());
                            } else if (p.response instanceof SetACLResponse) {
                                cb.processResult(rc, clientPath, p.ctx, ((SetACLResponse) p.response).getStat());
                            }
                        } else {
                            cb.processResult(rc, clientPath, p.ctx, null);
                        }
                    } else if (p.response instanceof GetDataResponse) {
                        DataCallback cb = (DataCallback) p.cb;
                        GetDataResponse rsp = (GetDataResponse) p.response;
                        if (rc == 0) {
                            cb.processResult(rc, clientPath, p.ctx, rsp.getData(), rsp.getStat());
                        } else {
                            cb.processResult(rc, clientPath, p.ctx, null, null);
                        }
                    } else if (p.response instanceof GetACLResponse) {
                        ACLCallback cb = (ACLCallback) p.cb;
                        GetACLResponse rsp = (GetACLResponse) p.response;
                        if (rc == 0) {
                            cb.processResult(rc, clientPath, p.ctx, rsp.getAcl(), rsp.getStat());
                        } else {
                            cb.processResult(rc, clientPath, p.ctx, null, null);
                        }
                    } else if (p.response instanceof GetChildrenResponse) {
                        ChildrenCallback cb = (ChildrenCallback) p.cb;
                        GetChildrenResponse rsp = (GetChildrenResponse) p.response;
                        if (rc == 0) {
                            cb.processResult(rc, clientPath, p.ctx, rsp.getChildren());
                        } else {
                            cb.processResult(rc, clientPath, p.ctx, null);
                        }
                    } else if (p.response instanceof GetAllChildrenNumberResponse) {
                        AllChildrenNumberCallback cb = (AllChildrenNumberCallback) p.cb;
                        GetAllChildrenNumberResponse rsp = (GetAllChildrenNumberResponse) p.response;
                        if (rc == 0) {
                            cb.processResult(rc, clientPath, p.ctx, rsp.getTotalNumber());
                        } else {
                            cb.processResult(rc, clientPath, p.ctx, -1);
                        }
                    } else if (p.response instanceof GetChildren2Response) {
                        Children2Callback cb = (Children2Callback) p.cb;
                        GetChildren2Response rsp = (GetChildren2Response) p.response;
                        if (rc == 0) {
                            cb.processResult(rc, clientPath, p.ctx, rsp.getChildren(), rsp.getStat());
                        } else {
                            cb.processResult(rc, clientPath, p.ctx, null, null);
                        }
                    } else if (p.response instanceof CreateResponse) {
                        StringCallback cb = (StringCallback) p.cb;
                        CreateResponse rsp = (CreateResponse) p.response;
                        if (rc == 0) {
                            cb.processResult(
                                rc,
                                clientPath,
                                p.ctx,
                                (chrootPath == null
                                    ? rsp.getPath()
                                    : rsp.getPath().substring(chrootPath.length())));
                        } else {
                            cb.processResult(rc, clientPath, p.ctx, null);
                        }
                    } else if (p.response instanceof Create2Response) {
                        Create2Callback cb = (Create2Callback) p.cb;
                        Create2Response rsp = (Create2Response) p.response;
                        if (rc == 0) {
                            cb.processResult(
                                    rc,
                                    clientPath,
                                    p.ctx,
                                    (chrootPath == null
                                            ? rsp.getPath()
                                            : rsp.getPath().substring(chrootPath.length())),
                                    rsp.getStat());
                        } else {
                            cb.processResult(rc, clientPath, p.ctx, null, null);
                        }
                    } else if (p.response instanceof MultiResponse) {
                        MultiCallback cb = (MultiCallback) p.cb;
                        MultiResponse rsp = (MultiResponse) p.response;
                        if (rc == 0) {
                            List<OpResult> results = rsp.getResultList();
                            int newRc = rc;
                            for (OpResult result : results) {
                                if (result instanceof ErrorResult
                                    && KeeperException.Code.OK.intValue()
                                       != (newRc = ((ErrorResult) result).getErr())) {
                                    break;
                                }
                            }
                            cb.processResult(newRc, clientPath, p.ctx, results);
                        } else {
                            cb.processResult(rc, clientPath, p.ctx, null);
                        }
                    } else if (p.response instanceof GetEphemeralsResponse) {
                        EphemeralsCallback cb = (EphemeralsCallback) p.cb;
                        GetEphemeralsResponse rsp = (GetEphemeralsResponse) p.response;
                        if (rc == 0) {
                            cb.processResult(rc, p.ctx, rsp.getEphemerals());
                        } else {
                            cb.processResult(rc, p.ctx, null);
                        }
                    } else if (p.cb instanceof VoidCallback) {
                        VoidCallback cb = (VoidCallback) p.cb;
                        cb.processResult(rc, clientPath, p.ctx);
                    }
                }
            } catch (Throwable t) {
                LOG.error("Unexpected throwable", t);
            }
        }

    }

    // @VisibleForTesting
    // 我们要记得一件事：如果客户端偶注册 Watcher，那么通过这里的 Package 就能够拿到对应的 Watcher 的注册信息
    protected void finishPacket(Packet p) {
        int err = p.replyHeader.getErr();//得到响应中的错误信息码，关于错误信息码最需要提醒的一点是其实际上就是响应状态码，可以表示成功
        // 这里将从 Package 实例中取出对应的 Watcher 并注册到 ZKWatchManager 实例中
        // 如果状态码不为正常(为 0 值)，那么就不会选择将此 Package 内的 WatchRegistration 注册到 WatchManager 中
        if (p.watchRegistration != null) {
            p.watchRegistration.register(err);
        }
        // Add all the removed watch events to the event queue, so that the
        // clients will be notified with 'Data/Child WatchRemoved' event type.

        if (p.watchDeregistration != null) {
            Map<EventType, Set<Watcher>> materializedWatchers = null;
            try {
                //响应返回错误码时，将 WatchRegistration 注册到 WatchManager 中
                materializedWatchers = p.watchDeregistration.unregister(err);
                for (Entry<EventType, Set<Watcher>> entry : materializedWatchers.entrySet()) {
                    Set<Watcher> watchers = entry.getValue();
                    if (watchers.size() > 0) {
                        //这里将
                        queueEvent(p.watchDeregistration.getClientPath(), err, watchers, entry.getKey());
                        // ignore connectionloss when removing from local
                        // session
                        p.replyHeader.setErr(Code.OK.intValue());
                    }
                }
            } catch (KeeperException.NoWatcherException nwe) {
                p.replyHeader.setErr(nwe.code().intValue());
            } catch (KeeperException ke) {
                p.replyHeader.setErr(ke.code().intValue());
            }
        }

        if (p.cb == null) {
            synchronized (p) {
                p.finished = true;
                //唤醒等待 Package 的 ZooKeeper 客户端线程
                p.notifyAll();
            }
        } else {
            p.finished = true;
            eventThread.queuePacket(p);
        }
    }

    void queueEvent(String clientPath, int err, Set<Watcher> materializedWatchers, EventType eventType) {
        KeeperState sessionState = KeeperState.SyncConnected;
        if (KeeperException.Code.SESSIONEXPIRED.intValue() == err
            || KeeperException.Code.CONNECTIONLOSS.intValue() == err) {
            sessionState = Event.KeeperState.Disconnected;
        }
        WatchedEvent event = new WatchedEvent(eventType, sessionState, clientPath);
        eventThread.queueEvent(event, materializedWatchers);
    }

    void queueCallback(AsyncCallback cb, int rc, String path, Object ctx) {
        eventThread.queueCallback(cb, rc, path, ctx);
    }

    // for test only
    protected void onConnecting(InetSocketAddress addr) {

    }

    private void conLossPacket(Packet p) {
        if (p.replyHeader == null) {
            return;
        }
        switch (state) {
        case AUTH_FAILED:
            p.replyHeader.setErr(KeeperException.Code.AUTHFAILED.intValue());
            break;
        case CLOSED:
            p.replyHeader.setErr(KeeperException.Code.SESSIONEXPIRED.intValue());
            break;
        default:
            p.replyHeader.setErr(KeeperException.Code.CONNECTIONLOSS.intValue());
        }
        finishPacket(p);
    }

    private volatile long lastZxid;

    public long getLastZxid() {
        return lastZxid;
    }

    static class EndOfStreamException extends IOException {

        private static final long serialVersionUID = -5438877188796231422L;

        public EndOfStreamException(String msg) {
            super(msg);
        }

        @Override
        public String toString() {
            return "EndOfStreamException: " + getMessage();
        }

    }

    private static class SessionTimeoutException extends IOException {

        private static final long serialVersionUID = 824482094072071178L;

        public SessionTimeoutException(String msg) {
            super(msg);
        }

    }

    private static class SessionExpiredException extends IOException {

        private static final long serialVersionUID = -1388816932076193249L;

        public SessionExpiredException(String msg) {
            super(msg);
        }

    }

    private static class RWServerFoundException extends IOException {

        private static final long serialVersionUID = 90431199887158758L;

        public RWServerFoundException(String msg) {
            super(msg);
        }

    }

    /**
     * This class services the outgoing request queue and generates the heart
     * beats. It also spawns the ReadThread.
     */
    class SendThread extends ZooKeeperThread {

        private long lastPingSentNs;
        private final ClientCnxnSocket clientCnxnSocket;
        private Random r = new Random();
        private boolean isFirstConnect = true;
        // 这个方法是 SendThread 线程用于处理可读事件对应的字节信息
        void readResponse(ByteBuffer incomingBuffer) throws IOException {
            ByteBufferInputStream bbis = new ByteBufferInputStream(incomingBuffer);
            BinaryInputArchive bbia = BinaryInputArchive.getArchive(bbis);
            ReplyHeader replyHdr = new ReplyHeader();
            //将字节数据序列化为一个 ReplyHeader 实例
            replyHdr.deserialize(bbia, "header");
            //根据 ReplyHeader 实例给的 xid 字段值来判断响应类型
            switch (replyHdr.getXid()) {
            case PING_XID://ping 响应
                LOG.debug("Got ping response for session id: 0x{} after {}ms.",
                    Long.toHexString(sessionId),
                    ((System.nanoTime() - lastPingSentNs) / 1000000));
                return;
              case AUTHPACKET_XID://鉴权响应
                LOG.debug("Got auth session id: 0x{}", Long.toHexString(sessionId));
                if (replyHdr.getErr() == KeeperException.Code.AUTHFAILED.intValue()) {
                    changeZkState(States.AUTH_FAILED);
                    eventThread.queueEvent(new WatchedEvent(Watcher.Event.EventType.None,
                        Watcher.Event.KeeperState.AuthFailed, null));
                    eventThread.queueEventOfDeath();
                }
              return;
            case NOTIFICATION_XID://事件通知响应
                //打印日志
                LOG.debug("Got notification session id: 0x{}",
                    Long.toHexString(sessionId));
                //构造一个新的 WatcherEvent 实例
                WatcherEvent event = new WatcherEvent();
                //WatcherEvent.deserialize() 方法将 BinaryInputArchive 反序列化为 WatcherEvent 实例
                event.deserialize(bbia, "response");
                //转换路径表示与路径修正
                // convert from a server path to a client path
                if (chrootPath != null) {
                    String serverPath = event.getPath();
                    if (serverPath.compareTo(chrootPath) == 0) {
                        event.setPath("/");
                    } else if (serverPath.length() > chrootPath.length()) {
                        event.setPath(serverPath.substring(chrootPath.length()));
                     } else {
                         LOG.warn("Got server path {} which is too short for chroot path {}.",
                             event.getPath(), chrootPath);
                     }
                }
                //将用于网络传输的 WatcherEvent 实例转换为用于逻辑操作的 WatchedEvent 实例
                WatchedEvent we = new WatchedEvent(event);
                //打印日志
                LOG.debug("Got {} for session id 0x{}", we, Long.toHexString(sessionId));
                //EventThread.queueEvent() 方法的主要功能就是将一个 WatchedEvent 实例加入其内部的 waitingEvents 队列中
                //注意：在 waitingEvents 队列中的元素最终会被 EventThread 来消费
                eventThread.queueEvent(we);
                return;
            default:
                break;
            }

            // If SASL authentication is currently in progress, construct and
            // send a response packet immediately, rather than queuing a
            // response as with other packets.
            if (tunnelAuthInProgress()) {
                GetSASLRequest request = new GetSASLRequest();
                request.deserialize(bbia, "token");
                zooKeeperSaslClient.respondToServer(request.getToken(), ClientCnxn.this);
                return;
            }
            //注意 Ping 包并不会走到这个逻辑，Ping 包的 package 也不会加入 pendingQueue 对立中
            //由于客户端是阻塞模型，因此在这里因为不会拿到 Ping 包响应，因此拿到的一定是各种命令（例如 create）的响应
            //另一方面，正是每一条客户端的命令都会阻塞（直到接收到上一条命令的响应后才能继续写一条的写入），因此直接从队列中
            //因此，直接从 pendingQueue 中取出的 package 恰好对应于导致客户端阻塞的那一条命令
            Packet packet;
            synchronized (pendingQueue) {
                if (pendingQueue.size() == 0) {
                    throw new IOException("Nothing in the queue, but got " + replyHdr.getXid());
                }
                packet = pendingQueue.remove();
            }
            /*
             * Since requests are processed in order, we better get a response
             * to the first request!
             */
            //将响应结果放到 Package 实例中
            try {
                if (packet.requestHeader.getXid() != replyHdr.getXid()) {
                    packet.replyHeader.setErr(KeeperException.Code.CONNECTIONLOSS.intValue());
                    throw new IOException("Xid out of order. Got Xid " + replyHdr.getXid()
                                          + " with err " + replyHdr.getErr()
                                          + " expected Xid " + packet.requestHeader.getXid()
                                          + " for a packet with details: " + packet);
                }

                packet.replyHeader.setXid(replyHdr.getXid());
                packet.replyHeader.setErr(replyHdr.getErr());
                packet.replyHeader.setZxid(replyHdr.getZxid());
                if (replyHdr.getZxid() > 0) {
                    lastZxid = replyHdr.getZxid();
                }
                if (packet.response != null && replyHdr.getErr() == 0) {
                    packet.response.deserialize(bbia, "response");
                }

                LOG.debug("Reading reply session id: 0x{}, packet:: {}", Long.toHexString(sessionId), packet);
            } finally {
                /**
                 * finishPacket 方法主要有两个逻辑：
                 * 1. 在同步阻塞模式下唤醒等待 Package 的 ZooKeeper 客户端线程
                 * 2. 根据响应是否有 Watcher 标志符来决定是否将对应的 Watcher 注册到 ZKWatchManager 中去
                 * 注意事项：这里还是要强调一点，如果一旦发现来自服务端的响应为 Ping、鉴权、事件通知，那么并不能运行到这里，
                 * 在 switch-case 中 case 语句块中就会返回。换言之，只有正常的请求的响应（例如 getData() 操作的请求才会到这里）
                 */

                finishPacket(packet);
            }
        }

        SendThread(ClientCnxnSocket clientCnxnSocket) throws IOException {
            super(makeThreadName("-SendThread()"));
            changeZkState(States.CONNECTING);
            this.clientCnxnSocket = clientCnxnSocket;
            setDaemon(true);
        }

        // TODO: can not name this method getState since Thread.getState()
        // already exists
        // It would be cleaner to make class SendThread an implementation of
        // Runnable
        /**
         * Used by ClientCnxnSocket
         *
         * @return
         */
        synchronized ZooKeeper.States getZkState() {
            return state;
        }

        synchronized void changeZkState(ZooKeeper.States newState) throws IOException {
            if (!state.isAlive() && newState == States.CONNECTING) {
                throw new IOException(
                        "Connection has already been closed and reconnection is not allowed");
            }
            // It's safer to place state modification at the end.
            state = newState;
        }

        ClientCnxnSocket getClientCnxnSocket() {
            return clientCnxnSocket;
        }

        /**
         * Setup session, previous watches, authentication.
         */
        void primeConnection() throws IOException {
            LOG.info(
                "Socket connection established, initiating session, client: {}, server: {}",
                clientCnxnSocket.getLocalSocketAddress(),
                clientCnxnSocket.getRemoteSocketAddress());
            isFirstConnect = false;
            long sessId = (seenRwServerBefore) ? sessionId : 0;
            ConnectRequest conReq = new ConnectRequest(0, lastZxid, sessionTimeout, sessId, sessionPasswd);
            // We add backwards since we are pushing into the front
            // Only send if there's a pending watch
            if (!clientConfig.getBoolean(ZKClientConfig.DISABLE_AUTO_WATCH_RESET)) {
                List<String> dataWatches = watchManager.getDataWatchList();
                List<String> existWatches = watchManager.getExistWatchList();
                List<String> childWatches = watchManager.getChildWatchList();
                List<String> persistentWatches = watchManager.getPersistentWatchList();
                List<String> persistentRecursiveWatches = watchManager.getPersistentRecursiveWatchList();
                if (!dataWatches.isEmpty() || !existWatches.isEmpty() || !childWatches.isEmpty()
                        || !persistentWatches.isEmpty() || !persistentRecursiveWatches.isEmpty()) {
                    Iterator<String> dataWatchesIter = prependChroot(dataWatches).iterator();
                    Iterator<String> existWatchesIter = prependChroot(existWatches).iterator();
                    Iterator<String> childWatchesIter = prependChroot(childWatches).iterator();
                    Iterator<String> persistentWatchesIter = prependChroot(persistentWatches).iterator();
                    Iterator<String> persistentRecursiveWatchesIter = prependChroot(persistentRecursiveWatches).iterator();
                    long setWatchesLastZxid = lastZxid;

                    while (dataWatchesIter.hasNext() || existWatchesIter.hasNext() || childWatchesIter.hasNext()
                            || persistentWatchesIter.hasNext() || persistentRecursiveWatchesIter.hasNext()) {
                        List<String> dataWatchesBatch = new ArrayList<String>();
                        List<String> existWatchesBatch = new ArrayList<String>();
                        List<String> childWatchesBatch = new ArrayList<String>();
                        List<String> persistentWatchesBatch = new ArrayList<String>();
                        List<String> persistentRecursiveWatchesBatch = new ArrayList<String>();
                        int batchLength = 0;

                        // Note, we may exceed our max length by a bit when we add the last
                        // watch in the batch. This isn't ideal, but it makes the code simpler.
                        while (batchLength < SET_WATCHES_MAX_LENGTH) {
                            final String watch;
                            if (dataWatchesIter.hasNext()) {
                                watch = dataWatchesIter.next();
                                dataWatchesBatch.add(watch);
                            } else if (existWatchesIter.hasNext()) {
                                watch = existWatchesIter.next();
                                existWatchesBatch.add(watch);
                            } else if (childWatchesIter.hasNext()) {
                                watch = childWatchesIter.next();
                                childWatchesBatch.add(watch);
                            }  else if (persistentWatchesIter.hasNext()) {
                                watch = persistentWatchesIter.next();
                                persistentWatchesBatch.add(watch);
                            } else if (persistentRecursiveWatchesIter.hasNext()) {
                                watch = persistentRecursiveWatchesIter.next();
                                persistentRecursiveWatchesBatch.add(watch);
                            } else {
                                break;
                            }
                            batchLength += watch.length();
                        }

                        Record record;
                        int opcode;
                        if (persistentWatchesBatch.isEmpty() && persistentRecursiveWatchesBatch.isEmpty()) {
                            // maintain compatibility with older servers - if no persistent/recursive watchers
                            // are used, use the old version of SetWatches
                            record = new SetWatches(setWatchesLastZxid, dataWatchesBatch, existWatchesBatch, childWatchesBatch);
                            opcode = OpCode.setWatches;
                        } else {
                            record = new SetWatches2(setWatchesLastZxid, dataWatchesBatch, existWatchesBatch,
                                    childWatchesBatch, persistentWatchesBatch, persistentRecursiveWatchesBatch);
                            opcode = OpCode.setWatches2;
                        }
                        RequestHeader header = new RequestHeader(ClientCnxn.SET_WATCHES_XID, opcode);
                        Packet packet = new Packet(header, new ReplyHeader(), record, null, null);
                        outgoingQueue.addFirst(packet);
                    }
                }
            }

            for (AuthData id : authInfo) {
                outgoingQueue.addFirst(
                    new Packet(
                        new RequestHeader(ClientCnxn.AUTHPACKET_XID, OpCode.auth),
                        null,
                        new AuthPacket(0, id.scheme, id.data),
                        null,
                        null));
            }
            outgoingQueue.addFirst(new Packet(null, null, conReq, null, null, readOnly));
            clientCnxnSocket.connectionPrimed();
            LOG.debug("Session establishment request sent on {}", clientCnxnSocket.getRemoteSocketAddress());
        }

        private List<String> prependChroot(List<String> paths) {
            if (chrootPath != null && !paths.isEmpty()) {
                for (int i = 0; i < paths.size(); ++i) {
                    String clientPath = paths.get(i);
                    String serverPath;
                    // handle clientPath = "/"
                    if (clientPath.length() == 1) {
                        serverPath = chrootPath;
                    } else {
                        serverPath = chrootPath + clientPath;
                    }
                    paths.set(i, serverPath);
                }
            }
            return paths;
        }

        private void sendPing() {
            lastPingSentNs = System.nanoTime();
            RequestHeader h = new RequestHeader(ClientCnxn.PING_XID, OpCode.ping);
            queuePacket(h, null, null, null, null, null, null, null, null);
        }

        private InetSocketAddress rwServerAddress = null;

        private static final int minPingRwTimeout = 100;

        private static final int maxPingRwTimeout = 60000;

        private int pingRwTimeout = minPingRwTimeout;

        // Set to true if and only if constructor of ZooKeeperSaslClient
        // throws a LoginException: see startConnect() below.
        private boolean saslLoginFailed = false;

        private void startConnect(InetSocketAddress addr) throws IOException {
            // initializing it for new connection
            saslLoginFailed = false;
            if (!isFirstConnect) {
                try {
                    Thread.sleep(r.nextInt(1000));
                } catch (InterruptedException e) {
                    LOG.warn("Unexpected exception", e);
                }
            }
            //修改当前客户端的状态为 CONNECTING（连接中，将其封装为一个方法的原因在于为了确保状态修改时不处于关闭等状态）
            changeZkState(States.CONNECTING);

            String hostPort = addr.getHostString() + ":" + addr.getPort();
            MDC.put("myid", hostPort);
            //这里的 setName() 方法为继承于 Thread 的方法，用于设置 Thread 实例内的 name 字段；
            setName(getName().replaceAll("\\(.*\\)", "(" + hostPort + ")"));
            // 简单认证与安全层 ( SASL ) 是一个在网络协议中用来认证和数据加密的构架
            // 这并不是 ZooKeeper 客户端启动的重点步骤，这里就不过多地展开了
            if (clientConfig.isSaslClientEnabled()) {
                try {
                    if (zooKeeperSaslClient != null) {
                        zooKeeperSaslClient.shutdown();
                    }
                    zooKeeperSaslClient = new ZooKeeperSaslClient(SaslServerPrincipal.getServerPrincipal(addr, clientConfig), clientConfig);
                } catch (LoginException e) {
                    // An authentication error occurred when the SASL client tried to initialize:
                    // for Kerberos this means that the client failed to authenticate with the KDC.
                    // This is different from an authentication error that occurs during communication
                    // with the Zookeeper server, which is handled below.
                    LOG.warn(
                        "SASL configuration failed. "
                            + "Will continue connection to Zookeeper server without "
                            + "SASL authentication, if Zookeeper server allows it.", e);
                    eventThread.queueEvent(new WatchedEvent(Watcher.Event.EventType.None, Watcher.Event.KeeperState.AuthFailed, null));
                    saslLoginFailed = true;
                }
            }
            //记录一条开启 Socket 连接的日志
            logStartConnect(addr);
            //真正开始 Socket 连接
            clientCnxnSocket.connect(addr);//默认情况下，这个类的实例类型为 ClientCnxnSocketNIO，但是可以选择为基于 Netty 实现的 ClientCnxnSocketNetty
        }

        private void logStartConnect(InetSocketAddress addr) {
            LOG.info("Opening socket connection to server {}.", addr);
            if (zooKeeperSaslClient != null) {
                LOG.info("SASL config status: {}", zooKeeperSaslClient.getConfigStatus());
            }
        }

        @Override
        public void run() {
            clientCnxnSocket.introduce(this, sessionId, outgoingQueue);
            clientCnxnSocket.updateNow();
            clientCnxnSocket.updateLastSendAndHeard();
            int to;
            long lastPingRwServer = Time.currentElapsedTime();
            final int MAX_SEND_PING_INTERVAL = 10000; //10 seconds
            InetSocketAddress serverAddress = null;
            //连接是否存活的标准是：当前 ZooKeeper 实例既没有被关闭，又没有验证失败
            while (state.isAlive()) {
                try {
                    if (!clientCnxnSocket.isConnected()) {//如果此时 Socket 的连接还没有建立，那么就会启动如下的逻辑开始建立连接
                        // don't re-establish connection if we are closing
                        if (closing) {
                            break;
                        }
                        if (rwServerAddress != null) {
                            serverAddress = rwServerAddress;
                            rwServerAddress = null;
                        } else {
                            //可见 HostProvider 类的作用就是以轮询遍历地方式提供 host
                            serverAddress = hostProvider.next(1000);
                        }
                        onConnecting(serverAddress);//这个方法仅仅用于测试使用
                        //开始 Socket 连接（NIO）
                        startConnect(serverAddress);
                        //
                        clientCnxnSocket.updateLastSendAndHeard();
                    }

                    if (state.isConnected()) {//如果当前状态处于 Socket 已连接，那么进行如下的逻辑(这里主要用于权限检查，不是重点)
                        // determine whether we need to send an AuthFailed event.
                        if (zooKeeperSaslClient != null) {
                            boolean sendAuthEvent = false;
                            if (zooKeeperSaslClient.getSaslState() == ZooKeeperSaslClient.SaslState.INITIAL) {
                                try {
                                    zooKeeperSaslClient.initialize(ClientCnxn.this);
                                } catch (SaslException e) {
                                    LOG.error("SASL authentication with Zookeeper Quorum member failed.", e);
                                    changeZkState(States.AUTH_FAILED);
                                    sendAuthEvent = true;
                                }
                            }
                            KeeperState authState = zooKeeperSaslClient.getKeeperState();
                            if (authState != null) {
                                if (authState == KeeperState.AuthFailed) {
                                    // An authentication error occurred during authentication with the Zookeeper Server.
                                    changeZkState(States.AUTH_FAILED);
                                    sendAuthEvent = true;
                                } else {
                                    if (authState == KeeperState.SaslAuthenticated) {
                                        sendAuthEvent = true;
                                    }
                                }
                            }

                            if (sendAuthEvent) {
                                eventThread.queueEvent(new WatchedEvent(Watcher.Event.EventType.None, authState, null));
                                if (state == States.AUTH_FAILED) {
                                    eventThread.queueEventOfDeath();
                                }
                            }
                        }
                        //to 在连接已经建立的情况下用于统计读取数据的时间，其中被减数为规定值，减数为实际使用值
                        to = readTimeout - clientCnxnSocket.getIdleRecv();
                    } else {//to 在连接未建立状态用于连接是否超时的统计
                        to = connectTimeout - clientCnxnSocket.getIdleRecv();
                    }

                    if (to <= 0) {//说明超时了
                        String warnInfo = String.format(
                            "Client session timed out, have not heard from server in %dms for session id 0x%s",
                            clientCnxnSocket.getIdleRecv(),
                            Long.toHexString(sessionId));
                        LOG.warn(warnInfo);
                        //这里的抛出的异常会被 while 循环内的最外层 try 捕获
                        throw new SessionTimeoutException(warnInfo);
                    }
                    if (state.isConnected()) {
                        //1000(1 second) is to prevent race condition missing to send the second ping
                        //also make sure not to send too many pings when readTimeout is small
                        int timeToNextPing = readTimeout / 2
                                             - clientCnxnSocket.getIdleSend()
                                             - ((clientCnxnSocket.getIdleSend() > 1000) ? 1000 : 0);
                        //send a ping request either time is due or no packet sent out within MAX_SEND_PING_INTERVAL
                        //只有在达到规定的 Ping 包发送间隔后才会发送一个 Ping 包
                        if (timeToNextPing <= 0 || clientCnxnSocket.getIdleSend() > MAX_SEND_PING_INTERVAL) {
                            sendPing();//向队列中加入一个 ping 包，相当于心跳包 注意事项：这里没有真正地发送 ping 包
                            clientCnxnSocket.updateLastSend();
                        } else {
                            if (timeToNextPing < to) {
                                to = timeToNextPing;
                            }
                        }
                    }
                    // 如果客户端处于只读模式，那么进行如下的逻辑
                    // If we are in read-only mode, seek for read/write server
                    if (state == States.CONNECTEDREADONLY) {
                        long now = Time.currentElapsedTime();
                        int idlePingRwServer = (int) (now - lastPingRwServer);
                        if (idlePingRwServer >= pingRwTimeout) {
                            lastPingRwServer = now;
                            idlePingRwServer = 0;
                            pingRwTimeout = Math.min(2 * pingRwTimeout, maxPingRwTimeout);
                            pingRwServer();
                        }
                        to = Math.min(to, pingRwTimeout - idlePingRwServer);
                    }
                    //进行真正队列的消费，进行 package 的传输（包括客户端命令对应的 package 以及 ping 包）
                    //因为 NIO 网络传输逻辑有多个实现类，这类以基于 JDK NIO 逻辑的 ClientCnxnSocketNIO 进行说明
                    clientCnxnSocket.doTransport(to, pendingQueue, ClientCnxn.this);
                } catch (Throwable e) {
                    if (closing) {
                        // closing so this is expected
                        LOG.warn(
                            "An exception was thrown while closing send thread for session 0x{}.",
                            Long.toHexString(getSessionId()),
                            e);
                        break;
                    } else {
                        LOG.warn(
                            "Session 0x{} for sever {}, Closing socket connection. "
                                + "Attempting reconnect except it is a SessionExpiredException.",
                            Long.toHexString(getSessionId()),
                            serverAddress,
                            e);

                        // At this point, there might still be new packets appended to outgoingQueue.
                        // they will be handled in next connection or cleared up if closed.
                        cleanAndNotifyState();
                    }
                }
            }

            synchronized (state) {
                // When it comes to this point, it guarantees that later queued
                // packet to outgoingQueue will be notified of death.
                cleanup();
            }
            clientCnxnSocket.close();
            if (state.isAlive()) {
                eventThread.queueEvent(new WatchedEvent(Event.EventType.None, Event.KeeperState.Disconnected, null));
            }
            eventThread.queueEvent(new WatchedEvent(Event.EventType.None, Event.KeeperState.Closed, null));
            ZooTrace.logTraceMessage(
                LOG,
                ZooTrace.getTextTraceLevel(),
                "SendThread exited loop for session: 0x" + Long.toHexString(getSessionId()));
        }

        private void cleanAndNotifyState() {
            cleanup();
            if (state.isAlive()) {
                eventThread.queueEvent(new WatchedEvent(Event.EventType.None, Event.KeeperState.Disconnected, null));
            }
            clientCnxnSocket.updateNow();
            clientCnxnSocket.updateLastSendAndHeard();
        }

        private void pingRwServer() throws RWServerFoundException {
            String result = null;
            InetSocketAddress addr = hostProvider.next(0);

            LOG.info("Checking server {} for being r/w. Timeout {}", addr, pingRwTimeout);

            Socket sock = null;
            BufferedReader br = null;
            try {
                sock = new Socket(addr.getHostString(), addr.getPort());
                sock.setSoLinger(false, -1);
                sock.setSoTimeout(1000);
                sock.setTcpNoDelay(true);
                sock.getOutputStream().write("isro".getBytes());
                sock.getOutputStream().flush();
                sock.shutdownOutput();
                br = new BufferedReader(new InputStreamReader(sock.getInputStream()));
                result = br.readLine();
            } catch (ConnectException e) {
                // ignore, this just means server is not up
            } catch (IOException e) {
                // some unexpected error, warn about it
                LOG.warn("Exception while seeking for r/w server.", e);
            } finally {
                if (sock != null) {
                    try {
                        sock.close();
                    } catch (IOException e) {
                        LOG.warn("Unexpected exception", e);
                    }
                }
                if (br != null) {
                    try {
                        br.close();
                    } catch (IOException e) {
                        LOG.warn("Unexpected exception", e);
                    }
                }
            }

            if ("rw".equals(result)) {
                pingRwTimeout = minPingRwTimeout;
                // save the found address so that it's used during the next
                // connection attempt
                rwServerAddress = addr;
                throw new RWServerFoundException("Majority server found at "
                                                 + addr.getHostString() + ":" + addr.getPort());
            }
        }

        private void cleanup() {
            clientCnxnSocket.cleanup();
            synchronized (pendingQueue) {
                for (Packet p : pendingQueue) {
                    conLossPacket(p);
                }
                pendingQueue.clear();
            }
            // We can't call outgoingQueue.clear() here because
            // between iterating and clear up there might be new
            // packets added in queuePacket().
            Iterator<Packet> iter = outgoingQueue.iterator();
            while (iter.hasNext()) {
                Packet p = iter.next();
                conLossPacket(p);
                iter.remove();
            }
        }

        /**
         * Callback invoked by the ClientCnxnSocket once a connection has been
         * established.
         *
         * @param _negotiatedSessionTimeout
         * @param _sessionId
         * @param _sessionPasswd
         * @param isRO
         * @throws IOException
         */
        void onConnected(
            int _negotiatedSessionTimeout,
            long _sessionId,
            byte[] _sessionPasswd,
            boolean isRO) throws IOException {
            negotiatedSessionTimeout = _negotiatedSessionTimeout;
            if (negotiatedSessionTimeout <= 0) {
                changeZkState(States.CLOSED);

                eventThread.queueEvent(new WatchedEvent(Watcher.Event.EventType.None, Watcher.Event.KeeperState.Expired, null));
                eventThread.queueEventOfDeath();

                String warnInfo = String.format(
                    "Unable to reconnect to ZooKeeper service, session 0x%s has expired",
                    Long.toHexString(sessionId));
                LOG.warn(warnInfo);
                throw new SessionExpiredException(warnInfo);
            }

            if (!readOnly && isRO) {
                LOG.error("Read/write client got connected to read-only server");
            }

            readTimeout = negotiatedSessionTimeout * 2 / 3;
            connectTimeout = negotiatedSessionTimeout / hostProvider.size();
            hostProvider.onConnected();
            sessionId = _sessionId;
            sessionPasswd = _sessionPasswd;
            changeZkState((isRO) ? States.CONNECTEDREADONLY : States.CONNECTED);
            seenRwServerBefore |= !isRO;
            LOG.info(
                "Session establishment complete on server {}, session id = 0x{}, negotiated timeout = {}{}",
                clientCnxnSocket.getRemoteSocketAddress(),
                Long.toHexString(sessionId),
                negotiatedSessionTimeout,
                (isRO ? " (READ-ONLY mode)" : ""));
            KeeperState eventState = (isRO) ? KeeperState.ConnectedReadOnly : KeeperState.SyncConnected;
            eventThread.queueEvent(new WatchedEvent(Watcher.Event.EventType.None, eventState, null));
        }

        void close() {
            try {
                changeZkState(States.CLOSED);
            } catch (IOException e) {
                LOG.warn("Connection close fails when migrates state from {} to CLOSED",
                        getZkState());
            }
            clientCnxnSocket.onClosing();
        }

        void testableCloseSocket() throws IOException {
            clientCnxnSocket.testableCloseSocket();
        }

        public boolean tunnelAuthInProgress() {
            // 1. SASL client is disabled.
            if (!clientConfig.isSaslClientEnabled()) {
                return false;
            }

            // 2. SASL login failed.
            if (saslLoginFailed) {
                return false;
            }

            // 3. SendThread has not created the authenticating object yet,
            // therefore authentication is (at the earliest stage of being) in progress.
            if (zooKeeperSaslClient == null) {
                return true;
            }

            // 4. authenticating object exists, so ask it for its progress.
            return zooKeeperSaslClient.clientTunneledAuthenticationInProgress();
        }

        public void sendPacket(Packet p) throws IOException {
            clientCnxnSocket.sendPacket(p);
        }

    }

    /**
     * Shutdown the send/event threads. This method should not be called
     * directly - rather it should be called as part of close operation. This
     * method is primarily here to allow the tests to verify disconnection
     * behavior.
     */
    public void disconnect() {
        LOG.debug("Disconnecting client for session: 0x{}", Long.toHexString(getSessionId()));

        sendThread.close();
        try {
            sendThread.join();
        } catch (InterruptedException ex) {
            LOG.warn("Got interrupted while waiting for the sender thread to close", ex);
        }
        eventThread.queueEventOfDeath();
        if (zooKeeperSaslClient != null) {
            zooKeeperSaslClient.shutdown();
        }
    }

    /**
     * Close the connection, which includes; send session disconnect to the
     * server, shutdown the send/event threads.
     *
     * @throws IOException
     */
    public void close() throws IOException {
        LOG.debug("Closing client for session: 0x{}", Long.toHexString(getSessionId()));

        try {
            RequestHeader h = new RequestHeader();
            h.setType(ZooDefs.OpCode.closeSession);

            submitRequest(h, null, null, null);
        } catch (InterruptedException e) {
            // ignore, close the send/event threads
        } finally {
            disconnect();
        }
    }

    // @VisibleForTesting
    protected int xid = 1;

    // @VisibleForTesting
    volatile States state = States.NOT_CONNECTED;//初始状态下处于未连接状态

    /*
     * getXid() is called externally by ClientCnxnNIO::doIO() when packets are sent from the outgoingQueue to
     * the server. Thus, getXid() must be public.
     */
    public synchronized int getXid() {
        // Avoid negative cxid values.  In particular, cxid values of -4, -2, and -1 are special and
        // must not be used for requests -- see SendThread.readResponse.
        // Skip from MAX to 1.
        if (xid == Integer.MAX_VALUE) {
            xid = 1;
        }
        return xid++;
    }

    public ReplyHeader submitRequest(
        RequestHeader h,
        Record request,
        Record response,
        WatchRegistration watchRegistration) throws InterruptedException {
        return submitRequest(h, request, response, watchRegistration, null);
    }

    public ReplyHeader submitRequest(
        RequestHeader h,
        Record request,
        Record response,
        WatchRegistration watchRegistration,
        WatchDeregistration watchDeregistration) throws InterruptedException {
        ReplyHeader r = new ReplyHeader();
        //这里 queuePacket() 方法是一个关键要点
        Packet packet = queuePacket(
            h,
            r,
            request,
            response,
            null,
            null,
            null,
            null,
            watchRegistration,
            watchDeregistration);
        synchronized (packet) {
            //这里有两种模式，一种是有超时等待时间的 Socket 发送 packet，当 requestTimeout 小于等于零时，没有超时时间
            if (requestTimeout > 0) {
                // Wait for request completion with timeout
                waitForPacketFinish(r, packet);
            } else {
                // Wait for request completion infinitely
                while (!packet.finished) {
                    packet.wait();
                }
            }
        }
        if (r.getErr() == Code.REQUESTTIMEOUT.intValue()) {
            sendThread.cleanAndNotifyState();
        }
        return r;
    }

    /**
     * Wait for request completion with timeout.
     */
    private void waitForPacketFinish(ReplyHeader r, Packet packet) throws InterruptedException {
        long waitStartTime = Time.currentElapsedTime();
        while (!packet.finished) {
            packet.wait(requestTimeout);
            if (!packet.finished && ((Time.currentElapsedTime() - waitStartTime) >= requestTimeout)) {
                LOG.error("Timeout error occurred for the packet '{}'.", packet);
                r.setErr(Code.REQUESTTIMEOUT.intValue());
                break;
            }
        }
    }

    public void saslCompleted() {
        sendThread.getClientCnxnSocket().saslCompleted();
    }

    public void sendPacket(Record request, Record response, AsyncCallback cb, int opCode) throws IOException {
        // Generate Xid now because it will be sent immediately,
        // by call to sendThread.sendPacket() below.
        int xid = getXid();
        RequestHeader h = new RequestHeader();
        h.setXid(xid);
        h.setType(opCode);

        ReplyHeader r = new ReplyHeader();
        r.setXid(xid);

        Packet p = new Packet(h, r, request, response, null, false);
        p.cb = cb;
        sendThread.sendPacket(p);
    }

    public Packet queuePacket(
        RequestHeader h,
        ReplyHeader r,
        Record request,
        Record response,
        AsyncCallback cb,
        String clientPath,
        String serverPath,
        Object ctx,
        WatchRegistration watchRegistration) {
        return queuePacket(h, r, request, response, cb, clientPath, serverPath, ctx, watchRegistration, null);
    }

    public Packet queuePacket(
        RequestHeader h,
        ReplyHeader r,
        Record request,
        Record response,
        AsyncCallback cb,
        String clientPath,
        String serverPath,
        Object ctx,
        WatchRegistration watchRegistration,
        WatchDeregistration watchDeregistration) {
        Packet packet = null;

        // Note that we do not generate the Xid for the packet yet. It is
        // generated later at send-time, by an implementation of ClientCnxnSocket::doIO(),
        // where the packet is actually sent.
        // 将请求头、返回头、请求内容、返回内容、WatchRegistration 等实例封装为一个 Packet 实例
        // 注意：虽然 Package 具有 Watcher 注册信息的，但 Package 在序列化时，并不会将 Watcher 一并进行序列化传输，
        // 而是选择将其排除在外，封装的意义在于通过 Package 能够找到 Watcher 的注册信息
        packet = new Packet(h, r, request, response, watchRegistration);
        packet.cb = cb;
        packet.ctx = ctx;
        packet.clientPath = clientPath;
        packet.serverPath = serverPath;
        packet.watchDeregistration = watchDeregistration;
        // The synchronized block here is for two purpose:
        // 1. synchronize with the final cleanup() in SendThread.run() to avoid race
        // 2. synchronized against each packet. So if a closeSession packet is added,
        // later packet will be notified.
        synchronized (state) {
            if (!state.isAlive() || closing) {
                conLossPacket(packet);
            } else {
                // If the client is asking to close the session then
                // mark as closing
                if (h.getType() == OpCode.closeSession) {
                    closing = true;
                }
                //将封装完毕的 Packet 添加到队列中
                outgoingQueue.add(packet);
            }
        }
        /**
         * 如果是 JDK NIO，那么此方法会负责调用 Selector.wakeup() 方法
         * NIO中的Selector封装了底层的系统调用，其中 Selector.wakeup() 用于唤醒阻塞在select方法上的线程
         */


        sendThread.getClientCnxnSocket().packetAdded();
        return packet;
    }

    public void addAuthInfo(String scheme, byte[] auth) {
        if (!state.isAlive()) {
            return;
        }
        authInfo.add(new AuthData(scheme, auth));
        queuePacket(
            new RequestHeader(ClientCnxn.AUTHPACKET_XID, OpCode.auth),
            null,
            new AuthPacket(0, scheme, auth),
            null,
            null,
            null,
            null,
            null,
            null);
    }

    States getState() {
        return state;
    }

    private static class LocalCallback {

        private final AsyncCallback cb;
        private final int rc;
        private final String path;
        private final Object ctx;

        public LocalCallback(AsyncCallback cb, int rc, String path, Object ctx) {
            this.cb = cb;
            this.rc = rc;
            this.path = path;
            this.ctx = ctx;
        }

    }

    private void initRequestTimeout() {
        try {
            requestTimeout = clientConfig.getLong(
                ZKClientConfig.ZOOKEEPER_REQUEST_TIMEOUT,
                ZKClientConfig.ZOOKEEPER_REQUEST_TIMEOUT_DEFAULT);
            LOG.info(
                "{} value is {}. feature enabled={}",
                ZKClientConfig.ZOOKEEPER_REQUEST_TIMEOUT,
                requestTimeout,
                requestTimeout > 0);
        } catch (NumberFormatException e) {
            LOG.error(
                "Configured value {} for property {} can not be parsed to long.",
                clientConfig.getProperty(ZKClientConfig.ZOOKEEPER_REQUEST_TIMEOUT),
                ZKClientConfig.ZOOKEEPER_REQUEST_TIMEOUT);
            throw e;
        }
    }

}
