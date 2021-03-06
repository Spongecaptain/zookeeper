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

package org.apache.zookeeper.server;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * NIOServerCnxnFactory implements a multi-threaded ServerCnxnFactory using
 * NIO non-blocking socket calls. Communication between threads is handled via
 * queues.
 *
 *   - 1   accept thread, which accepts new connections and assigns to a
 *         selector thread
 *   - 1-N selector threads, each of which selects on 1/N of the connections.
 *         The reason the factory supports more than one selector thread is that
 *         with large numbers of connections, select() itself can become a
 *         performance bottleneck.
 *   - 0-M socket I/O worker threads, which perform basic socket reads and
 *         writes. If configured with 0 worker threads, the selector threads
 *         do the socket I/O directly.
 *   - 1   connection expiration thread, which closes idle connections; this is
 *         necessary to expire connections on which no session is established.
 *
 * Typical (default) thread counts are: on a 32 core machine, 1 accept thread,
 * 1 connection expiration thread, 4 selector threads, and 64 worker threads.
 */

/**
 * 这是一个工厂类，内部管理着线程以及线程池
 * 线程以及线程池的规模由 ServerCnxnfactory 类的 configure() 等方法（多个重载方法）来决定，具体可以从这个方法入手来看规模是如何决定的
 * 线程池的初始化在 start() 方法中完成
 * 线程的初始化则在 configure() 方法中完成，启动在 start() 方法中完成
 * 需要注意的是：不同于 Tomcat 中的 per thread per request，ZooKeeper 与客户端的连接基于 NIO，用于与客户端通信的线程数是固定的。
 */
public class NIOServerCnxnFactory extends ServerCnxnFactory {

    private static final Logger LOG = LoggerFactory.getLogger(NIOServerCnxnFactory.class);

    /** Default sessionless connection timeout in ms: 10000 (10s) */
    public static final String ZOOKEEPER_NIO_SESSIONLESS_CNXN_TIMEOUT = "zookeeper.nio.sessionlessCnxnTimeout";
    /**
     * With 500 connections to an observer with watchers firing on each, is
     * unable to exceed 1GigE rates with only 1 selector.
     * Defaults to using 2 selector threads with 8 cores and 4 with 32 cores.
     * Expressed as sqrt(numCores/2). Must have at least 1 selector thread.
     */
    public static final String ZOOKEEPER_NIO_NUM_SELECTOR_THREADS = "zookeeper.nio.numSelectorThreads";
    /** Default: 2 * numCores */
    public static final String ZOOKEEPER_NIO_NUM_WORKER_THREADS = "zookeeper.nio.numWorkerThreads";
    /** Default: 64kB */
    public static final String ZOOKEEPER_NIO_DIRECT_BUFFER_BYTES = "zookeeper.nio.directBufferBytes";
    /** Default worker pool shutdown timeout in ms: 5000 (5s) */
    public static final String ZOOKEEPER_NIO_SHUTDOWN_TIMEOUT = "zookeeper.nio.shutdownTimeout";

    static {
        Thread.setDefaultUncaughtExceptionHandler((t, e) -> LOG.error("Thread {} died", t, e));

        /**
         * Value of 0 disables use of direct buffers and instead uses
         * gathered write call.
         *
         * Default to using 64k direct buffers.
         */
        directBufferBytes = Integer.getInteger(ZOOKEEPER_NIO_DIRECT_BUFFER_BYTES, 64 * 1024);
    }

    /**
     * AbstractSelectThread is an abstract base class containing a few bits
     * of code shared by the AcceptThread (which selects on the listen socket)
     * and SelectorThread (which selects on client connections) classes.
     */
    private abstract class AbstractSelectThread extends ZooKeeperThread {

        protected final Selector selector;

        public AbstractSelectThread(String name) throws IOException {
            super(name);
            // Allows the JVM to shutdown even if this thread is still running.
            setDaemon(true);
            this.selector = Selector.open();
        }

        public void wakeupSelector() {
            selector.wakeup();
        }

        /**
         * Close the selector. This should be called when the thread is about to
         * exit and no operation is going to be performed on the Selector or
         * SelectionKey
         */
        protected void closeSelector() {
            try {
                selector.close();
            } catch (IOException e) {
                LOG.warn("ignored exception during selector close.", e);
            }
        }

        protected void cleanupSelectionKey(SelectionKey key) {
            if (key != null) {
                try {
                    key.cancel();
                } catch (Exception ex) {
                    LOG.debug("ignoring exception during selectionkey cancel", ex);
                }
            }
        }

        protected void fastCloseSock(SocketChannel sc) {
            if (sc != null) {
                try {
                    // Hard close immediately, discarding buffers
                    sc.socket().setSoLinger(true, 0);
                } catch (SocketException e) {
                    LOG.warn("Unable to set socket linger to 0, socket close may stall in CLOSE_WAIT", e);
                }
                NIOServerCnxn.closeSock(sc);
            }
        }

    }

    /**
     * There is a single AcceptThread which accepts new connections and assigns
     * them to a SelectorThread using a simple round-robin scheme to spread
     * them across the SelectorThreads. It enforces maximum number of
     * connections per IP and attempts to cope with running out of file
     * descriptors by briefly sleeping before retrying.
     */

    /**
     * AcceptThead 没有重写 start() 方法，因此直接查看其 run() 方法即可，其 run() 方法的主要执行逻辑如下：
     * - 通过 Selector.select() 方法来查询是否有事件发生，在没有事件发生时阻塞，否则进行事件处理
     * - 非阻塞下判断 Selector 实例内部产生的事件类型，其仅仅处理新连接事件，对应 SelectionKey.isAcceptable() 方法返回 true
     * - 判断当前 ZooKeeper 服务端是否达到最大客户端连接数，如果有达到，那么拒绝新连接，否则接收新的客户端连接
     * - 将新连接对应的 SocketChannel 实例配置为非阻塞模式
     * - 从注册的 SelectorThread 中取出一个线程，通过 selectorThread.addAcceptedConnection(sc) 方法来使当前线程负责新连接（SocketChannel 的通信）
     * - 实际上，上述方法的内部执行逻辑是将 SocketChannel 加入到被选择的 SelectorThread 实例的队列中，等待 SelectorThread 来消费(负责其 I/O 逻辑)
     * -
     */
    private class AcceptThread extends AbstractSelectThread {

        private final ServerSocketChannel acceptSocket;
        private final SelectionKey acceptKey;
        private final RateLogger acceptErrorLogger = new RateLogger(LOG);
        private final Collection<SelectorThread> selectorThreads;//SelectorThread 实例的容器
        private Iterator<SelectorThread> selectorIterator;//SelectorThread 迭代器
        private volatile boolean reconfiguring = false;

        public AcceptThread(ServerSocketChannel ss, InetSocketAddress addr, Set<SelectorThread> selectorThreads) throws IOException {
            super("NIOServerCxnFactory.AcceptThread:" + addr);
            this.acceptSocket = ss;
            this.acceptKey = acceptSocket.register(selector, SelectionKey.OP_ACCEPT);
            this.selectorThreads = Collections.unmodifiableList(new ArrayList<SelectorThread>(selectorThreads));
            selectorIterator = this.selectorThreads.iterator();
        }
        //run() 方法的具体执行逻辑可以看 AcceptThread 类上的注释
        public void run() {
            try {
                //AcceptThread 类的 Main Loop
                //确保循环中服务没有关闭、ServerSocketChannel 没有关闭
                while (!stopped && !acceptSocket.socket().isClosed()) {
                    try {
                        select();//用于事件的监听
                    } catch (RuntimeException e) {
                        LOG.warn("Ignoring unexpected runtime exception", e);
                    } catch (Exception e) {
                        LOG.warn("Ignoring unexpected exception", e);
                    }
                }
            } finally {
                closeSelector();
                // This will wake up the selector threads, and tell the
                // worker thread pool to begin shutdown.
                if (!reconfiguring) {
                    NIOServerCnxnFactory.this.stop();
                }
                LOG.info("accept thread exitted run method");
            }
        }

        public void setReconfiguring() {
            reconfiguring = true;
        }

        private void select() {
            try {
                //在没有事件时阻塞
                selector.select();

                Iterator<SelectionKey> selectedKeys = selector.selectedKeys().iterator();
                while (!stopped && selectedKeys.hasNext()) {
                    SelectionKey key = selectedKeys.next();
                    selectedKeys.remove();

                    if (!key.isValid()) {
                        continue;
                    }
                    //SelectionKey.isAcceptable() 方法返回返回 true 时说明产生了新连接事件
                    if (key.isAcceptable()) {
                        //在 doAccept() 方法中处理新连接事件
                        if (!doAccept()) {
                            // If unable to pull a new connection off the accept
                            // queue, pause accepting to give us time to free
                            // up file descriptors and so the accept thread
                            // doesn't spin in a tight loop.
                            pauseAccept(10);
                        }
                    } else {
                        LOG.warn("Unexpected ops in accept select {}", key.readyOps());
                    }
                }
            } catch (IOException e) {
                LOG.warn("Ignoring IOException while selecting", e);
            }
        }

        /**
         * Mask off the listen socket interest ops and use select() to sleep
         * so that other threads can wake us up by calling wakeup() on the
         * selector.
         */
        private void pauseAccept(long millisecs) {
            acceptKey.interestOps(0);
            try {
                selector.select(millisecs);
            } catch (IOException e) {
                // ignore
            } finally {
                acceptKey.interestOps(SelectionKey.OP_ACCEPT);
            }
        }

        /**
         * Accept new socket connections. Enforces maximum number of connections
         * per client IP address. Round-robin assigns to selector thread for
         * handling. Returns whether pulled a connection off the accept queue
         * or not. If encounters an error attempts to fast close the socket.
         *
         * @return whether was able to accept a connection or not
         */
        private boolean doAccept() {
            boolean accepted = false;
            SocketChannel sc = null;
            try {
                //通过 ServerSocketChannel.accept() 方法得到 SocketChannel 实例
                sc = acceptSocket.accept();
                accepted = true;
                //检查客户端连接数是否超出配置 maxCnxns 值
                if (limitTotalNumberOfCnxns()) {
                    throw new IOException("Too many connections max allowed is " + maxCnxns);
                }
                InetAddress ia = sc.socket().getInetAddress();
                int cnxncount = getClientCnxnCount(ia);
                //检查是否超出 maxClientCnxns 的最大配置
                if (maxClientCnxns > 0 && cnxncount >= maxClientCnxns) {
                    throw new IOException("Too many connections from " + ia + " - max is " + maxClientCnxns);
                }

                LOG.debug("Accepted socket connection from {}", sc.socket().getRemoteSocketAddress());
                //配置为非阻塞
                sc.configureBlocking(false);

                // Round-robin assign this connection to a selector thread
                // 如果迭代器迭代到尾巴了，那么重新构造一个一模一样的迭代器（用于循环）
                if (!selectorIterator.hasNext()) {
                    selectorIterator = selectorThreads.iterator();
                }
                //从迭代器中取出一个 selectorThread 实例来负责此 SocketChannel 的通信
                SelectorThread selectorThread = selectorIterator.next();
                //将新连接对应的 SocketChannel 实例加入到 SelectorThread 内部的队列中
                if (!selectorThread.addAcceptedConnection(sc)) {
                    throw new IOException("Unable to add connection to selector queue"
                                          + (stopped ? " (shutdown in progress)" : ""));
                }
                acceptErrorLogger.flush();
            } catch (IOException e) {
                // accept, maxClientCnxns, configureBlocking
                ServerMetrics.getMetrics().CONNECTION_REJECTED.add(1);
                acceptErrorLogger.rateLimitLog("Error accepting new connection: " + e.getMessage());
                fastCloseSock(sc);
            }
            return accepted;
        }

    }

    /**
     * The SelectorThread receives newly accepted connections from the
     * AcceptThread and is responsible for selecting for I/O readiness
     * across the connections. This thread is the only thread that performs
     * any non-threadsafe or potentially blocking calls on the selector
     * (registering new connections and reading/writing interest ops).
     *
     * Assignment of a connection to a SelectorThread is permanent and only
     * one SelectorThread will ever interact with the connection. There are
     * 1-N SelectorThreads, with connections evenly apportioned between the
     * SelectorThreads.
     *
     * If there is a worker thread pool, when a connection has I/O to perform
     * the SelectorThread removes it from selection by clearing its interest
     * ops and schedules the I/O for processing by a worker thread. When the
     * work is complete, the connection is placed on the ready queue to have
     * its interest ops restored and resume selection.
     *
     * If there is no worker thread pool, the SelectorThread performs the I/O
     * directly.
     */

    /**
     * SelectorThread 线程也没有重写 Thread.start() 方法，因此其运行逻辑集中于 run() 方法上
     * SelectorThread.run() 方法的运行逻辑如下：
     * 1.第一部分工作是对 Selector 事件的处理
     * - 通过 `Selector.select()` 方法来查询是否有事件发生，在没有事件发生时阻塞，否则进行事件处理；
     * - 非阻塞下判断 Selector 实例内部产生的事件类型，其仅仅处理可读、可写事件，
     * 对应 `SelectionKey.isWritable()` 或者 `SelectionKey.isReadable()` 方法返回 true；
     * - 在可读可写事件发生后，进行 IO 逻辑的处理，IO 处理的步骤是：
     *   - 得到 SelectionKey 上作为附件存储的 NIOServerCnxn 实例
     *   - 将此读写 IO 事件对应的 SelectionKey 包装为一个 IOWorkRequest 实例，封装的主要意义在于 IOWorkRequest 可以被线程池处理
     *   - 在 IO 时将 SelectionKey 设置为对任何事件都不感兴趣，通过 `SelectionKey.interestOps(0)` 实现
     *   - 更新一下 NIOServerCnxn 实例的过期时间（每一个 NIOServerCnxn 有被 NIOServerCnxnFactory.cnxnExpiryQueue 阻塞队列存储）
     *   - 将封装结果 IOWorkRequest 实例交给线程池 NIOServerCnxnFactory.workerPool 来负责处理（也是异步的队列与线程）
     * 2.第二部分工作是对新注册的 SocketChannel 构造一个 NIOServerCnxn 实例与之匹配，并注册到 Selector 中
     * - 将从 AcceptThread 线程分配而来的 SocketChannel 实例（已存储于当前 SelectorThread 实例的 acceptedQueue 队列中）
     *   它们虽然已分配，但是还未注册到 当前 SelectorThread 实例的 Selector 实例中，这里进行注册
     *   注册的逻辑是：
     *      - 将 SocketChannel 实例注册到 Selector 中，并表示对可读事件感兴趣
     *      - 构造 NIOServerCnxn 实例，其接受 SocketChannel、SelectionKey 以及 SelectorThread 实例
     *      - 将 NIOServerCnxn 实例作为 SelectionKey 的附件，方便在产生可读事件时方便地获取到其一一对应的 NIOServerCnxn 实例
     *      - 注册 NIOServerCnxn 实例
     * 3.第三部分工作是对第一步中因为处理 IO 而暂停事件的 SocketChanenl 重新恢复为原来的兴趣
     *  能够恢复原兴趣的原因在于我们用 NIOServerCnxn 实例保留了原 SocketChanenl 的兴趣字段
     *
     *
     */
    class SelectorThread extends AbstractSelectThread {

        private final int id;
        private final Queue<SocketChannel> acceptedQueue;//用于存储从 AcceptThread 线程实例传来的 SocketChannel 实例
        private final Queue<SelectionKey> updateQueue;

        public SelectorThread(int id) throws IOException {
            super("NIOServerCxnFactory.SelectorThread-" + id);
            this.id = id;
            acceptedQueue = new LinkedBlockingQueue<SocketChannel>();
            updateQueue = new LinkedBlockingQueue<SelectionKey>();
        }

        /**
         * Place new accepted connection onto a queue for adding. Do this
         * so only the selector thread modifies what keys are registered
         * with the selector.
         */
        public boolean addAcceptedConnection(SocketChannel accepted) {
            if (stopped || !acceptedQueue.offer(accepted)) {
                return false;
            }
            wakeupSelector();
            return true;
        }

        /**
         * Place interest op update requests onto a queue so that only the
         * selector thread modifies interest ops, because interest ops
         * reads/sets are potentially blocking operations if other select
         * operations are happening.
         */
        public boolean addInterestOpsUpdateRequest(SelectionKey sk) {
            if (stopped || !updateQueue.offer(sk)) {
                return false;
            }
            wakeupSelector();
            return true;
        }

        /**
         * The main loop for the thread selects() on the connections and
         * dispatches ready I/O work requests, then registers all pending
         * newly accepted connections and updates any interest ops on the
         * queue.
         */
        public void run() {
            try {
                //这是 SelectorThread 的 Main Loop
                while (!stopped) {
                    try {
                        //注意：虽然 SelectorThread 与 AcceptThread 类都由 select() 方法，但它们的执行逻辑不同
                        //第一部分工作
                        select();
                        //第二部分工作，ZooKeeper 官方注释说的很明白，其任务是：
                        //将从 AcceptThread 线程分配而来的 SocketChannel 实例（已存储于当前 SelectorThread 实例的 acceptedQueue 队列中）
                        //它们虽然已分配，但是还未注册到 当前 SelectorThread 实例的 Selector 实例中，这里进行注册
                        processAcceptedConnections();
                        //第三部分工作是对第一步中因为处理 IO 而暂停事件的 SocketChanenl 重新设置为对读事件感兴趣
                        processInterestOpsUpdateRequests();
                    } catch (RuntimeException e) {
                        LOG.warn("Ignoring unexpected runtime exception", e);
                    } catch (Exception e) {
                        LOG.warn("Ignoring unexpected exception", e);
                    }
                }
                //下面的逻辑主要用于关闭相关资源，这里不属于重点
                // Close connections still pending on the selector. Any others
                // with in-flight work, let drain out of the work queue.
                for (SelectionKey key : selector.keys()) {
                    NIOServerCnxn cnxn = (NIOServerCnxn) key.attachment();
                    if (cnxn.isSelectable()) {
                        cnxn.close(ServerCnxn.DisconnectReason.SERVER_SHUTDOWN);
                    }
                    cleanupSelectionKey(key);
                }
                SocketChannel accepted;
                while ((accepted = acceptedQueue.poll()) != null) {
                    fastCloseSock(accepted);
                }
                updateQueue.clear();
            } finally {
                closeSelector();
                // This will wake up the accept thread and the other selector
                // threads, and tell the worker thread pool to begin shutdown.
                NIOServerCnxnFactory.this.stop();
                LOG.info("selector thread exitted run method");
            }
        }

        private void select() {
            try {
                //在 Selector 实例没有事件时阻塞
                selector.select();
                Set<SelectionKey> selected = selector.selectedKeys();
                ArrayList<SelectionKey> selectedList = new ArrayList<SelectionKey>(selected);
                //这里重排序 SelectionKey 的顺序
                Collections.shuffle(selectedList);
                Iterator<SelectionKey> selectedKeys = selectedList.iterator();
                while (!stopped && selectedKeys.hasNext()) {
                    SelectionKey key = selectedKeys.next();
                    selected.remove(key);

                    if (!key.isValid()) {
                        cleanupSelectionKey(key);
                        continue;
                    }
                    //当发生可读可写事件时，进行读写 IO
                    if (key.isReadable() || key.isWritable()) {
                        handleIO(key);
                    } else {
                        LOG.warn("Unexpected ops in select {}", key.readyOps());
                    }
                }
            } catch (IOException e) {
                LOG.warn("Ignoring IOException while selecting", e);
            }
        }

        /**
         * Schedule I/O for processing on the connection associated with
         * the given SelectionKey. If a worker thread pool is not being used,
         * I/O is run directly by this thread.
         */
        private void  handleIO(SelectionKey key) {
            //将此读写 IO 事件对应的 SelectionKey 包装为一个 IOWorkRequest 实例，封装的主要意义在于 IOWorkRequest 可以被线程池处理
            IOWorkRequest workRequest = new IOWorkRequest(this, key);
            //得到 SelectionKey 上作为附件存储的 NIOServerCnxn 实例
            NIOServerCnxn cnxn = (NIOServerCnxn) key.attachment();

            // Stop selecting this key while processing on its
            // connection
            cnxn.disableSelectable();
            //将 SelectionKey 设置为对任何事件都不感兴趣
            key.interestOps(0);
            //更新一下连接的过期时间
            touchCnxn(cnxn);
            //将 IOWorkRequest 实例交给线程池来处理
            workerPool.schedule(workRequest);
        }

        /**
         * Iterate over the queue of accepted connections that have been
         * assigned to this thread but not yet placed on the selector.
         */
        private void processAcceptedConnections() {
            SocketChannel accepted;
            while (!stopped && (accepted = acceptedQueue.poll()) != null) {
                SelectionKey key = null;
                try {
                    //将 SocketChannel 实例注册到 Selector 中，并表示对可读事件感兴趣
                    key = accepted.register(selector, SelectionKey.OP_READ);
                    NIOServerCnxn cnxn = createConnection(accepted, key, this);
                    //将 NIOServerCnxn 实例作为 SelectionKey 的附件，方便在产生可读事件时方便地获取到其一一对应的 NIOServerCnxn 实例
                    key.attach(cnxn);
                    //注册 NIOServerCnxn 实例
                    addCnxn(cnxn);
                } catch (IOException e) {
                    // register, createConnection
                    cleanupSelectionKey(key);
                    fastCloseSock(accepted);
                }
            }
        }

        /**
         * Iterate over the queue of connections ready to resume selection,
         * and restore their interest ops selection mask.
         */
        private void processInterestOpsUpdateRequests() {
            SelectionKey key;
            while (!stopped && (key = updateQueue.poll()) != null) {
                if (!key.isValid()) {
                    cleanupSelectionKey(key);
                }
                NIOServerCnxn cnxn = (NIOServerCnxn) key.attachment();//可以注意到，ServerCnxn 作为 SelectionKey 实例的附件而存在
                if (cnxn.isSelectable()) {
                    key.interestOps(cnxn.getInterestOps());
                }
            }
        }

    }

    /**
     * IOWorkRequest is a small wrapper class to allow doIO() calls to be
     * run on a connection using a WorkerService.
     */
    private class IOWorkRequest extends WorkerService.WorkRequest {

        private final SelectorThread selectorThread;
        private final SelectionKey key;
        private final NIOServerCnxn cnxn;

        IOWorkRequest(SelectorThread selectorThread, SelectionKey key) {
            this.selectorThread = selectorThread;
            this.key = key;
            this.cnxn = (NIOServerCnxn) key.attachment();
        }

        public void doWork() throws InterruptedException {
            //检查有效性
            if (!key.isValid()) {
                selectorThread.cleanupSelectionKey(key);
                return;
            }
            //检查是否为可读可写事件
            if (key.isReadable() || key.isWritable()) {
                //调用 NIOServerCnxn#doIO() 方法来完成 IO 事件的处理
                cnxn.doIO(key);

                // Check if we shutdown or doIO() closed this connection
                if (stopped) {
                    cnxn.close(ServerCnxn.DisconnectReason.SERVER_SHUTDOWN);
                    return;
                }
                if (!key.isValid()) {
                    selectorThread.cleanupSelectionKey(key);
                    return;
                }
                //更新该 NIOServerCnxn 实例的过期时间，避免被清理
                touchCnxn(cnxn);
            }

            // Mark this connection as once again ready for selection
            cnxn.enableSelectable();
            // Push an update request on the queue to resume selecting
            // on the current set of interest ops, which may have changed
            // as a result of the I/O operations we just performed.
            if (!selectorThread.addInterestOpsUpdateRequest(key)) {
                cnxn.close(ServerCnxn.DisconnectReason.CONNECTION_MODE_CHANGED);
            }
        }

        @Override
        public void cleanup() {
            cnxn.close(ServerCnxn.DisconnectReason.CLEAN_UP);
        }

    }

    /**
     * This thread is responsible for closing stale connections so that
     * connections on which no session is established are properly expired.
     */

    /**
     * ConnectionExpirerThread 线程同样没有没有重写 start() 方法，因此直接分析 run() 方法即可
     * 其用于关闭哪些 Session 已经过期了的连接
     * 注意，在 SelectorThread 线程中，每次有收到新的读事件，都会更新 cnxnExpiryQueue 队列中对应 ServerCnxn 实例的过期时间
     * 其执行逻辑为：
     * - 得到队列中所有元素的最短过期时间
     * - 如果最短过期时间未到，那么线程按照相应的截止时间进行线程休眠
     * - 线程阻塞后利用 poll() 方法拿到已经过期的 NIOServerCnxn 实例，然后进行关闭
     */
    private class ConnectionExpirerThread extends ZooKeeperThread {

        ConnectionExpirerThread() {
            super("ConnnectionExpirer");
        }

        public void run() {
            try {
                while (!stopped) {
                    //得到队列中所有元素的最短过期时间
                    long waitTime = cnxnExpiryQueue.getWaitTime();
                    //如果最短过期时间未到，那么线程按照相应的截止时间进行休眠
                    if (waitTime > 0) {
                        Thread.sleep(waitTime);
                        continue;
                    }
                    //注意这个 poll() 只会拿到已经过期的 NIOServerCnxn 实例，然后进行关闭
                    for (NIOServerCnxn conn : cnxnExpiryQueue.poll()) {
                        ServerMetrics.getMetrics().SESSIONLESS_CONNECTIONS_EXPIRED.add(1);
                        conn.close(ServerCnxn.DisconnectReason.CONNECTION_EXPIRED);
                    }
                }

            } catch (InterruptedException e) {
                LOG.info("ConnnectionExpirerThread interrupted");
            }
        }

    }

    ServerSocketChannel ss;

    /**
     * We use this buffer to do efficient socket I/O. Because I/O is handled
     * by the worker threads (or the selector threads directly, if no worker
     * thread pool is created), we can create a fixed set of these to be
     * shared by connections.
     */
    private static final ThreadLocal<ByteBuffer> directBuffer = new ThreadLocal<ByteBuffer>() {
        @Override
        protected ByteBuffer initialValue() {
            return ByteBuffer.allocateDirect(directBufferBytes);
        }
    };

    public static ByteBuffer getDirectBuffer() {
        return directBufferBytes > 0 ? directBuffer.get() : null;
    }

    // ipMap is used to limit connections per IP
    private final ConcurrentHashMap<InetAddress, Set<NIOServerCnxn>> ipMap = new ConcurrentHashMap<InetAddress, Set<NIOServerCnxn>>();

    protected int maxClientCnxns = 60;
    int listenBacklog = -1;

    int sessionlessCnxnTimeout;
    private ExpiryQueue<NIOServerCnxn> cnxnExpiryQueue;

    protected WorkerService workerPool;

    private static int directBufferBytes;
    private int numSelectorThreads;//2*CPU 核心数
    private int numWorkerThreads;//max(√(CPU 核心数/2),1)
    private long workerShutdownTimeoutMS;

    /**
     * Construct a new server connection factory which will accept an unlimited number
     * of concurrent connections from each client (up to the file descriptor
     * limits of the operating system). startup(zks) must be called subsequently.
     */
    public NIOServerCnxnFactory() {
    }

    private volatile boolean stopped = true;
    private ConnectionExpirerThread expirerThread;
    private AcceptThread acceptThread;
    private final Set<SelectorThread> selectorThreads = new HashSet<SelectorThread>();//SelectorThread 线程容器

    @Override
    public void configure(InetSocketAddress addr, int maxcc, int backlog, boolean secure) throws IOException {
        if (secure) {
            throw new UnsupportedOperationException("SSL isn't supported in NIOServerCnxn");
        }
        configureSaslLogin();

        maxClientCnxns = maxcc;
        initMaxCnxns();
        sessionlessCnxnTimeout = Integer.getInteger(ZOOKEEPER_NIO_SESSIONLESS_CNXN_TIMEOUT, 10000);
        // We also use the sessionlessCnxnTimeout as expiring interval for
        // cnxnExpiryQueue. These don't need to be the same, but the expiring
        // interval passed into the ExpiryQueue() constructor below should be
        // less than or equal to the timeout.
        cnxnExpiryQueue = new ExpiryQueue<NIOServerCnxn>(sessionlessCnxnTimeout);
        expirerThread = new ConnectionExpirerThread();

        int numCores = Runtime.getRuntime().availableProcessors();//得到电脑 CPU 的核心数
        // 32 cores sweet spot seems to be 4 selector threads
        //得到当前机器下最大且最合适的 SelectorThread 线程数，其值总是小于等于核心数，例如在我 8 核心数的机器上就是 2 个线程
        numSelectorThreads = Integer.getInteger(
            ZOOKEEPER_NIO_NUM_SELECTOR_THREADS,
            Math.max((int) Math.sqrt((float) numCores / 2), 1));
        if (numSelectorThreads < 1) {
            throw new IOException("numSelectorThreads must be at least 1");
        }
        //WorkerThread 大小默认为当前机器 CPU 核心数的 2 倍，例如在我 8 核心数的机器上就是 16 个线程
        numWorkerThreads = Integer.getInteger(ZOOKEEPER_NIO_NUM_WORKER_THREADS, 2 * numCores);
        workerShutdownTimeoutMS = Long.getLong(ZOOKEEPER_NIO_SHUTDOWN_TIMEOUT, 5000);

        String logMsg = "Configuring NIO connection handler with "
            + (sessionlessCnxnTimeout / 1000) + "s sessionless connection timeout, "
            + numSelectorThreads + " selector thread(s), "
            + (numWorkerThreads > 0 ? numWorkerThreads : "no") + " worker threads, and "
            + (directBufferBytes == 0 ? "gathered writes." : ("" + (directBufferBytes / 1024) + " kB direct buffers."));
        LOG.info(logMsg);
        //构造 SelectorThread 线程实例，然后将这些实例加入 HashSet 容器
        for (int i = 0; i < numSelectorThreads; ++i) {
            selectorThreads.add(new SelectorThread(i));
        }

        listenBacklog = backlog;
        //构造 ServerSocketChannel 实例用于服务器的端口监听
        this.ss = ServerSocketChannel.open();
        ss.socket().setReuseAddress(true);
        LOG.info("binding to port {}", addr);
        if (listenBacklog == -1) {
            // ServerSocketChannel 实例绑定端口
            ss.socket().bind(addr);
        } else {
            ss.socket().bind(addr, listenBacklog);
        }
        // 设置 ServerSocketChannel 为异步模式
        ss.configureBlocking(false);
        // 构造 AcceptThread 实例，其内部封装了 selectorThreads 个数大小的 SelectorThread 线程
        acceptThread = new AcceptThread(ss, addr, selectorThreads);
    }

    private void tryClose(ServerSocketChannel s) {
        try {
            s.close();
        } catch (IOException sse) {
            LOG.error("Error while closing server socket.", sse);
        }
    }

    @Override
    public void reconfigure(InetSocketAddress addr) {
        ServerSocketChannel oldSS = ss;
        try {
            acceptThread.setReconfiguring();
            tryClose(oldSS);
            acceptThread.wakeupSelector();
            try {
                acceptThread.join();
            } catch (InterruptedException e) {
                LOG.error("Error joining old acceptThread when reconfiguring client port.", e);
                Thread.currentThread().interrupt();
            }
            this.ss = ServerSocketChannel.open();
            ss.socket().setReuseAddress(true);
            LOG.info("binding to port {}", addr);
            ss.socket().bind(addr);
            ss.configureBlocking(false);
            acceptThread = new AcceptThread(ss, addr, selectorThreads);
            acceptThread.start();
        } catch (IOException e) {
            LOG.error("Error reconfiguring client port to {}", addr, e);
            tryClose(oldSS);
        }
    }

    /** {@inheritDoc} */
    public int getMaxClientCnxnsPerHost() {
        return maxClientCnxns;
    }

    /** {@inheritDoc} */
    public void setMaxClientCnxnsPerHost(int max) {
        maxClientCnxns = max;
    }

    /** {@inheritDoc} */
    public int getSocketListenBacklog() {
        return listenBacklog;
    }

    /**
     * 注意：先调用 configure() 方法，再调用 start() 方法
     * start() 方法用于：
     *  1. 构造指定大小的线程池 WorkerService 实例
     *  2. 启动在 configure() 方法中构造的若干 SelectorThread 线程实例
     *  3. 启动 AcceptThread 线程
     *  4. 启动 ConnectionExpirerThread 线程
     */
    @Override
    public void start() {
        stopped = false;
        //构造线程池实例，大小为 numWorkerThreads 即 max(√(CPU 核心数/2),1)
        if (workerPool == null) {
            workerPool = new WorkerService("NIOWorker", numWorkerThreads, false);
        }
        //对在 configure() 方法中构造的若干 SelectorThread 实例通过 Thread.start() 启动
        for (SelectorThread thread : selectorThreads) {
            if (thread.getState() == Thread.State.NEW) {
                thread.start();
            }
        }
        //启动 AcceptThread 线程
        // ensure thread is started once and only once
        if (acceptThread.getState() == Thread.State.NEW) {
            acceptThread.start();
        }
        //启动 ConnectionExpirerThread 线程
        if (expirerThread.getState() == Thread.State.NEW) {
            expirerThread.start();
        }
    }

    @Override
    public void startup(ZooKeeperServer zks, boolean startServer) throws IOException, InterruptedException {
        //1. 启动 NIOServerCnxnFactory 实例
        start();
        setZooKeeperServer(zks);
        if (startServer) {
            //启动持久化逻辑
            zks.startdata();
            //启动 ZooKeeperServer
            zks.startup();
        }
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return (InetSocketAddress) ss.socket().getLocalSocketAddress();
    }

    @Override
    public int getLocalPort() {
        return ss.socket().getLocalPort();
    }

    /**
     * De-registers the connection from the various mappings maintained
     * by the factory.
     */
    public boolean removeCnxn(NIOServerCnxn cnxn) {
        // If the connection is not in the master list it's already been closed
        if (!cnxns.remove(cnxn)) {
            return false;
        }
        cnxnExpiryQueue.remove(cnxn);

        removeCnxnFromSessionMap(cnxn);

        InetAddress addr = cnxn.getSocketAddress();
        if (addr != null) {
            Set<NIOServerCnxn> set = ipMap.get(addr);
            if (set != null) {
                set.remove(cnxn);
                // Note that we make no effort here to remove empty mappings
                // from ipMap.
            }
        }

        // unregister from JMX
        unregisterConnection(cnxn);
        return true;
    }

    /**
     * Add or update cnxn in our cnxnExpiryQueue
     * @param cnxn
     */
    public void touchCnxn(NIOServerCnxn cnxn) {
        cnxnExpiryQueue.update(cnxn, cnxn.getSessionTimeout());
    }

    private void addCnxn(NIOServerCnxn cnxn) throws IOException {
        InetAddress addr = cnxn.getSocketAddress();
        if (addr == null) {
            throw new IOException("Socket of " + cnxn + " has been closed");
        }
        Set<NIOServerCnxn> set = ipMap.get(addr);
        if (set == null) {
            // in general we will see 1 connection from each
            // host, setting the initial cap to 2 allows us
            // to minimize mem usage in the common case
            // of 1 entry --  we need to set the initial cap
            // to 2 to avoid rehash when the first entry is added
            // Construct a ConcurrentHashSet using a ConcurrentHashMap
            set = Collections.newSetFromMap(new ConcurrentHashMap<NIOServerCnxn, Boolean>(2));
            // Put the new set in the map, but only if another thread
            // hasn't beaten us to it
            Set<NIOServerCnxn> existingSet = ipMap.putIfAbsent(addr, set);
            if (existingSet != null) {
                set = existingSet;
            }
        }
        set.add(cnxn);

        cnxns.add(cnxn);
        touchCnxn(cnxn);
    }

    protected NIOServerCnxn createConnection(SocketChannel sock, SelectionKey sk, SelectorThread selectorThread) throws IOException {
        return new NIOServerCnxn(zkServer, sock, sk, this, selectorThread);
    }

    private int getClientCnxnCount(InetAddress cl) {
        Set<NIOServerCnxn> s = ipMap.get(cl);
        if (s == null) {
            return 0;
        }
        return s.size();
    }

    /**
     * clear all the connections in the selector
     *
     */
    @Override
    @SuppressWarnings("unchecked")
    public void closeAll(ServerCnxn.DisconnectReason reason) {
        // clear all the connections on which we are selecting
        for (ServerCnxn cnxn : cnxns) {
            try {
                // This will remove the cnxn from cnxns
                cnxn.close(reason);
            } catch (Exception e) {
                LOG.warn(
                    "Ignoring exception closing cnxn session id 0x{}",
                    Long.toHexString(cnxn.getSessionId()),
                    e);
            }
        }
    }

    public void stop() {
        stopped = true;

        // Stop queuing connection attempts
        try {
            ss.close();
        } catch (IOException e) {
            LOG.warn("Error closing listen socket", e);
        }

        if (acceptThread != null) {
            if (acceptThread.isAlive()) {
                acceptThread.wakeupSelector();
            } else {
                acceptThread.closeSelector();
            }
        }
        if (expirerThread != null) {
            expirerThread.interrupt();
        }
        for (SelectorThread thread : selectorThreads) {
            if (thread.isAlive()) {
                thread.wakeupSelector();
            } else {
                thread.closeSelector();
            }
        }
        if (workerPool != null) {
            workerPool.stop();
        }
    }

    public void shutdown() {
        try {
            // close listen socket and signal selector threads to stop
            stop();

            // wait for selector and worker threads to shutdown
            join();

            // close all open connections
            closeAll(ServerCnxn.DisconnectReason.SERVER_SHUTDOWN);

            if (login != null) {
                login.shutdown();
            }
        } catch (InterruptedException e) {
            LOG.warn("Ignoring interrupted exception during shutdown", e);
        } catch (Exception e) {
            LOG.warn("Ignoring unexpected exception during shutdown", e);
        }

        if (zkServer != null) {
            zkServer.shutdown();
        }
    }

    @Override
    public void join() throws InterruptedException {
        if (acceptThread != null) {
            acceptThread.join();
        }
        for (SelectorThread thread : selectorThreads) {
            thread.join();
        }
        if (workerPool != null) {
            workerPool.join(workerShutdownTimeoutMS);
        }
    }

    @Override
    public Iterable<ServerCnxn> getConnections() {
        return cnxns;
    }

    public void dumpConnections(PrintWriter pwriter) {
        pwriter.print("Connections ");
        cnxnExpiryQueue.dump(pwriter);
    }

    @Override
    public void resetAllConnectionStats() {
        // No need to synchronize since cnxns is backed by a ConcurrentHashMap
        for (ServerCnxn c : cnxns) {
            c.resetStats();
        }
    }

    @Override
    public Iterable<Map<String, Object>> getAllConnectionInfo(boolean brief) {
        HashSet<Map<String, Object>> info = new HashSet<Map<String, Object>>();
        // No need to synchronize since cnxns is backed by a ConcurrentHashMap
        for (ServerCnxn c : cnxns) {
            info.add(c.getConnectionInfo(brief));
        }
        return info;
    }

}
