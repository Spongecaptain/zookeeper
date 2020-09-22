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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import javax.security.sasl.SaslException;
import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.ServerMetrics;
import org.apache.zookeeper.server.TxnLogProposalIterator;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.ZooKeeperThread;
import org.apache.zookeeper.server.ZooTrace;
import org.apache.zookeeper.server.quorum.Leader.Proposal;
import org.apache.zookeeper.server.quorum.QuorumPeer.LearnerType;
import org.apache.zookeeper.server.quorum.auth.QuorumAuthServer;
import org.apache.zookeeper.server.util.MessageTracker;
import org.apache.zookeeper.server.util.ZxidUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * There will be an instance of this class created by the Leader for each
 * learner. All communication with a learner is handled by this
 * class.
 */
public class LearnerHandler extends ZooKeeperThread {

    private static final Logger LOG = LoggerFactory.getLogger(LearnerHandler.class);

    protected final Socket sock;

    public Socket getSocket() {
        return sock;
    }

    final LearnerMaster learnerMaster;

    /** Deadline for receiving the next ack. If we are bootstrapping then
     * it's based on the initLimit, if we are done bootstrapping it's based
     * on the syncLimit. Once the deadline is past this learner should
     * be considered no longer "sync'd" with the leader. */
    volatile long tickOfNextAckDeadline;

    /**
     * ZooKeeper server identifier of this learner
     */
    protected long sid = 0;

    long getSid() {
        return sid;
    }

    String getRemoteAddress() {
        return sock == null ? "<null>" : sock.getRemoteSocketAddress().toString();
    }

    protected int version = 0x1;

    int getVersion() {
        return version;
    }

    /**
     * The packets to be sent to the learner
     */
    final LinkedBlockingQueue<QuorumPacket> queuedPackets = new LinkedBlockingQueue<QuorumPacket>();
    private final AtomicLong queuedPacketsSize = new AtomicLong();

    protected final AtomicLong packetsReceived = new AtomicLong();
    protected final AtomicLong packetsSent = new AtomicLong();

    protected final AtomicLong requestsReceived = new AtomicLong();

    protected volatile long lastZxid = -1;

    public synchronized long getLastZxid() {
        return lastZxid;
    }

    protected final Date established = new Date();

    public Date getEstablished() {
        return (Date) established.clone();
    }

    /**
     * Marker packets would be added to quorum packet queue after every
     * markerPacketInterval packets.
     * It is ok if packetCounter overflows.
     */
    private final int markerPacketInterval = 1000;
    private AtomicInteger packetCounter = new AtomicInteger();

    /**
     * This class controls the time that the Leader has been
     * waiting for acknowledgement of a proposal from this Learner.
     * If the time is above syncLimit, the connection will be closed.
     * It keeps track of only one proposal at a time, when the ACK for
     * that proposal arrives, it switches to the last proposal received
     * or clears the value if there is no pending proposal.
     */
    private class SyncLimitCheck {

        private boolean started = false;
        private long currentZxid = 0;
        private long currentTime = 0;
        private long nextZxid = 0;
        private long nextTime = 0;

        public synchronized void start() {
            started = true;
        }

        public synchronized void updateProposal(long zxid, long time) {
            if (!started) {
                return;
            }
            if (currentTime == 0) {
                currentTime = time;
                currentZxid = zxid;
            } else {
                nextTime = time;
                nextZxid = zxid;
            }
        }

        public synchronized void updateAck(long zxid) {
            if (currentZxid == zxid) {
                currentTime = nextTime;
                currentZxid = nextZxid;
                nextTime = 0;
                nextZxid = 0;
            } else if (nextZxid == zxid) {
                LOG.warn(
                    "ACK for 0x{} received before ACK for 0x{}",
                    Long.toHexString(zxid),
                    Long.toHexString(currentZxid));
                nextTime = 0;
                nextZxid = 0;
            }
        }

        public synchronized boolean check(long time) {
            if (currentTime == 0) {
                return true;
            } else {
                long msDelay = (time - currentTime) / 1000000;
                return (msDelay < learnerMaster.syncTimeout());
            }
        }

    }

    private SyncLimitCheck syncLimitCheck = new SyncLimitCheck();

    private static class MarkerQuorumPacket extends QuorumPacket {

        long time;
        MarkerQuorumPacket(long time) {
            this.time = time;
        }

        @Override
        public int hashCode() {
            return Objects.hash(time);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            MarkerQuorumPacket that = (MarkerQuorumPacket) o;
            return time == that.time;
        }

    }

    private BinaryInputArchive ia;

    private BinaryOutputArchive oa;

    private final BufferedInputStream bufferedInput;
    private BufferedOutputStream bufferedOutput;

    protected final MessageTracker messageTracker;

    // for test only
    protected void setOutputArchive(BinaryOutputArchive oa) {
        this.oa = oa;
    }
    protected void setBufferedOutput(BufferedOutputStream bufferedOutput) {
        this.bufferedOutput = bufferedOutput;
    }

    /**
     * Keep track of whether we have started send packets thread
     */
    private volatile boolean sendingThreadStarted = false;

    /**
     * For testing purpose, force learnerMaster to use snapshot to sync with followers
     */
    public static final String FORCE_SNAP_SYNC = "zookeeper.forceSnapshotSync";
    private boolean forceSnapSync = false;

    /**
     * Keep track of whether we need to queue TRUNC or DIFF into packet queue
     * that we are going to blast it to the learner
     */
    private boolean needOpPacket = true;

    /**
     * Last zxid sent to the learner as part of synchronization
     */
    private long leaderLastZxid;

    /**
     * for sync throttling
     */
    private LearnerSyncThrottler syncThrottler = null;

    LearnerHandler(Socket sock, BufferedInputStream bufferedInput, LearnerMaster learnerMaster) throws IOException {
        super("LearnerHandler-" + sock.getRemoteSocketAddress());
        this.sock = sock;
        this.learnerMaster = learnerMaster;
        this.bufferedInput = bufferedInput;

        if (Boolean.getBoolean(FORCE_SNAP_SYNC)) {
            forceSnapSync = true;
            LOG.info("Forcing snapshot sync is enabled");
        }

        try {
            QuorumAuthServer authServer = learnerMaster.getQuorumAuthServer();
            if (authServer != null) {
                authServer.authenticate(sock, new DataInputStream(bufferedInput));
            }
        } catch (IOException e) {
            LOG.error("Server failed to authenticate quorum learner, addr: {}, closing connection", sock.getRemoteSocketAddress(), e);
            try {
                sock.close();
            } catch (IOException ie) {
                LOG.error("Exception while closing socket", ie);
            }
            throw new SaslException("Authentication failure: " + e.getMessage());
        }

        this.messageTracker = new MessageTracker(MessageTracker.BUFFERED_MESSAGE_SIZE);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("LearnerHandler ").append(sock);
        sb.append(" tickOfNextAckDeadline:").append(tickOfNextAckDeadline());
        sb.append(" synced?:").append(synced());
        sb.append(" queuedPacketLength:").append(queuedPackets.size());
        return sb.toString();
    }

    /**
     * If this packet is queued, the sender thread will exit
     */
    final QuorumPacket proposalOfDeath = new QuorumPacket();

    private LearnerType learnerType = LearnerType.PARTICIPANT;
    public LearnerType getLearnerType() {
        return learnerType;
    }

    /**
     * This method will use the thread to send packets added to the
     * queuedPackets list
     *
     * @throws InterruptedException
     */
    private void sendPackets() throws InterruptedException {
        long traceMask = ZooTrace.SERVER_PACKET_TRACE_MASK;
        while (true) {
            try {
                QuorumPacket p;
                p = queuedPackets.poll();
                if (p == null) {
                    bufferedOutput.flush();
                    p = queuedPackets.take();
                }

                ServerMetrics.getMetrics().LEARNER_HANDLER_QP_SIZE.add(Long.toString(this.sid), queuedPackets.size());

                if (p instanceof MarkerQuorumPacket) {
                    MarkerQuorumPacket m = (MarkerQuorumPacket) p;
                    ServerMetrics.getMetrics().LEARNER_HANDLER_QP_TIME
                        .add(Long.toString(this.sid), (System.nanoTime() - m.time) / 1000000L);
                    continue;
                }

                queuedPacketsSize.addAndGet(-packetSize(p));
                if (p == proposalOfDeath) {
                    // Packet of death!
                    break;
                }
                if (p.getType() == Leader.PING) {
                    traceMask = ZooTrace.SERVER_PING_TRACE_MASK;
                }
                if (p.getType() == Leader.PROPOSAL) {
                    syncLimitCheck.updateProposal(p.getZxid(), System.nanoTime());
                }
                if (LOG.isTraceEnabled()) {
                    ZooTrace.logQuorumPacket(LOG, traceMask, 'o', p);
                }

                // Log the zxid of the last request, if it is a valid zxid.
                if (p.getZxid() > 0) {
                    lastZxid = p.getZxid();
                }
                oa.writeRecord(p, "packet");
                packetsSent.incrementAndGet();
                messageTracker.trackSent(p.getType());
            } catch (IOException e) {
                if (!sock.isClosed()) {
                    LOG.warn("Unexpected exception at {}", this, e);
                    try {
                        // this will cause everything to shutdown on
                        // this learner handler and will help notify
                        // the learner/observer instantaneously
                        sock.close();
                    } catch (IOException ie) {
                        LOG.warn("Error closing socket for handler {}", this, ie);
                    }
                }
                break;
            }
        }
    }

    public static String packetToString(QuorumPacket p) {
        String type;
        String mess = null;

        switch (p.getType()) {
        case Leader.ACK:
            type = "ACK";
            break;
        case Leader.COMMIT:
            type = "COMMIT";
            break;
        case Leader.FOLLOWERINFO:
            type = "FOLLOWERINFO";
            break;
        case Leader.NEWLEADER:
            type = "NEWLEADER";
            break;
        case Leader.PING:
            type = "PING";
            break;
        case Leader.PROPOSAL:
            type = "PROPOSAL";
            break;
        case Leader.REQUEST:
            type = "REQUEST";
            break;
        case Leader.REVALIDATE:
            type = "REVALIDATE";
            ByteArrayInputStream bis = new ByteArrayInputStream(p.getData());
            DataInputStream dis = new DataInputStream(bis);
            try {
                long id = dis.readLong();
                mess = " sessionid = " + id;
            } catch (IOException e) {
                LOG.warn("Unexpected exception", e);
            }

            break;
        case Leader.UPTODATE:
            type = "UPTODATE";
            break;
        case Leader.DIFF:
            type = "DIFF";
            break;
        case Leader.TRUNC:
            type = "TRUNC";
            break;
        case Leader.SNAP:
            type = "SNAP";
            break;
        case Leader.ACKEPOCH:
            type = "ACKEPOCH";
            break;
        case Leader.SYNC:
            type = "SYNC";
            break;
        case Leader.INFORM:
            type = "INFORM";
            break;
        case Leader.COMMITANDACTIVATE:
            type = "COMMITANDACTIVATE";
            break;
        case Leader.INFORMANDACTIVATE:
            type = "INFORMANDACTIVATE";
            break;
        default:
            type = "UNKNOWN" + p.getType();
        }
        String entry = null;
        if (type != null) {
            entry = type + " " + Long.toHexString(p.getZxid()) + " " + mess;
        }
        return entry;
    }

    /**
     * This thread will receive packets from the peer and process them and
     * also listen to new connections from new peers.
     */
    @Override
    public void run() {
        try {
            //将 LearnerHandler 注册到 LearnerMaster 实例中（最常见的类型为 Leader）
            learnerMaster.addLearnerHandler(this);
            //初始化一下 ack deadline
            tickOfNextAckDeadline = learnerMaster.getTickOfInitialAckDeadline();
            // 因为 ZooKeeper 基于一个不常见的 Jute 序列化框架，其利用
            // BinaryInputArchive 以及 BinaryOutputArchive 来完成反序列化与序列化任务
            // 这里是对这两个字段 ia 与 oa 进行初始化
            ia = BinaryInputArchive.getArchive(bufferedInput);
            bufferedOutput = new BufferedOutputStream(sock.getOutputStream());
            oa = BinaryOutputArchive.getArchive(bufferedOutput);
            //构造一个 QuorumPacket 实例
            //注：我们现在直接基于 Jute 框架来进行 Socket 数据的读取，
            // 因此我们必须先构造一个空的 QuorumPacket 实例，来负责存放从 Socket 读取到的数据
            QuorumPacket qp = new QuorumPacket();
            /**
             * 阻塞 1
             * 一直阻塞到接收到 LearnerHandler 对应的 Learner 节点发来的第一个 QuorumPacket 消息实例
             */
            //从 BinaryInputArchive 实例中获取数据，因为其本质上基于 InputStream 实现，因此在没有字节数据时会阻塞
            //PS：按照 Leader 处的处理逻辑，我们很容易猜测出在 Learner 端在建立 Socket 连接后一定会主动地发送一个 tag 为 packet 的数据包
            //阻塞结束后 qp 实例中已经有 Learner 节点的概述情况，例如 epoch 以及 zxid 等信息
            ia.readRecord(qp, "packet");

            messageTracker.trackReceived(qp.getType());
            //如果从 Learner 节点上发来的第一个 Socket 数据表明其不属于 FOLLOWER 以及 OBSERVER，那么就直接结束 run() 方法
            if (qp.getType() != Leader.FOLLOWERINFO && qp.getType() != Leader.OBSERVERINFO) {
                LOG.error("First packet {} is not FOLLOWERINFO or OBSERVERINFO!", qp.toString());

                return;
            }

            if (learnerMaster instanceof ObserverMaster && qp.getType() != Leader.OBSERVERINFO) {
                throw new IOException("Non observer attempting to connect to ObserverMaster. type = " + qp.getType());
            }
            //这里是根据 QuorumPacket 实例简单封装的字节数据进行简要的数据读取，我们可以得到:
            //发来信息的 serverID、protocolVersion、configVersion
            //下面是依次得到这些数据，然后进行相关逻辑处理
            byte[] learnerInfoData = qp.getData();
            if (learnerInfoData != null) {
                ByteBuffer bbsid = ByteBuffer.wrap(learnerInfoData);
                if (learnerInfoData.length >= 8) {
                    this.sid = bbsid.getLong();
                }
                if (learnerInfoData.length >= 12) {
                    this.version = bbsid.getInt(); // protocolVersion
                }
                if (learnerInfoData.length >= 20) {
                    long configVersion = bbsid.getLong();
                    if (configVersion > learnerMaster.getQuorumVerifierVersion()) {
                        throw new IOException("Follower is ahead of the leader (has a later activated configuration)");
                    }
                }
            } else {
                this.sid = learnerMaster.getAndDecrementFollowerCounter();
            }
            //利用 Socket 消息中的 sid 数据来本地查询此节点的其他信息
            String followerInfo = learnerMaster.getPeerInfo(this.sid);
            //首先要判断此 Socket 中的 sid 是有对应的具体 ZooKeeper Server(即是否在 Leader 节点处的配置文件中注册)
            if (followerInfo.isEmpty()) {
                LOG.info(
                    "Follower sid: {} not in the current config {}",
                    this.sid,
                    Long.toHexString(learnerMaster.getQuorumVerifierVersion()));
            } else {
                LOG.info("Follower sid: {} : info : {}", this.sid, followerInfo);
            }
            //根据消息中节点的类型来决定 LearnerHandler 的类型
            if (qp.getType() == Leader.OBSERVERINFO) {
                learnerType = LearnerType.OBSERVER;
            }
            //向 LearnerMaster（通常是 Leader）注册一下 LearnerHandler 以及 Socket 信息
            learnerMaster.registerLearnerHandlerBean(this, sock);
            //得到 qp 数据包中最近的 epoch 值，其存储于 zxid 的高 32 位中
            long lastAcceptedEpoch = ZxidUtils.getEpochFromZxid(qp.getZxid());

            long peerLastZxid;
            StateSummary ss = null;
            //得到 qp 数据包中的 zxid 数据，其本身为 zxid 字段的低 32 位
            long zxid = qp.getZxid();
            /**
             * 阻塞 2
             * 会因为 epoch 还没达成共识而进入超时阻塞
             */
            //这里我们以 Leader.getEpochToPropose() 方法为例，这个方法会阻塞到达成 Leader 选举共识或者超时
            long newEpoch = learnerMaster.getEpochToPropose(this.getSid(), lastAcceptedEpoch);
            //阻塞结束，此时 epoch 已经达成共识
            //将达成共识的 epoch 最新值与 zxid (0)作为高 32 低 32 位构造出一个 newLeaderZxid 值
            long newLeaderZxid = ZxidUtils.makeZxid(newEpoch, 0);
            //这个 if 语句主要根据协议版本号来采取不同的策略，不是重点，直接看 else 逻辑即可
            if (this.getVersion() < 0x10000) {
                // we are going to have to extrapolate the epoch information
                long epoch = ZxidUtils.getEpochFromZxid(zxid);
                ss = new StateSummary(epoch, zxid);
                // fake the message
                learnerMaster.waitForEpochAck(this.getSid(), ss);
            } else {
                byte[] ver = new byte[4];
                ByteBuffer.wrap(ver).putInt(0x10000);
                //Leader 在这里要么达成了选举共识，要么超时，不管怎么样，这里要构造一个 packet 发送给此线程对应的 Learner 节点
                QuorumPacket newEpochPacket = new QuorumPacket(Leader.LEADERINFO, newLeaderZxid, ver, null);
                //这里是用序列化框架 Jute 封装的一个阻塞式 Socket 数据发送过程
                /**
                 * 阻塞 3：
                 * 会因为 Socket I/O 的写操作而阻塞
                 */
                oa.writeRecord(newEpochPacket, "packet");
                messageTracker.trackSent(Leader.LEADERINFO);
                bufferedOutput.flush();
                //然后构造一个 QuorumPacket 实例来接收此线程对应的 Learner 节点的响应
                QuorumPacket ackEpochPacket = new QuorumPacket();
                /**
                 * 阻塞 4：
                 * 会因为 Socket I/O 的读操作而阻塞
                 */
                ia.readRecord(ackEpochPacket, "packet");
                messageTracker.trackReceived(ackEpochPacket.getType());
                //只有响应类型为 ACKEPOCH 时，才被认为是合法的响应，否则结束线程并打印日志
                if (ackEpochPacket.getType() != Leader.ACKEPOCH) {
                    LOG.error("{} is not ACKEPOCH", ackEpochPacket.toString());
                    return;
                }
                //将 ackEpochPacket 实例封装为 bbepoch
                ByteBuffer bbepoch = ByteBuffer.wrap(ackEpochPacket.getData());
                //通过响应得到 StateSummary 实例
                ss = new StateSummary(bbepoch.getInt(), ackEpochPacket.getZxid());
                /**
                 * 阻塞 5：
                 * 阻塞直到得到半数以上的节点对新 epoch 达成共识
                 */
                //这里我们来看 Leader#learnerMaster() 方法，其用于阻塞直到 Leader 得到过半数节点对新 epoch 的 ack
                learnerMaster.waitForEpochAck(this.getSid(), ss);
            }
            /**
             * 下面的逻辑则主要用于 Leader 与 Follower 节点之前进行数据的同步
             *  DIFF、TRUNC、SNAP 有所区别，下面用例子来说明:
             *  - DIFF：Leader 节点的 zxid 为 100-600，LearnerHandler 对应的 Learner 最大 zxid 为 500，
             *  那么 Leader 节点只会将不同部分，即 501 - 600 的写操作日志同步到 Learner 节点
             *  - TRUNC：Leader 节点的 zxid 为 100-600，LearnerHandler 对应的 Learner 最大 zxid 为 700，
             *  那么 Leader 节点命令 Learner 节点将 601 - 700 的写操作通过写日志进行回滚（注意，通常不会有如此大数据量的写操作需要回滚）
             *  - SNAP：Leader 节点的 zxid 为 100-600，LearnerHandler 对应的 Learner 最大 zxid 为 10，
             *  那么 Leader 节点直接通过内存快照的方式来完成数据同步
             *
             *  注意事项：在 Leader 选举时，只有具有最大 zxid 的节点才能够被选为 Leader 节点，但是因为 Leader 选举只需要过半数的节点参与
             *  因此存在 zxid 更大，但是没有参与 Leader 选举，后来加入的 Follower 节点存在
             */
            //得到 Learner 最新的 zxid
            peerLastZxid = ss.getLastZxid();

            // Take any necessary action if we need to send TRUNC or DIFF
            // startForwarding() will be called in all cases
            // 这个方法非常重要，用于决定我们采用基于写日志的 DIFF、TRUNC 还是直接基于快照的 SNAP 来完成 Leader 与 Learner 节点间的数据同步
            // 但是需要注意的是：此方法并不会直接进行网络数据包的传输，而仅仅是构造出要传输的数据包，并将它们添加到队列中
            boolean needSnap = syncFollower(peerLastZxid, learnerMaster);

            // syncs between followers and the leader are exempt from throttling because it
            // is importatnt to keep the state of quorum servers up-to-date. The exempted syncs
            // are counted as concurrent syncs though
            boolean exemptFromThrottle = getLearnerType() != LearnerType.OBSERVER;
            /* if we are not truncating or sending a diff just send a snapshot */
            //如果 needSnap 为 true，说明只能进行 SNAP 式地数据同步
            if (needSnap) {
                syncThrottler = learnerMaster.getLearnerSnapSyncThrottler();
                syncThrottler.beginSync(exemptFromThrottle);
                ServerMetrics.getMetrics().INFLIGHT_SNAP_COUNT.add(syncThrottler.getSyncInProgress());
                try {
                    long zxidToSend = learnerMaster.getZKDatabase().getDataTreeLastProcessedZxid();
                    /**
                     * 阻塞：这是一个 OIO 的阻塞式 Socket 数据发送
                     */
                    //首先发送一个 SNAP 包，用于告知 Learner 节点：Leader 节点打算使用 SNAP 的方式来进行数据同步
                    oa.writeRecord(new QuorumPacket(Leader.SNAP, zxidToSend, null, null), "packet");
                    messageTracker.trackSent(Leader.SNAP);
                    bufferedOutput.flush();

                    LOG.info(
                        "Sending snapshot last zxid of peer is 0x{}, zxid of leader is 0x{}, "
                            + "send zxid of db as 0x{}, {} concurrent snapshot sync, "
                            + "snapshot sync was {} from throttle",
                        Long.toHexString(peerLastZxid),
                        Long.toHexString(leaderLastZxid),
                        Long.toHexString(zxidToSend),
                        syncThrottler.getSyncInProgress(),
                        exemptFromThrottle ? "exempt" : "not exempt");
                    // Dump data to peer
                    /**
                     * 阻塞：这是一个 OIO 的阻塞式 Socket 数据发送
                     */
                    // 将 DataBase 直接序列化，然后发送
                    learnerMaster.getZKDatabase().serializeSnapshot(oa);
                    oa.writeString("BenWasHere", "signature");
                    bufferedOutput.flush();
                } finally {
                    ServerMetrics.getMetrics().SNAP_COUNT.add(1);
                }
            } else {
                syncThrottler = learnerMaster.getLearnerDiffSyncThrottler();
                syncThrottler.beginSync(exemptFromThrottle);
                ServerMetrics.getMetrics().INFLIGHT_DIFF_COUNT.add(syncThrottler.getSyncInProgress());
                ServerMetrics.getMetrics().DIFF_COUNT.add(1);
            }

            LOG.debug("Sending NEWLEADER message to {}", sid);
            // the version of this quorumVerifier will be set by leader.lead() in case
            // the leader is just being established. waitForEpochAck makes sure that readyToStart is true if
            // we got here, so the version was set
            if (getVersion() < 0x10000) {
                QuorumPacket newLeaderQP = new QuorumPacket(Leader.NEWLEADER, newLeaderZxid, null, null);
                oa.writeRecord(newLeaderQP, "packet");
            } else {
                QuorumPacket newLeaderQP = new QuorumPacket(Leader.NEWLEADER, newLeaderZxid, learnerMaster.getQuorumVerifierBytes(), null);
                queuedPackets.add(newLeaderQP);
            }
            bufferedOutput.flush();
            //这个方法才开始真正的用于数据同步 Packet 数据包的发送工作，队列上的元素由新构造并启动的异步线程负责消费
            // Start thread that blast packets in the queue to learner
            startSendingPackets();

            /*
             * Have to wait for the first ACK, wait until
             * the learnerMaster is ready, and only then we can
             * start processing messages.
             */
            /**
             * 阻塞 6：
             * 等待来自此 LearnerHandler 实例对应的 Learner 的响应，阻塞直到收到对应的消息
             * 收到消息表明 Leader 与当前 Learner 节点之间的数据同步完成
             */
            qp = new QuorumPacket();
            ia.readRecord(qp, "packet");

            messageTracker.trackReceived(qp.getType());
            //确保收到 ACK 类型不为 Leader.ACK，因为此时当前节点认为其自己为 Leader 节点
            if (qp.getType() != Leader.ACK) {
                LOG.error("Next packet was supposed to be an ACK, but received packet: {}", packetToString(qp));
                return;
            }

            LOG.debug("Received NEWLEADER-ACK message from {}", sid);
            /**
             * 阻塞 7：
             * 等待有过半参与者完成数据的同步完成的 ACK 响应，一旦完成说明新的 Leader 完成过半参与者（包括当前节点自身）的数据同步操作
             * 具体逻辑与之间的过半验证非常相似，因此这里就不分析了
             */
            learnerMaster.waitForNewLeaderAck(getSid(), qp.getZxid());
            // 启动控制同步时间逻辑
            syncLimitCheck.start();
            // sync ends when NEWLEADER-ACK is received
            syncThrottler.endSync();

            if (needSnap) {
                ServerMetrics.getMetrics().INFLIGHT_SNAP_COUNT.add(syncThrottler.getSyncInProgress());
            } else {
                ServerMetrics.getMetrics().INFLIGHT_DIFF_COUNT.add(syncThrottler.getSyncInProgress());
            }
            syncThrottler = null;

            // now that the ack has been processed expect the syncLimit
            sock.setSoTimeout(learnerMaster.syncTimeout());

            /*
             * Wait until learnerMaster starts up
             */
            /**
             * 阻塞 8：
             * 阻塞直到 Leader 节点对应的 LeaderZooKeeperServer 实例启动了
             */
            learnerMaster.waitForStartup();

            // Mutation packets will be queued during the serialize,
            // so we need to mark when the peer can actually start
            // using the data
            // 向当前 LearnerHandler 实例对应的 Learner 发送 UPTPDATE 消息，说明此时同步已经完成
            // 其他 Follower/Observer 可以向 Leader 转发写请求了
            LOG.debug("Sending UPTODATE message to {}", sid);
            queuedPackets.add(new QuorumPacket(Leader.UPTODATE, -1, null, null));
            /**
             * 下面的逻辑则是 LeaderHandler 线程的 Main Loop
             * Main Loop 为集群的数据一致性初始化完毕后的正常状态执行逻辑
             */
            while (true) {
                /**
                 * OIO 阻塞式读取并序列化来自 Learner 的 Socket 消息
                 */
                qp = new QuorumPacket();
                ia.readRecord(qp, "packet");
                messageTracker.trackReceived(qp.getType());

                long traceMask = ZooTrace.SERVER_PACKET_TRACE_MASK;
                //处理消息类型为 PING（心跳包），可以见得 Ping 包的发送者将是 Learner，接收者则是 Leader
                if (qp.getType() == Leader.PING) {
                    traceMask = ZooTrace.SERVER_PING_TRACE_MASK;
                }
                if (LOG.isTraceEnabled()) {
                    ZooTrace.logQuorumPacket(LOG, traceMask, 'i', qp);
                }
                //更新一下连接的过期时间
                tickOfNextAckDeadline = learnerMaster.getTickOfNextAckDeadline();
                //自增至今已经接收到的数据包总数
                packetsReceived.incrementAndGet();

                ByteBuffer bb;
                long sessionId;
                int cxid;
                int type;
                //下面按照 Leader 发来数据包的不同类型来执行不同的逻辑
                switch (qp.getType()) {
                //当消息为对 Leader 命令的 ACK 时(2PC 中来自 Learner 对 Leader Proposal 的 ACK 回复)
                case Leader.ACK:
                    if (this.learnerType == LearnerType.OBSERVER) {
                        LOG.debug("Received ACK from Observer {}", this.sid);
                    }
                    //首先是对 Leader 与 Learner 之间用于消息同步的 syncLimitCheck 实例进行更新
                    syncLimitCheck.updateAck(qp.getZxid());
                    //2PC 中对来自 Learner 的 ACK 的处理
                    learnerMaster.processAck(this.sid, qp.getZxid(), sock.getLocalSocketAddress());
                    break;
                //当消息为对 Leader 的心跳 ping 时
                case Leader.PING:
                    // Process the touches
                    ByteArrayInputStream bis = new ByteArrayInputStream(qp.getData());
                    DataInputStream dis = new DataInputStream(bis);
                    while (dis.available() > 0) {
                        long sess = dis.readLong();
                        int to = dis.readInt();
                        learnerMaster.touch(sess, to);
                    }
                    break;
                //当消息为对 Leader 的重新验证时（重新激活两者之间的 Session）
                case Leader.REVALIDATE:
                    ServerMetrics.getMetrics().REVALIDATE_COUNT.add(1);
                    learnerMaster.revalidateSession(qp, this);
                    break;
                //当消息为对 Leader 的消息转发时(当 Client 向 Learner 节点发送 Request 时，其最终会向 Learner 节点发送写请求)
                case Leader.REQUEST:
                    bb = ByteBuffer.wrap(qp.getData());
                    sessionId = bb.getLong();
                    cxid = bb.getInt();
                    type = bb.getInt();
                    bb = bb.slice();
                    Request si;
                    // 同步请求
                    if (type == OpCode.sync) {
                        si = new LearnerSyncRequest(this, sessionId, cxid, type, bb, qp.getAuthinfo());
                    // 平常的转发写请求
                    } else {
                        si = new Request(null, sessionId, cxid, type, bb, qp.getAuthinfo());
                    }
                    si.setOwner(this);
                    learnerMaster.submitLearnerRequest(si);
                    requestsReceived.incrementAndGet();
                    break;
                default:
                    LOG.warn("unexpected quorum packet, type: {}", packetToString(qp));
                    break;
                }
            }
        } catch (IOException e) {
            if (sock != null && !sock.isClosed()) {
                LOG.error("Unexpected exception causing shutdown while sock still open", e);
                //close the socket to make sure the
                //other side can see it being close
                try {
                    sock.close();
                } catch (IOException ie) {
                    // do nothing
                }
            }
        } catch (InterruptedException e) {
            LOG.error("Unexpected exception in LearnerHandler.", e);
        } catch (SyncThrottleException e) {
            LOG.error("too many concurrent sync.", e);
            syncThrottler = null;
        } catch (Exception e) {
            LOG.error("Unexpected exception in LearnerHandler.", e);
            throw e;
        } finally {
            if (syncThrottler != null) {
                syncThrottler.endSync();
                syncThrottler = null;
            }
            String remoteAddr = getRemoteAddress();
            LOG.warn("******* GOODBYE {} ********", remoteAddr);
            messageTracker.dumpToLog(remoteAddr);
            shutdown();
        }
    }

    /**
     * Start thread that will forward any packet in the queue to the follower
     */
    protected void startSendingPackets() {
        if (!sendingThreadStarted) {
            // Start sending packets
            //这里启动一个异步线程来消费 queuedPackets 队列上的 QuorumPacket 实例
            new Thread() {
                public void run() {
                    Thread.currentThread().setName("Sender-" + sock.getRemoteSocketAddress());
                    try {
                        sendPackets();
                    } catch (InterruptedException e) {
                        LOG.warn("Unexpected interruption", e);
                    }
                }
            }.start();
            sendingThreadStarted = true;
        } else {
            LOG.error("Attempting to start sending thread after it already started");
        }
    }

    /**
     * Tests need not send marker packets as they are only needed to
     * log quorum packet delays
     */
    protected boolean shouldSendMarkerPacketForLogging() {
        return true;
    }

    /**
     * Determine if we need to sync with follower using DIFF/TRUNC/SNAP
     * and setup follower to receive packets from commit processor
     * 这个方法非常重要，用于决定我们采用基于写日志的 DIFF、TRUNC 还是直接基于快照的 SNAP 来完成 Leader 与 Learner 节点间的数据同步
     * @param peerLastZxid 此参数为 Learner 上最新的 zxid
     * @param learnerMaster 此参数通常为 Leader 实例
     * @return true if snapshot transfer is needed.
     */
    boolean syncFollower(long peerLastZxid, LearnerMaster learnerMaster) {
        /*
         * When leader election is completed, the leader will set its
         * lastProcessedZxid to be (epoch < 32). There will be no txn associated
         * with this zxid.
         *
         * The learner will set its lastProcessedZxid to the same value if
         * it get DIFF or SNAP from the learnerMaster. If the same learner come
         * back to sync with learnerMaster using this zxid, we will never find this
         * zxid in our history. In this case, we will ignore TRUNC logic and
         * always send DIFF if we have old enough history
         */
        boolean isPeerNewEpochZxid = (peerLastZxid & 0xffffffffL) == 0;
        // Keep track of the latest zxid which already queued
        long currentZxid = peerLastZxid;
        boolean needSnap = true;//用于表明是否要基于内存快照进行数据同步
        ZKDatabase db = learnerMaster.getZKDatabase();
        boolean txnLogSyncEnabled = db.isTxnLogSyncEnabled();
        //得到日志锁
        ReentrantReadWriteLock lock = db.getLogLock();
        //获取到读锁
        ReadLock rl = lock.readLock();
        try {
            //加读锁，此时可以并发地进行读操作，但是不能进行写操作
            rl.lock();
            //分别得到 Leader 本地最大以及最小的事务日志提交 id，其范围为 minCommitted ~ maxCommitted
            long maxCommittedLog = db.getmaxCommittedLog();
            long minCommittedLog = db.getminCommittedLog();
            //得到最近处理的 zxid
            long lastProcessedZxid = db.getDataTreeLastProcessedZxid();

            LOG.info("Synchronizing with Learner sid: {} maxCommittedLog=0x{}"
                     + " minCommittedLog=0x{} lastProcessedZxid=0x{}"
                     + " peerLastZxid=0x{}",
                     getSid(),
                     Long.toHexString(maxCommittedLog),
                     Long.toHexString(minCommittedLog),
                     Long.toHexString(lastProcessedZxid),
                     Long.toHexString(peerLastZxid));
            //如果 Leader 处的日志记录为空，那么直接将 min\max 事务 id 赋值为最近处理的 zxid 值 lastProcessedZxid
            if (db.getCommittedLog().isEmpty()) {
                /*
                 * It is possible that committedLog is empty. In that case
                 * setting these value to the latest txn in learnerMaster db
                 * will reduce the case that we need to handle
                 *
                 * Here is how each case handle by the if block below
                 * 1. lastProcessZxid == peerZxid -> Handle by (2)
                 * 2. lastProcessZxid < peerZxid -> Handle by (3)
                 * 3. lastProcessZxid > peerZxid -> Handle by (5)
                 */
                minCommittedLog = lastProcessedZxid;
                maxCommittedLog = lastProcessedZxid;
            }

            /*
             * Here are the cases that we want to handle
             *
             * 1. Force sending snapshot (for testing purpose)
             * 2. Peer and learnerMaster is already sync, send empty diff
             * 3. Follower has txn that we haven't seen. This may be old leader
             *    so we need to send TRUNC. However, if peer has newEpochZxid,
             *    we cannot send TRUNC since the follower has no txnlog
             * 4. Follower is within committedLog range or already in-sync.
             *    We may need to send DIFF or TRUNC depending on follower's zxid
             *    We always send empty DIFF if follower is already in-sync
             * 5. Follower missed the committedLog. We will try to use on-disk
             *    txnlog + committedLog to sync with follower. If that fail,
             *    we will send snapshot
             */
            //因为进行基于内存快照式的同步存在效率比较低的问题
            if (forceSnapSync) {
                // Force learnerMaster to use snapshot to sync with follower
                LOG.warn("Forcing snapshot sync - should not see this in production");
            //下面的逻辑块是进行一个空的 DIFF 操作，意味 Learner 节点与 Leader 节点已经在数据一致性上达成了共识
            } else if (lastProcessedZxid == peerLastZxid) {
                // Follower is already sync with us, send empty diff
                LOG.info(
                    "Sending DIFF zxid=0x{} for peer sid: {}",
                    Long.toHexString(peerLastZxid),
                    getSid());
                queueOpPacket(Leader.DIFF, peerLastZxid);
                needOpPacket = false;
                needSnap = false;
            //下面的逻辑块表明 Learner 的最新 zxid 领先于 Leader 的 zxid，因此选择 TRUNC 式的数据同步
            //回滚时 Leader 只需要告知 Learner 回滚到哪一个事务 id 即可，并不需要其他消息的传递
            } else if (peerLastZxid > maxCommittedLog && !isPeerNewEpochZxid) {
                // Newer than committedLog, send trunc and done
                LOG.debug(
                    "Sending TRUNC to follower zxidToSend=0x{} for peer sid:{}",
                    Long.toHexString(maxCommittedLog),
                    getSid());
                queueOpPacket(Leader.TRUNC, maxCommittedLog);
                currentZxid = maxCommittedLog;
                needOpPacket = false;
                needSnap = false;
            //下面的逻辑块表明 Learner 的最新 zxid 位于 Leader 的 min-max id 范围之内
            } else if ((maxCommittedLog >= peerLastZxid) && (minCommittedLog <= peerLastZxid)) {
                // Follower is within commitLog range
                LOG.info("Using committedLog for peer sid: {}", getSid());
                //首先得到日志的迭代器
                Iterator<Proposal> itr = db.getCommittedLog().iterator();
                //这个方法是进行 DIFF 方式数据同步的核心
                currentZxid = queueCommittedProposals(itr, peerLastZxid, null, maxCommittedLog);
                needSnap = false;
            //下面逻辑表明 Learner 的事务日志落后太多了，比 Leader 处最小的事务都小，那么就需要进行快照式的复制
            } else if (peerLastZxid < minCommittedLog && txnLogSyncEnabled) {
                // Use txnlog and committedLog to sync

                // Calculate sizeLimit that we allow to retrieve txnlog from disk
                long sizeLimit = db.calculateTxnLogSizeLimit();
                // This method can return empty iterator if the requested zxid
                // is older than on-disk txnlog
                Iterator<Proposal> txnLogItr = db.getProposalsFromTxnLog(peerLastZxid, sizeLimit);
                if (txnLogItr.hasNext()) {
                    LOG.info("Use txnlog and committedLog for peer sid: {}", getSid());
                    currentZxid = queueCommittedProposals(txnLogItr, peerLastZxid, minCommittedLog, maxCommittedLog);

                    if (currentZxid < minCommittedLog) {
                        LOG.info(
                            "Detected gap between end of txnlog: 0x{} and start of committedLog: 0x{}",
                            Long.toHexString(currentZxid),
                            Long.toHexString(minCommittedLog));
                        currentZxid = peerLastZxid;
                        // Clear out currently queued requests and revert
                        // to sending a snapshot.
                        queuedPackets.clear();
                        needOpPacket = true;
                    } else {
                        LOG.debug("Queueing committedLog 0x{}", Long.toHexString(currentZxid));
                        Iterator<Proposal> committedLogItr = db.getCommittedLog().iterator();
                        currentZxid = queueCommittedProposals(committedLogItr, currentZxid, null, maxCommittedLog);
                        needSnap = false;
                    }
                }
                // closing the resources
                if (txnLogItr instanceof TxnLogProposalIterator) {
                    TxnLogProposalIterator txnProposalItr = (TxnLogProposalIterator) txnLogItr;
                    txnProposalItr.close();
                }
            } else {
                LOG.warn(
                    "Unhandled scenario for peer sid: {} maxCommittedLog=0x{}"
                        + " minCommittedLog=0x{} lastProcessedZxid=0x{}"
                        + " peerLastZxid=0x{} txnLogSyncEnabled={}",
                    getSid(),
                    Long.toHexString(maxCommittedLog),
                    Long.toHexString(minCommittedLog),
                    Long.toHexString(lastProcessedZxid),
                    Long.toHexString(peerLastZxid),
                    txnLogSyncEnabled);
            }
            if (needSnap) {
                currentZxid = db.getDataTreeLastProcessedZxid();
            }

            LOG.debug("Start forwarding 0x{} for peer sid: {}", Long.toHexString(currentZxid), getSid());
            //开始数据的同步，不过事实上还是仅仅构造 Packet，并将其加入到队列中
            leaderLastZxid = learnerMaster.startForwarding(this, currentZxid);
        } finally {
            //释放读锁
            rl.unlock();
        }

        if (needOpPacket && !needSnap) {
            // This should never happen, but we should fall back to sending
            // snapshot just in case.
            LOG.error("Unhandled scenario for peer sid: {} fall back to use snapshot",  getSid());
            needSnap = true;
        }

        return needSnap;
    }

    /**
     * Queue committed proposals into packet queue. The range of packets which
     * is going to be queued are (peerLaxtZxid, maxZxid]
     *
     * @param itr  iterator point to the proposals
     * @param peerLastZxid  last zxid seen by the follower
     * @param maxZxid  max zxid of the proposal to queue, null if no limit
     * @param lastCommittedZxid when sending diff, we need to send lastCommittedZxid
     *        on the leader to follow Zab 1.0 protocol.
     * @return last zxid of the queued proposal
     */
    protected long queueCommittedProposals(Iterator<Proposal> itr, long peerLastZxid, Long maxZxid, Long lastCommittedZxid) {
        boolean isPeerNewEpochZxid = (peerLastZxid & 0xffffffffL) == 0;
        long queuedZxid = peerLastZxid;
        // as we look through proposals, this variable keeps track of previous
        // proposal Id.
        long prevProposalZxid = -1;
        //遍历事务迭代器，将符合要求的事务操作封装为 QuorumPacket 实例，并将其加入到 QuorumPacket 阻塞队列中
        //注意事项：迭代器的迭代过程中的 Proposal 实例的 zxid 并不一定完全按照 zxid 递增顺序进行

        /**
         * 为什么需要遍历事务迭代器的方式来完成数据一致性的同步操作？
         * 这是因为事务并不会完全按照 zxid 的大小顺序进行存放的。例如在某一时刻 Leader 节点接收到了大量的写请求，
         * 其在本地为这些写请求依次分配递增的 zxid，而且**并发地**进行 2PC 提交。但是因为网络原因，Follower
         * 节点处并不能确保一定按照 zxid 顺序来接收 Proposal，由于日志写操作只能顺序追加，因此事务在 Follower 节点处就不再是按照 zxid 来存储了，
         * 当然 Leader 节点也存在这个问题。
         * 总之，使用迭代器的原因在于：顺序写入的 Proposal 的 zxid 并不一定是按照递增顺序存储的，
         * Leader 在遍历日志时，必须从头到尾依次遍历。
         * 举一个例子：
         * Leader 处的 zxid 为：3，4，5，8，7，6 而 Learner 节点的 lastZxid 为 7，
         * 那么当 Leader 节点利用事务迭代器遍历到 zxid 为 8 的 Proposal 时，
         * 其会选择告知 Learner 需要为 zxid 为 8 的 Proposal 进行数据的同步。
         * 但是其不能直接决定迭代器之后的 Proposal 都需要给 Learner 节点进行数据同步，
         * 因为 Proposal 并不完全按照 zxid 递增顺序进行存储，
         * zxid 为 7、6 的 Proposal 并不需要给 Learner 进行数据同步。
         */

        while (itr.hasNext()) {
            Proposal propose = itr.next();
            //得到 Proposal 对应的 zxid
            long packetZxid = propose.packet.getZxid();
            // abort if we hit the limit
            if ((maxZxid != null) && (packetZxid > maxZxid)) {
                break;
            }

            // skip the proposals the peer already has
            // 跳过 Learner 处已经有的写操作日志 Proposal 的 zxid < peerLastZxid
            if (packetZxid < peerLastZxid) {
                prevProposalZxid = packetZxid;
                continue;
            }
            // 走到这的 Proposal 实例的 zxid >= peerLastZxid
            // If we are sending the first packet, figure out whether to trunc
            // or diff
            if (needOpPacket) {

                // Send diff when we see the follower's zxid in our history
                // 如果在 Leader 节点上发现有着与 Learner 节点的 peerLastZxid 一样的 zxid，那么也要发一个 DIFF 指令
                // 并因为将 needOpPacket 置为 false，因此下一个 Proposal 不会走这个 if 逻辑
                // 这至少表明 Leader 与 Learner 之间不需要进行 SNAP 以及 TRUNC 式的数据同步，最多采用 DIF 式的数据同步
                if (packetZxid == peerLastZxid) {
                    LOG.info(
                        "Sending DIFF zxid=0x{}  for peer sid: {}",
                        Long.toHexString(lastCommittedZxid),
                        getSid());

                    queueOpPacket(Leader.DIFF, lastCommittedZxid);
                    needOpPacket = false;
                    continue;
                }

                if (isPeerNewEpochZxid) {
                    // Send diff and fall through if zxid is of a new-epoch
                    LOG.info(
                        "Sending DIFF zxid=0x{}  for peer sid: {}",
                        Long.toHexString(lastCommittedZxid),
                        getSid());
                    //lastCommittedZxid 为 Leader 处最新的 zxid
                    queueOpPacket(Leader.DIFF, lastCommittedZxid);
                    needOpPacket = false;
                } else if (packetZxid > peerLastZxid) {
                    // Peer have some proposals that the learnerMaster hasn't seen yet
                    // it may used to be a leader
                    //如果 Leader 节点的 epoch 与 Learner 的 epoch 不同，那么
                    if (ZxidUtils.getEpochFromZxid(packetZxid) != ZxidUtils.getEpochFromZxid(peerLastZxid)) {
                        // We cannot send TRUNC that cross epoch boundary.
                        // The learner will crash if it is asked to do so.
                        // We will send snapshot this those cases.
                        LOG.warn("Cannot send TRUNC to peer sid: " + getSid() + " peer zxid is from different epoch");
                        return queuedZxid;
                    }

                    LOG.info(
                        "Sending TRUNC zxid=0x{}  for peer sid: {}",
                        Long.toHexString(prevProposalZxid),
                        getSid());
                    queueOpPacket(Leader.TRUNC, prevProposalZxid);
                    needOpPacket = false;
                }
            }

            if (packetZxid <= queuedZxid) {
                // We can get here, if we don't have op packet to queue
                // or there is a duplicate txn in a given iterator
                continue;
            }

            // Since this is already a committed proposal, we need to follow
            // it by a commit packet
            //利用 Proposal 实例得到其对应的 QuorumPacket 实例，然后将其加入队列中
            queuePacket(propose.packet);
            queueOpPacket(Leader.COMMIT, packetZxid);
            queuedZxid = packetZxid;

        }

        if (needOpPacket && isPeerNewEpochZxid) {
            // We will send DIFF for this kind of zxid in any case. This if-block
            // is the catch when our history older than learner and there is
            // no new txn since then. So we need an empty diff
            LOG.info(
                "Sending TRUNC zxid=0x{}  for peer sid: {}",
                Long.toHexString(lastCommittedZxid),
                getSid());
            queueOpPacket(Leader.DIFF, lastCommittedZxid);
            needOpPacket = false;
        }

        return queuedZxid;
    }

    public void shutdown() {
        // Send the packet of death
        try {
            queuedPackets.clear();
            queuedPackets.put(proposalOfDeath);
        } catch (InterruptedException e) {
            LOG.warn("Ignoring unexpected exception", e);
        }
        try {
            if (sock != null && !sock.isClosed()) {
                sock.close();
            }
        } catch (IOException e) {
            LOG.warn("Ignoring unexpected exception during socket close", e);
        }
        this.interrupt();
        learnerMaster.removeLearnerHandler(this);
        learnerMaster.unregisterLearnerHandlerBean(this);
    }

    public long tickOfNextAckDeadline() {
        return tickOfNextAckDeadline;
    }

    /**
     * ping calls from the learnerMaster to the peers
     */
    public void ping() {
        // If learner hasn't sync properly yet, don't send ping packet
        // otherwise, the learner will crash
        if (!sendingThreadStarted) {
            return;
        }
        long id;
        if (syncLimitCheck.check(System.nanoTime())) {
            id = learnerMaster.getLastProposed();
            QuorumPacket ping = new QuorumPacket(Leader.PING, id, null, null);
            queuePacket(ping);
        } else {
            LOG.warn("Closing connection to peer due to transaction timeout.");
            shutdown();
        }
    }

    /**
     * Queue leader packet of a given type
     * @param type
     * @param zxid
     */
    private void queueOpPacket(int type, long zxid) {
        QuorumPacket packet = new QuorumPacket(type, zxid, null, null);
        queuePacket(packet);
    }

    void queuePacket(QuorumPacket p) {
        queuedPackets.add(p);
        // Add a MarkerQuorumPacket at regular intervals.
        if (shouldSendMarkerPacketForLogging() && packetCounter.getAndIncrement() % markerPacketInterval == 0) {
            queuedPackets.add(new MarkerQuorumPacket(System.nanoTime()));
        }
        queuedPacketsSize.addAndGet(packetSize(p));
    }

    static long packetSize(QuorumPacket p) {
        /* Approximate base size of QuorumPacket: int + long + byte[] + List */
        long size = 4 + 8 + 8 + 8;
        byte[] data = p.getData();
        if (data != null) {
            size += data.length;
        }
        return size;
    }

    public boolean synced() {
        return isAlive() && learnerMaster.getCurrentTick() <= tickOfNextAckDeadline;
    }

    public synchronized Map<String, Object> getLearnerHandlerInfo() {
        Map<String, Object> info = new LinkedHashMap<>(9);
        info.put("remote_socket_address", getRemoteAddress());
        info.put("sid", getSid());
        info.put("established", getEstablished());
        info.put("queued_packets", queuedPackets.size());
        info.put("queued_packets_size", queuedPacketsSize.get());
        info.put("packets_received", packetsReceived.longValue());
        info.put("packets_sent", packetsSent.longValue());
        info.put("requests", requestsReceived.longValue());
        info.put("last_zxid", getLastZxid());

        return info;
    }

    public synchronized void resetObserverConnectionStats() {
        packetsReceived.set(0);
        packetsSent.set(0);
        requestsReceived.set(0);

        lastZxid = -1;
    }

    /**
     * For testing, return packet queue
     * @return
     */
    public Queue<QuorumPacket> getQueuedPackets() {
        return queuedPackets;
    }

    /**
     * For testing, we need to reset this value
     */
    public void setFirstPacket(boolean value) {
        needOpPacket = value;
    }

}
