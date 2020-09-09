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

package org.apache.zookeeper.server.watch;

import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.server.ServerCnxn;
import org.apache.zookeeper.server.ServerMetrics;
import org.apache.zookeeper.server.ZooTrace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class manages watches. It allows watches to be associated with a string
 * and removes watchers and their watches in addition to managing triggers.
 */

/**
 * WatchManager 为 ZooKeeper 服务端处理 Watch 事件的类，其负责三件事：
 * - Watcher 的注册：能将 path 映射为 Set<Watcher>，也能将 Watcher 映射为 Set<String>，其中 String 的语义是 path
 * - Watcher 注册的移除管理
 * - Watcher 事件的触发（trigger）
 */

public class WatchManager implements IWatchManager {

    private static final Logger LOG = LoggerFactory.getLogger(WatchManager.class);
    //完成 path 到 Set<Watcher> 的映射，可以注意到一个 path 下可以注册多个 Watcher，只是不能重复注册(ServerCnxn 也为 Watcher 类型)
    private final Map<String, Set<Watcher>> watchTable = new HashMap<>();
    //完成 Watcher 到 Set<String>(String 含义为 path) 的映射，可以注意到，一个 Watcher 可以观察多个 path
    private final Map<Watcher, Set<String>> watch2Paths = new HashMap<>();
    //用于管理 Watcher 模式的管理
    private final WatcherModeManager watcherModeManager = new WatcherModeManager();

    @Override
    public synchronized int size() {
        int result = 0;
        for (Set<Watcher> watches : watchTable.values()) {
            result += watches.size();
        }
        return result;
    }

    private boolean isDeadWatcher(Watcher watcher) {
        return watcher instanceof ServerCnxn && ((ServerCnxn) watcher).isStale();
    }

    @Override
    public boolean addWatch(String path, Watcher watcher) {
        return addWatch(path, watcher, WatcherMode.DEFAULT_WATCHER_MODE);
    }
    //一路到了这里
    //特别需要注意的是这里的 list 容器都基于 Set 实现，可见我们并不能重复注册
    //举一个例子来说就是：即使我们重复执行了 `get -w /foo` 命令，但最终也仅仅注册了一次 Watcher，
    //在触发一次之后，只有再次注册，才能触发新事件
    @Override
    public synchronized boolean addWatch(String path, Watcher watcher, WatcherMode watcherMode) {
        if (isDeadWatcher(watcher)) {
            LOG.debug("Ignoring addWatch with closed cnxn");
            return false;
        }
        //利用命令中的 path 得到一个 Watcher list，如果第一次对 path 施加 watcher 机制，这里自然是 null
        Set<Watcher> list = watchTable.get(path);
        if (list == null) {
            // don't waste memory if there are few watches on a node
            // rehash when the 4th entry is added, doubling size thereafter
            // seems like a good compromise
            //初始化大小为 4 的 HashSer<Watcher>，4 的大小是有考虑的，从尽量不浪费内存的角度
            list = new HashSet<>(4);
            watchTable.put(path, list);//将 key-path:value-Set<Watcher> 键值对加入到 watchTable 实例中
        }
        //将当前 watcher 加入到 list 中，注意，我们这里的 Watcher 实例具体类型为 ServerCnxn
        list.add(watcher);
        //利用 watch2Paths 将 watcher 映射为 path，在第一次时也为空，如果为空，那么就初始化
        Set<String> paths = watch2Paths.get(watcher);
        if (paths == null) {
            // cnxns typically have many watches, so use default cap here
            paths = new HashSet<>();
            watch2Paths.put(watcher, paths);
        }
        //我们继续往下看 WatcherModeManager.setWatcherMode(Watcher watcher, String path, WatcherMode mode) 方法
        watcherModeManager.setWatcherMode(watcher, path, watcherMode);
        //将 path 加入到 Set<String> paths 实例中
        return paths.add(path);
    }

    @Override
    public synchronized void removeWatcher(Watcher watcher) {
        Set<String> paths = watch2Paths.remove(watcher);
        if (paths == null) {
            return;
        }
        for (String p : paths) {
            Set<Watcher> list = watchTable.get(p);
            if (list != null) {
                list.remove(watcher);
                if (list.isEmpty()) {
                    watchTable.remove(p);
                }
            }
            watcherModeManager.removeWatcher(watcher, p);
        }
    }

    @Override
    public WatcherOrBitSet triggerWatch(String path, EventType type) {
        return triggerWatch(path, type, null);
    }

    @Override
    public WatcherOrBitSet triggerWatch(String path, EventType type, WatcherOrBitSet supress) {
        WatchedEvent e = new WatchedEvent(type, KeeperState.SyncConnected, path);
        Set<Watcher> watchers = new HashSet<>();
        PathParentIterator pathParentIterator = getPathParentIterator(path);
        synchronized (this) {
            for (String localPath : pathParentIterator.asIterable()) {
                Set<Watcher> thisWatchers = watchTable.get(localPath);
                if (thisWatchers == null || thisWatchers.isEmpty()) {
                    continue;
                }
                Iterator<Watcher> iterator = thisWatchers.iterator();
                while (iterator.hasNext()) {
                    Watcher watcher = iterator.next();
                    WatcherMode watcherMode = watcherModeManager.getWatcherMode(watcher, localPath);
                    if (watcherMode.isRecursive()) {
                        if (type != EventType.NodeChildrenChanged) {
                            watchers.add(watcher);
                        }
                    } else if (!pathParentIterator.atParentPath()) {
                        watchers.add(watcher);
                        if (!watcherMode.isPersistent()) {
                            iterator.remove();
                            Set<String> paths = watch2Paths.get(watcher);
                            if (paths != null) {
                                paths.remove(localPath);
                            }
                        }
                    }
                }
                if (thisWatchers.isEmpty()) {
                    watchTable.remove(localPath);
                }
            }
        }
        if (watchers.isEmpty()) {
            if (LOG.isTraceEnabled()) {
                ZooTrace.logTraceMessage(LOG, ZooTrace.EVENT_DELIVERY_TRACE_MASK, "No watchers for " + path);
            }
            return null;
        }

        for (Watcher w : watchers) {
            if (supress != null && supress.contains(w)) {
                continue;
            }
            w.process(e);
        }

        switch (type) {
            case NodeCreated:
                ServerMetrics.getMetrics().NODE_CREATED_WATCHER.add(watchers.size());
                break;

            case NodeDeleted:
                ServerMetrics.getMetrics().NODE_DELETED_WATCHER.add(watchers.size());
                break;

            case NodeDataChanged:
                ServerMetrics.getMetrics().NODE_CHANGED_WATCHER.add(watchers.size());
                break;

            case NodeChildrenChanged:
                ServerMetrics.getMetrics().NODE_CHILDREN_WATCHER.add(watchers.size());
                break;
            default:
                // Other types not logged.
                break;
        }

        return new WatcherOrBitSet(watchers);
    }

    @Override
    public synchronized String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append(watch2Paths.size()).append(" connections watching ").append(watchTable.size()).append(" paths\n");

        int total = 0;
        for (Set<String> paths : watch2Paths.values()) {
            total += paths.size();
        }
        sb.append("Total watches:").append(total);

        return sb.toString();
    }

    @Override
    public synchronized void dumpWatches(PrintWriter pwriter, boolean byPath) {
        if (byPath) {
            for (Entry<String, Set<Watcher>> e : watchTable.entrySet()) {
                pwriter.println(e.getKey());
                for (Watcher w : e.getValue()) {
                    pwriter.print("\t0x");
                    pwriter.print(Long.toHexString(((ServerCnxn) w).getSessionId()));
                    pwriter.print("\n");
                }
            }
        } else {
            for (Entry<Watcher, Set<String>> e : watch2Paths.entrySet()) {
                pwriter.print("0x");
                pwriter.println(Long.toHexString(((ServerCnxn) e.getKey()).getSessionId()));
                for (String path : e.getValue()) {
                    pwriter.print("\t");
                    pwriter.println(path);
                }
            }
        }
    }

    @Override
    public synchronized boolean containsWatcher(String path, Watcher watcher) {
        WatcherMode watcherMode = watcherModeManager.getWatcherMode(watcher, path);
        PathParentIterator pathParentIterator = getPathParentIterator(path);
        for (String localPath : pathParentIterator.asIterable()) {
            Set<Watcher> watchers = watchTable.get(localPath);
            if (!pathParentIterator.atParentPath()) {
                if (watchers != null) {
                    return true;    // at the leaf node, all watcher types match
                }
            }
            if (watcherMode.isRecursive()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public synchronized boolean removeWatcher(String path, Watcher watcher) {
        Set<String> paths = watch2Paths.get(watcher);
        if (paths == null || !paths.remove(path)) {
            return false;
        }

        Set<Watcher> list = watchTable.get(path);
        if (list == null || !list.remove(watcher)) {
            return false;
        }

        if (list.isEmpty()) {
            watchTable.remove(path);
        }

        watcherModeManager.removeWatcher(watcher, path);

        return true;
    }

    @Override
    public synchronized WatchesReport getWatches() {
        Map<Long, Set<String>> id2paths = new HashMap<>();
        for (Entry<Watcher, Set<String>> e : watch2Paths.entrySet()) {
            Long id = ((ServerCnxn) e.getKey()).getSessionId();
            Set<String> paths = new HashSet<>(e.getValue());
            id2paths.put(id, paths);
        }
        return new WatchesReport(id2paths);
    }

    @Override
    public synchronized WatchesPathReport getWatchesByPath() {
        Map<String, Set<Long>> path2ids = new HashMap<>();
        for (Entry<String, Set<Watcher>> e : watchTable.entrySet()) {
            Set<Long> ids = new HashSet<>(e.getValue().size());
            path2ids.put(e.getKey(), ids);
            for (Watcher watcher : e.getValue()) {
                ids.add(((ServerCnxn) watcher).getSessionId());
            }
        }
        return new WatchesPathReport(path2ids);
    }

    @Override
    public synchronized WatchesSummary getWatchesSummary() {
        int totalWatches = 0;
        for (Set<String> paths : watch2Paths.values()) {
            totalWatches += paths.size();
        }
        return new WatchesSummary(watch2Paths.size(), watchTable.size(), totalWatches);
    }

    @Override
    public void shutdown() { /* do nothing */ }

    @Override
    public int getRecursiveWatchQty() {
        return watcherModeManager.getRecursiveQty();
    }

    private PathParentIterator getPathParentIterator(String path) {
        if (watcherModeManager.getRecursiveQty() == 0) {
            return PathParentIterator.forPathOnly(path);
        }
        return PathParentIterator.forAll(path);
    }
}
