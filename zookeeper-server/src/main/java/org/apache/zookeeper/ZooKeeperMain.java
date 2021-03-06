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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.admin.ZooKeeperAdmin;
import org.apache.zookeeper.cli.CliCommand;
import org.apache.zookeeper.cli.CliException;
import org.apache.zookeeper.cli.CommandFactory;
import org.apache.zookeeper.cli.CommandNotFoundException;
import org.apache.zookeeper.cli.MalformedCommandException;
import org.apache.zookeeper.client.ZKClientConfig;
import org.apache.zookeeper.server.ExitCode;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.util.ServiceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The command line client to ZooKeeper.
 *
 * 这是 ZooKeeper 客户端的入口类，可以直接去看 main 方法
 */
@InterfaceAudience.Public
public class ZooKeeperMain {

    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperMain.class);
    static final Map<String, String> commandMap = new HashMap<String, String>();
    static final Map<String, CliCommand> commandMapCli = new HashMap<String, CliCommand>();

    protected MyCommandOptions cl = new MyCommandOptions();
    protected HashMap<Integer, String> history = new HashMap<Integer, String>();
    protected int commandCount = 0;
    protected boolean printWatches = true;
    protected int exitCode = ExitCode.EXECUTION_FINISHED.getValue();

    protected ZooKeeper zk;
    protected String host = "";

    public boolean getPrintWatches() {
        return printWatches;
    }

    static {
        commandMap.put("connect", "host:port");
        commandMap.put("history", "");
        commandMap.put("redo", "cmdno");
        commandMap.put("printwatches", "on|off");
        commandMap.put("quit", "");
        Stream.of(CommandFactory.Command.values())
            .map(command -> CommandFactory.getInstance(command))
            // add all commands to commandMapCli and commandMap
            .forEach(cliCommand ->{
                cliCommand.addToMap(commandMapCli);
                commandMap.put(
                        cliCommand.getCmdStr(),
                        cliCommand.getOptionStr());
            });
    }

    static void usage() {
        System.err.println("ZooKeeper -server host:port -client-configuration properties-file cmd args");
        List<String> cmdList = new ArrayList<String>(commandMap.keySet());
        Collections.sort(cmdList);
        for (String cmd : cmdList) {
            System.err.println("\t" + cmd + " " + commandMap.get(cmd));
        }
    }
    //这是默认的客户端 Watcher 事件处理器，其处理 Watcher 事件非常简单
    private class MyWatcher implements Watcher {

        public void process(WatchedEvent event) {

            if (getPrintWatches()) {
                //利用 System.out.println 打印出 `WATCHER::` 字符串
                ZooKeeperMain.printMessage("WATCHER::");
                //还是利用 System.out.println 打印出事件对应的字符串
                ZooKeeperMain.printMessage(event.toString());
            }
        }

    }

    /**
     * A storage class for both command line options and shell commands.
     *
     */
    static class MyCommandOptions {

        private Map<String, String> options = new HashMap<String, String>();
        private List<String> cmdArgs = null;
        private String command = null;
        public static final Pattern ARGS_PATTERN = Pattern.compile("\\s*([^\"\']\\S*|\"[^\"]*\"|'[^']*')\\s*");
        public static final Pattern QUOTED_PATTERN = Pattern.compile("^([\'\"])(.*)(\\1)$");

        public MyCommandOptions() {
            options.put("server", "localhost:2181");
            options.put("timeout", "30000");
        }

        public String getOption(String opt) {
            return options.get(opt);
        }

        public String getCommand() {
            return command;
        }

        public String getCmdArgument(int index) {
            return cmdArgs.get(index);
        }

        public int getNumArguments() {
            return cmdArgs.size();
        }

        public String[] getArgArray() {
            return cmdArgs.toArray(new String[0]);
        }

        /**
         * Parses a command line that may contain one or more flags
         * before an optional command string
         * @param args command line arguments
         * @return true if parsing succeeded, false otherwise.
         */
        public boolean parseOptions(String[] args) {
            List<String> argList = Arrays.asList(args);
            Iterator<String> it = argList.iterator();

            while (it.hasNext()) {
                String opt = it.next();
                try {
                    if (opt.equals("-server")) {
                        options.put("server", it.next());
                    } else if (opt.equals("-timeout")) {
                        options.put("timeout", it.next());
                    } else if (opt.equals("-r")) {
                        options.put("readonly", "true");
                    } else if (opt.equals("-client-configuration")) {
                        options.put("client-configuration", it.next());
                    }
                } catch (NoSuchElementException e) {
                    System.err.println("Error: no argument found for option " + opt);
                    return false;
                }

                if (!opt.startsWith("-")) {
                    command = opt;
                    cmdArgs = new ArrayList<String>();
                    cmdArgs.add(command);
                    while (it.hasNext()) {
                        cmdArgs.add(it.next());
                    }
                    return true;
                }
            }
            return true;
        }

        /**
         * Breaks a string into command + arguments.
         * @param cmdstring string of form "cmd arg1 arg2..etc"
         * @return true if parsing succeeded.
         */
        public boolean parseCommand(String cmdstring) {
            Matcher matcher = ARGS_PATTERN.matcher(cmdstring);

            List<String> args = new LinkedList<String>();
            while (matcher.find()) {
                String value = matcher.group(1);
                if (QUOTED_PATTERN.matcher(value).matches()) {
                    // Strip off the surrounding quotes
                    value = value.substring(1, value.length() - 1);
                }
                args.add(value);
            }
            if (args.isEmpty()) {
                return false;
            }
            command = args.get(0);
            cmdArgs = args;
            return true;
        }

    }

    /**
     * Makes a list of possible completions, either for commands
     * or for zk nodes if the token to complete begins with /
     *
     */

    protected void addToHistory(int i, String cmd) {
        history.put(i, cmd);
    }

    public static List<String> getCommands() {
        List<String> cmdList = new ArrayList<String>(commandMap.keySet());
        Collections.sort(cmdList);
        return cmdList;
    }
    //此方法的返回值，例如："[zk: localhost:2181,localhost:2182,localhost:2183(CONNECTED) 0]"
    protected String getPrompt() {
        return "[zk: " + host + "(" + zk.getState() + ")" + " " + commandCount + "] ";
    }

    public static void printMessage(String msg) {
        System.out.println("\n" + msg);
    }

    protected void connectToZK(String newHost) throws InterruptedException, IOException {
        //如果当前 ZooKeeper 实例还在运行，那么就进行关闭
        if (zk != null && zk.getState().isAlive()) {
            zk.close();
        }
        //下面是一些配置信息的解析与读取
        host = newHost;
        boolean readOnly = cl.getOption("readonly") != null;//ZeeKeeper 客户端的只读模式
        if (cl.getOption("secure") != null) {
            System.setProperty(ZKClientConfig.SECURE_CLIENT, "true");
            System.out.println("Secure connection is enabled");
        }

        ZKClientConfig clientConfig = null;

        if (cl.getOption("client-configuration") != null) {
            try {
                clientConfig = new ZKClientConfig(cl.getOption("client-configuration"));
            } catch (QuorumPeerConfig.ConfigException e) {
                e.printStackTrace();
                ServiceUtils.requestSystemExit(ExitCode.INVALID_INVOCATION.getValue());
            }
        }
        //ZooKeeperAdmin extends ZooKeeper，较早的 ZooKeeper 版本直接是构造一个 ZooKeeper 实例
        //子类构造过程中或显式或隐式地调用父类构造器，因此我们还是要从 ZooKeeper 类的构造器入手，来进行分析
        zk = new ZooKeeperAdmin(host, Integer.parseInt(cl.getOption("timeout")), new MyWatcher(), readOnly, clientConfig);
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        ZooKeeperMain main = new ZooKeeperMain(args);
        //main 方法对一个一个线程，这里的 run() 方法并非继承于 Thread.run()，之所以这样调用是为了突出这是一个(main)线程
        main.run();
    }

    public ZooKeeperMain(String[] args) throws IOException, InterruptedException {
        cl.parseOptions(args);//解析入口参数
        System.out.println("Connecting to " + cl.getOption("server"));
        connectToZK(cl.getOption("server"));
    }

    public ZooKeeperMain(ZooKeeper zk) {
        this.zk = zk;
    }
    //此方法作为 ZooKeeperMain 的 Main Loop 而存在
    void run() throws IOException, InterruptedException {

        if (cl.getCommand() == null) {
            System.out.println("Hello Spongecaptain!");
            System.out.println("Welcome to ZooKeeper!");

            boolean jlinemissing = false;
            // only use jline if it's in the classpath
            try {
                /**
                 * JLine 是一个用来处理控制台(命令行)输入的 Java 类库，但并不是以纯 Java 语言编写，而是部分基于操作系统平台
                 * JLine 并不是我们学习的重点，我们完全可以将其考虑为一个命令行命令接收工具
                 * 知道 JLine 的在 ZooKeeper Client 的入口为 executeLine(line); 方法即可
                 */

                Class<?> consoleC = Class.forName("jline.console.ConsoleReader");
                Class<?> completorC = Class.forName("org.apache.zookeeper.JLineZNodeCompleter");

                System.out.println("JLine support is enabled");

                Object console = consoleC.getConstructor().newInstance();

                Object completor = completorC.getConstructor(ZooKeeper.class).newInstance(zk);
                Method addCompletor = consoleC.getMethod("addCompleter", Class.forName("jline.console.completer.Completer"));
                addCompletor.invoke(console, completor);

                String line;
                Method readLine = consoleC.getMethod("readLine", String.class);
                //下面循环中的 getPrompt() 方法主要用于显示命令行前缀，例如："[zk: localhost:2181,localhost:2182,localhost:2183(CONNECTED) 0]"
                while ((line = (String) readLine.invoke(console, getPrompt())) != null) {
                    executeLine(line);//这里 ZooKeeper 命令行工具接收到一条命令，例如 "ls /"
                }
            } catch (ClassNotFoundException
                | NoSuchMethodException
                | InvocationTargetException
                | IllegalAccessException
                | InstantiationException e
            ) {
                LOG.debug("Unable to start jline", e);
                jlinemissing = true;
            }

            if (jlinemissing) {
                System.out.println("JLine support is disabled");
                BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

                String line;
                while ((line = br.readLine()) != null) {
                    executeLine(line);
                }
            }
        } else {
            // Command line args non-null.  Run what was passed.
            processCmd(cl);
        }
        ServiceUtils.requestSystemExit(exitCode);
    }

    public void executeLine(String line) throws InterruptedException, IOException {
        if (!line.equals("")) {//要求命令行非空，不能是单纯的空格或者回车
            //解析命令
            cl.parseCommand(line);
            //将执行过的命令记录下来，这使得我们能够在命令行中输入 history 命令得到命令的执行记录
            addToHistory(commandCount, line);
            processCmd(cl);//处理命令（重点）
            commandCount++;
        }
    }

    protected boolean processCmd(MyCommandOptions co) throws IOException, InterruptedException {
        boolean watch = false;
        try {
            //processZKCmd(MyCommandOptions co)方法的返回值主要用于测试，这里我们可以专注其处理逻辑
            watch = processZKCmd(co);
            exitCode = ExitCode.EXECUTION_FINISHED.getValue();
        } catch (CliException ex) {
            exitCode = ex.getExitCode();
            System.err.println(ex.getMessage());
        }
        return watch;
    }

    /**
     * @param co co 存储了命令行的各个选项，例如我们输入的是 "ls /" 命令，那么其内存存储了两个选项："ls"、"/"，
     *           还包括其他隐式参数，例如服务器列表、超时时间等参数
     * @return
     * @throws CliException
     * @throws IOException
     * @throws InterruptedException
     */
    protected boolean processZKCmd(MyCommandOptions co) throws CliException, IOException, InterruptedException {
        //这个 args 字符串数组即为输入的完整命令的各个元素，例如 create /foobar hello -e，将会被按照空格
        //依次拆分为 "create"、"/foobar"、"hello"、"-e" 字符串数组
        String[] args = co.getArgArray();
        //cmd 单纯指的是命令，例如 "get /cmd" 对应的 cmd 为 get，"set /foo 123" 对应的 cmd 为 set
        String cmd = co.getCommand();
        if (args.length < 1) {
            usage();
            throw new MalformedCommandException("No command entered");
        }
        //检查输入的 cmd 命令是否合法，否则执行 usage() 方法
        if (!commandMap.containsKey(cmd)) {
            usage();//usage() 方法在命令行输入不合法时，通过 System.err.println() 打印出合法的命令（红色）
            throw new CommandNotFoundException("Command not found " + cmd);
        }

        boolean watch = false;

        LOG.debug("Processing {}", cmd);
        //下面则是处理各种类型的命令行命令，包括 quit、redo、history、printwatches、
        if (cmd.equals("quit")) {
            zk.close();
            ServiceUtils.requestSystemExit(exitCode);
        } else if (cmd.equals("redo") && args.length >= 2) {
            Integer i = Integer.decode(args[1]);
            if (commandCount <= i || i < 0) { // don't allow redoing this redo
                throw new MalformedCommandException("Command index out of range");
            }
            cl.parseCommand(history.get(i));
            if (cl.getCommand().equals("redo")) {
                throw new MalformedCommandException("No redoing redos");
            }
            history.put(commandCount, history.get(i));
            processCmd(cl);
        } else if (cmd.equals("history")) {
            for (int i = commandCount - 10; i <= commandCount; ++i) {
                if (i < 0) {
                    continue;
                }
                System.out.println(i + " - " + history.get(i));
            }
        } else if (cmd.equals("printwatches")) {
            if (args.length == 1) {
                System.out.println("printwatches is " + (printWatches ? "on" : "off"));
            } else {
                printWatches = args[1].equals("on");
            }
        } else if (cmd.equals("connect")) {
            if (args.length >= 2) {
                connectToZK(args[1]);
            } else {
                connectToZK(host);
            }
        }

        // Below commands all need a live connection
        if (zk == null || !zk.getState().isAlive()) {
            System.out.println("Not connected");
            return false;
        }
        // 如果不是上述所有命令，那么就通过 commandMap 来进行执行
        // execute from commandMap
        CliCommand cliCmd = commandMapCli.get(cmd);
        if (cliCmd != null) {
            cliCmd.setZk(zk);
            //执行命令的主要逻辑,CliCommand 抽象类有非常多的具体子类
            //例如 create 方法对应 CreateCommand 类，而 getAcl 对应 GetAclCommand 类
            //我们这里以 create 方法为例进行说明(以 CreateCommand 类为例)
            watch = cliCmd.parse(args).exec();
        } else if (!commandMap.containsKey(cmd)) {
            usage();
        }
        return watch;
    }

}
