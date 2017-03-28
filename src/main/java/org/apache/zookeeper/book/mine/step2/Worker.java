package org.apache.zookeeper.book.mine.step2;

import org.apache.zookeeper.*;
import org.apache.zookeeper.AsyncCallback.*;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.book.ChildrenCache;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * author zhouwei.guo
 * date 2017/3/16.
 */
public class Worker implements Watcher {

    private String serverId = Integer.toHexString(new Random().nextInt());

    private String port;
    private ZooKeeper zk;

    private volatile boolean connected = false;
    private volatile boolean expired = false;

    private String status;

    private String name = "worker-" + serverId;

    private ChildrenCache assignTaskCache = new ChildrenCache();

    private ThreadPoolExecutor executor;

    public boolean isConnected() {
        return connected;
    }


    public boolean isExpired() {
        return expired;
    }


    public Worker(String port) {
        this.port = port;
        this.executor = new ThreadPoolExecutor(1,1,
                1000L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(200));
    }


    private void startZK() throws IOException {
        zk = new ZooKeeper(port, 15000, this);
    }


    @Override
    public void process(WatchedEvent event) {
        if (Event.EventType.None == event.getType()) {
            switch (event.getState()) {
                case SyncConnected:
                    connected = true;

                    break;
                case Disconnected:
                    connected = false;

                    break;
                case Expired:
                    expired = true;
                    connected = false;

                    break;
                default:
                    break;

            }
        }
    }


    private void bootstrap() {
        createAssignNode();
    }


    private void createAssignNode() {
        zk.create("/assign/" + name,
                new byte[0],
                Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL,
                assignStringCallback,
                null);
    }


    AsyncCallback.StringCallback assignStringCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    createAssignNode();

                    break;
                case OK:
                    System.out.println("Assgin node created.");

                    break;
                case NODEEXISTS:
                    System.out.println("Assign node already registered.");

                    break;
                default:
                    System.out.println("Something went wrong: " + KeeperException.create(Code.get(rc), path));
            }
        }
    };


    private void register() {
        zk.create("/workers/" + name,
                "Idle".getBytes(),
                Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL,
                workerStringCallback,
                null);
    }


    AsyncCallback.StringCallback workerStringCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    register();

                    break;
                case OK:
                    System.out.println("Registerd successfully: "+ name);

                    break;
                case NODEEXISTS:
                    System.out.println("Already registered:" + name);

                    break;
                default:
                    System.out.println("Something went wrong: " + KeeperException.create(Code.get(rc), path));
            }
        }
    };


    public void getTask() {
                zk.getChildren("/assign/" + name,
                newTaskWatcher,
                taskGetChildrenCallback,
                null);
    }

    Watcher newTaskWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            if (EventType.NodeChildrenChanged == event.getType()) {
                assert event.getPath().equals("/assign/" + name);

                getTask();
            }

        }
    };


    ChildrenCallback taskGetChildrenCallback =  new ChildrenCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, List<String> children) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    getTask();

                    break;
                case OK:
                    if (children != null) {
                        executor.execute(new Runnable() {
                            List<String> children;
                            DataCallback cb;

                            public Runnable init(List<String> children, DataCallback cb){
                                this.children = children;
                                this.cb = cb;

                                return this;
                            }

                            @Override
                            public void run() {
                                if (children == null) {
                                    return;
                                }

                                System.out.println("Looping into tasks");
                                setStatus("Working");
                                for (String task : children) {
                                    System.out.println("new task: " + task);
                                    zk.getData("/assign/" + name + "/" + task,
                                            false,
                                            cb,
                                            task);
                                }

                            }

                        }.init(assignTaskCache.addedAndSet(children), taskDataCallback));
                    }

                    break;
                default:
                    System.out.println("getChildren failed: " + KeeperException.create(Code.get(rc), path));
            }
        }
    };


    public void setStatus(String status) {
        this.status = status;
        updateStatus(status);
    }


    private synchronized void updateStatus(String status) {
        if (this.status == status) {
            zk.setData("/workers/" + name,
                    status.getBytes(),
                    -1,
                    statusUpdateCallback,
                    status);
        }
    }


    StatCallback statusUpdateCallback = new StatCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    updateStatus((String) ctx);

                    break;
            }
        }
    };


    DataCallback taskDataCallback = new DataCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    zk.getData(path, false, taskDataCallback, ctx);

                    break;
                case OK:
                    executor.execute(new Runnable() {
                        byte[] data;
                        Object ctx;

                        public Runnable init(byte[] data, Object ctx) {
                            this.data = data;
                            this.ctx = ctx;

                            return this;
                        }

                        @Override
                        public void run() {
                            System.out.println("Excute ur task: " + new String(data));
                            zk.create("/status/" + (String) ctx,
                                    "Done".getBytes(),
                                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                                    CreateMode.PERSISTENT,
                                    taskStatusCreateCallback,
                                    null);
                            zk.delete("/assign/" + name + "/" + (String) ctx,
                                    -1,
                                    taskVoidCallback,
                                    null);
                        }
                    }.init(data, ctx));

                    break;
                default:
                    System.out.println("Failed to get task data:" + KeeperException.create(Code.get(rc), path));
            }
        }
    };


    StringCallback taskStatusCreateCallback = new StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    zk.create(path,
                            "Done".getBytes(),
                            ZooDefs.Ids.OPEN_ACL_UNSAFE,
                            CreateMode.EPHEMERAL,
                            taskStatusCreateCallback,
                            null);

                    break;
                case OK:
                    System.out.println("Created status znode correctly: " + name);

                    break;
                case NODEEXISTS:
                    System.out.println("Status znode exit:" + path);

                    break;

                default:
                    System.out.println("Failed to create task data: " + KeeperException.create(Code.get(rc), path));
            }
        }
    };


    VoidCallback taskVoidCallback = new VoidCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    zk.delete(path,
                            -1,
                            taskVoidCallback,
                            null);

                    break;
                case OK:
                    System.out.println("Task correctly deleted: " + path);

                    break;
                default:
                    System.out.println("Failed to delete task data: " + KeeperException.create(Code.get(rc), path));
            }
        }
    };



    public static void main(String[] args) throws IOException, InterruptedException {

        /**
         * step_1 创建zk 会话
         */
        Worker w = new Worker(args[0]);
        w.startZK();

        while (!w.isConnected()) {
            Thread.sleep(100);
        }

        /**
         * step_2 创建 /assing/work-xxx1 节点
         */
        w.bootstrap();

        /**
         * step_3 Registers this worker so that the Master knows that it's here
         */
        w.register();

        /**
         * step_4 Getting assigned tasks.
         */
        w.getTask();

        while (!w.isExpired()) {
            Thread.sleep(1000);
        }
    }

}
