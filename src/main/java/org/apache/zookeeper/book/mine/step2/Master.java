package org.apache.zookeeper.book.mine.step2;

import org.apache.zookeeper.*;
import org.apache.zookeeper.book.ChildrenCache;
import org.apache.zookeeper.book.recovery.RecoveredAssignments;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Random;


public class Master implements Watcher {
    
    private static final Logger LOG = LoggerFactory.getLogger(Master.class);

    enum MasterStates {RUNNING, ELECTED, NOTELECTED};

    private volatile MasterStates state = MasterStates.RUNNING;

    private ZooKeeper zk;
    private String hostPort;

    private Random random = new Random(this.hashCode());
    private String serverId = Integer.toHexString(random.nextInt());

    private ChildrenCache workersCache;
    private ChildrenCache tasksCache;

    private volatile boolean connected = false;
    private volatile boolean expired = false;

    public boolean isConnected() {
        return connected;
    }


    public boolean isExpired() {
        return expired;
    }

    /**
     * Creates a new master instance.
     *
     * @param hostPort
     */
    Master(String hostPort) {
        this.hostPort = hostPort;
    }


    /**
     * Creates a new ZooKeeper session.
     *
     * @throws IOException
     */
    void startZK() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
    }


    /**
     * close the zookeeper session.
     *
     * @throws InterruptedException
     */
    void stopZK() throws InterruptedException {
        zk.close();
    }


    /**
     * This method implements the process method of the
     * Watcher interface. We use it to deal with the
     * different states of a session.
     *
     * @param e new session event to be processed
     */
    public void process(WatchedEvent e) {
        System.out.println("Processing event: " + e.toString());

        if (Event.EventType.None == e.getType()) {
            switch (e.getState()) {
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
                    System.out.println("Watcher eventType: " + e.getState());
                    break;

            }
        }
    }


    /**
     * returns true if there is a master.
     *
     * @return
     */
    public void checkMaster() {
        zk.getData("/master",
                false,
                masterCheckCallback,
                null);
    }


    AsyncCallback.DataCallback masterCheckCallback = new AsyncCallback.DataCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    checkMaster();

                    break;
                case NONODE:
                    runForMaster();

                    break;
                case OK:
                    if (serverId.equals(new String(data))) {
                        state = MasterStates.ELECTED;
                        takeLeadership();
                    } else {
                        state = MasterStates.NOTELECTED;
                        masterExits();
                    }

                    break;
                default:
                    System.out.println("checkMaster default." + KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };


    /**
     * Tries to create a /master lock znode acquire leadership.
     *
     */
    public void runForMaster() {
        System.out.println("Running for master.");
        zk.create("/master",
                serverId.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL,
                masterCreateCallback,
                null);

    }

    AsyncCallback.StringCallback masterCreateCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    checkMaster();
                    break;

                case OK:
                    state = MasterStates.ELECTED;
                    takeLeadership();
                    break;

                case NODEEXISTS:
                    state = MasterStates.NOTELECTED;
                    masterExits();
                    break;

                default:
                    state = MasterStates.NOTELECTED;
                    System.out.println("Someting went wrong when running for master.");
            }
            System.out.println("back: I'm " + (MasterStates.ELECTED == state ? "" : "not ") + "the leader " + serverId);
        }

    };


    private void takeLeadership() {
        System.out.println("Going for list of workers");
        getWorkers();

        // TODO: 2017/3/16
        new RecoveredAssignments(zk).recover(new RecoveredAssignments.RecoveryCallback() {
            @Override
            public void recoveryComplete(int rc, List<String> tasks) {
                if (RecoveredAssignments.RecoveryCallback.FAILED == rc) {
                    System.out.println("Recovery of assigned tasks failed.");
                } else {
                    System.out.println("Assigning recovered tasks.");
                    getTasks();
                }
            }
        });

    }


    public void bootstrap() {
        createParent("/workers", new byte[0]);
        createParent("/assign", new byte[0]);
        createParent("/tasks", new byte[0]);
        createParent("/status", new byte[0]);
    }


    private void createParent(String path, byte[] data) {
        zk.create(path,
                data,
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT,
                createParentCallback,
                data);
    }


    AsyncCallback.StringCallback createParentCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    createParent(path, (byte[]) ctx);
                    break;

                case OK:
                    System.out.println("parent znode " + path + " created");
                    break;

                case NODEEXISTS:
                    System.out.println("Parent already registered: " + path);
                    break;
                default:
                    System.out.println("something went wrong:" + KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };


    public void masterExits() {
        zk.exists("/master",
                masterExitsWatcher,
                masterExitsCallback,
                null);
    }


    Watcher masterExitsWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            if (event.getType().equals(Event.EventType.NodeDeleted)) {
                assert "/master".equals(event.getPath());
                runForMaster();
            }
        }
    };


    AsyncCallback.StatCallback masterExitsCallback = new AsyncCallback.StatCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    masterExits();
                    break;

                case OK:
                    if (stat != null) {
                        runForMaster();
                    }
                    break;
                case NONODE:
                    state = MasterStates.RUNNING;
                    runForMaster();
                    System.out.println("It sounds like the previous is gone. so let's run for master again.");
                default:
                    System.out.println("default. " + KeeperException.Code.get(rc));
                    checkMaster();
            }
        }
    };


    public void getWorkers() {
        zk.getChildren("/workers",
                        workersChangeWatcher,
                        workersGetChildrenCallback,
                        null);
    }


    Watcher workersChangeWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            if (event.getType() == Event.EventType.NodeChildrenChanged) {
                assert "/workers".equals(event.getPath());

                getWorkers();
            }
        }
    };


    AsyncCallback.ChildrenCallback workersGetChildrenCallback = new AsyncCallback.ChildrenCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, List<String> children) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    getWorkers();

                    break;
                case OK:
                    System.out.println("success get a list of workers:" + children.size());
                    reassignAndSet(children);

                    break;
                default:
                    System.out.println("getChild failed." + KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };


    private void reassignAndSet(List<String> children) {
        List<String> toProcess;

        if (workersCache == null) {
            workersCache = new ChildrenCache(children);
            toProcess = null;
        } else {
            System.out.println("Removing and setting.");
            toProcess = workersCache.removedAndSet(children);
        }

        if (toProcess != null) {
            for (String worker : toProcess) {
                getAbsentWorkerTasks(worker);
            }
        }
    }


    private void getAbsentWorkerTasks(String worker) {
        zk.getChildren("/assign/" + worker, false, workerAssignmentCallback, null);
    }


    AsyncCallback.ChildrenCallback workerAssignmentCallback = new AsyncCallback.ChildrenCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, List<String> children) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    getAbsentWorkerTasks(path);

                    break;
                case OK:
                    System.out.println("Successfully got a list of assignment:" + children.size() + " tasks.");

                    /**
                     * Reassign the tasks of the absent worker.
                     */
                    for (String task : children) {
                        getDataReassign(path + "/" + task, task);
                    }

                    break;
                default:
                    System.out.println("getChildren faild. " + KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }

    };


    /*
     ************************************************
     * Recovery of tasks assigned to absent worker. *
     ************************************************
     */

    /**
     * Get reassign task data.
     * @param path Path of assign task
     * @param task Task name excluding the path prefix
     */
    private void getDataReassign(String path, String task) {
        zk.getData(path,
                false,
                getDataReassignCallback,
                task);
    }


    AsyncCallback.DataCallback getDataReassignCallback = new AsyncCallback.DataCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    getDataReassign(path, (String) ctx);

                    break;
                case OK:
                    recreateTask(new RecreateTaskCtx(path, (String) ctx, data));

                    break;
                default:
                    System.out.println("Something went wrong when getting data." + KeeperException.create(KeeperException.Code.get(rc), path));

            }
        }

    };


    class RecreateTaskCtx {
        String path;
        String task;
        byte[] data;

        public RecreateTaskCtx(String path, String task, byte[] data) {
            this.path = path;
            this.task = task;
            this.data = data;
        }
    }


    private void recreateTask(RecreateTaskCtx ctx) {
        zk.create("/tasks/" + ctx.task,
                ctx.data,
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT,
                recreateTaskCallback,
                ctx);
    }


    AsyncCallback.StringCallback recreateTaskCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    recreateTask((RecreateTaskCtx) ctx);

                    break;
                case OK:
                    deleteAssignment(((RecreateTaskCtx) ctx).path);

                    break;
                case NODEEXISTS:

                    // TODO: 2017/3/13
                    recreateTask((RecreateTaskCtx) ctx);
                    break;
                default:
                    System.out.println("Something went wrong when recreating task.");
            }
        }
    };


    private void deleteAssignment(String path) {
        zk.delete(path, -1, taskDeletionCallback, null);
    }


    AsyncCallback.VoidCallback taskDeletionCallback = new AsyncCallback.VoidCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    deleteAssignment(path);

                    break;
                case OK:
                    System.out.println("Task coreectly deleted: " + path);

                    break;
                default:
                    System.out.println("Fail to delete task data "
                            + KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };


    private void getTasks() {
        zk.getChildren("/tasks",
                taskChangeWatcher,
                taskGetChildrenCallback,
                null);
    }


    Watcher taskChangeWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            if (Event.EventType.NodeChildrenChanged == event.getType()) {
                assert "/tasks".equals(event.getPath());
                getTasks();
            }
        }
    };


    AsyncCallback.ChildrenCallback taskGetChildrenCallback = new AsyncCallback.ChildrenCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, List<String> children) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    getTasks();

                    break;
                case OK:
                    List<String> toProcess;
                    if (tasksCache == null) {
                        tasksCache = new ChildrenCache(children);

                        toProcess = children;
                    } else {
                        toProcess = tasksCache.addedAndSet(children);
                    }

                    if (toProcess != null) {
                        assignTasks(toProcess);
                    }

                    break;
                default:
                    System.out.println("getTaskChildren failed. " + KeeperException.create(KeeperException.Code.get(rc), path));

            }
        }
    };


    private void assignTasks(List<String> toProcess) {
        for (String task : toProcess) {
            getTaskData(task);
        }
    }


    private void getTaskData(String task) {
        zk.getData("/tasks/" + task,
                false,
                taskDataCallback,
                task);
    }

    AsyncCallback.DataCallback taskDataCallback = new AsyncCallback.DataCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    getTaskData((String) ctx);

                    break;
                case OK:
                    /*
                     * Choose worker at random.
                     */
                    List<String> list = workersCache.getList();
                    String designateWorker = list.get(random.nextInt(list.size()));

                    /*
                     * Assign task to randomly chosen worker.
                     */
                    String assignmentPath = "/assign/" + designateWorker + "/" + (String) ctx;
                    System.out.println("Assignment path: " + assignmentPath);
                    createAssignment(assignmentPath, data);

                    break;
                default:
                    System.out.println("Error when trying to get task data." +
                        KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };



    private void createAssignment(String path, byte[] data) {
        zk.create(path,
                data,
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT,
                assignTasksCallback,
                data);
    }


    AsyncCallback.StringCallback assignTasksCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    createAssignment(path, (byte[]) ctx);

                    break;
                case OK:
                    System.out.println("Task assigned correctly: " + name);
                    deleteTask(name.substring(name.lastIndexOf("/") + 1));

                    break;
                case NODEEXISTS:
                    System.out.println("Task already assigned");

                    break;
                default:
                    System.out.println("Error when trying to assign task." +
                        KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }

    };


    private void deleteTask(String name) {
        zk.delete("/tasks/" + name, -1, taskDeleteCallback, name);
    }


    AsyncCallback.VoidCallback taskDeleteCallback = new AsyncCallback.VoidCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    deleteTask((String) ctx);

                    break;
                case OK:
                    System.out.println("Successfully deleted " + path);

                    break;
                case NONODE:
                    System.out.println("Task has been deleted already.");

                    break;
                default:
                    System.out.println("Something went wrong here. " +
                        KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };



    /**
     * Main method providing an example of how to run the master.
     * 
     * @param args
     * @throws Exception
     */
    public static void main(String args[]) throws Exception {

        /**
         * step_1 创建一个Zookeeper会话
         */
        Master m = new Master(args[0]);
        m.startZK();

        /**
         * step_2
         */
        while(m.isConnected()) {
            Thread.sleep(100);
        }

        /**
         * step_3 初始化元数据
         */
        m.bootstrap();

        /**
         * step_4 竞争Master主节点及后续处理
         */
        m.runForMaster();

        /**
         * step_5 判定zk回话是否过期
         */
        while (!m.isExpired()) {
            Thread.sleep(1000);
        }

        m.stopZK();

    }

}
