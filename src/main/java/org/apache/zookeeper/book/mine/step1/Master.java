package org.apache.zookeeper.book.mine.step1;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;


public class Master implements Watcher {
    private static final Logger LOG = LoggerFactory.getLogger(Master.class);

    private ZooKeeper zk;
    private String hostPort;

    private Random random = new Random(this.hashCode());
    private String serverId = Integer.toHexString(random.nextInt());

    private static boolean isLeader = false;

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
        System.out.println(e);
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
                    try {
                        runForMaster();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    break;
            }
        }
    };


    /**
     * Tries to create a /master lock znode acquire leadership.
     *
     */
    public void runForMaster() throws InterruptedException {
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
                    isLeader = true;
                    break;
                default:
                    isLeader = false;
            }
            System.out.println("back: I'm " + (isLeader ? "" : "not ") + "the leader.");
        }
    };

    public void bootstap() {
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
                    System.out.println("parent created");
                    break;

                case NODEEXISTS:
                    System.out.println("Parent already registered: " + path);
                    break;
                default:
                    System.out.println("something went wrong:" + KeeperException.create(KeeperException.Code.get(rc), path));
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
        Master m = new Master(args[0]);
        m.startZK();

        m.bootstap();

        m.runForMaster();

        //wait fo a bit
        Thread.sleep(3000);

        if (isLeader) {
            System.out.println("i'm the leader.");
        } else {
            System.out.println("someone else is the leader.");
        }

        m.stopZK();
    }

}
