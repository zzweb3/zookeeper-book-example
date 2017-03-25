package org.apache.zookeeper.book.mine.step0;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Random;


public class Master implements Watcher {


    private ZooKeeper zk;
    private String hostPort;

    private Random random = new Random(this.hashCode());
    private String serverId = Integer.toHexString(random.nextInt());

    private boolean isLeader = false;

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
    public boolean checkMaster() {
        while (true) {
            try {
                Stat stat = new Stat();
                byte[] data = zk.getData("/master",
                                            false,
                                            stat);
                isLeader = new String(data).equals(serverId);
                return isLeader;
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (KeeperException e) {
                if (e instanceof KeeperException.NoNodeException) {
                    // no master, so try create again.
                    return false;
                } else if (e instanceof KeeperException.ConnectionLossException) {

                }
            }
        }
    }


    /**
     * Tries to create a /master lock znode acquire leadership.
     *
     */
    public void runForMaster() throws InterruptedException {
        while (true) {
            try {
                zk.create("/master",
                        serverId.getBytes(),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.EPHEMERAL);
                isLeader = true;
                break;
            } catch (KeeperException e) {
                if (e instanceof KeeperException.NodeExistsException) {
                    isLeader = false;
                    break;
                } else if (e instanceof KeeperException.ConnectionLossException) {

                }
            }
            if (checkMaster()) break;
        }

    }

    
    /**
     * Main method providing an example of how to run the master.
     * 
     * @param args
     * @throws Exception
     */
    public static void main(String args[]) throws Exception { 
        Master m = new Master(args[0]);
        m.startZK();

        m.runForMaster();

        if (m.isLeader) {
            System.out.println("i'm the leader.");
            //wait fo a bit
            Thread.sleep(6000);
        } else {
            System.out.println("someone else is the leader.");
        }

        m.stopZK();
    }

}
