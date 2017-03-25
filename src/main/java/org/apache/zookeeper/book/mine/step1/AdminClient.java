package org.apache.zookeeper.book.mine.step1;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Date;

/**
 * author zhouwei.guo
 * date 2017/3/8.
 */
public class AdminClient implements Watcher {

    private ZooKeeper zk;
    private String hostPort;


    public AdminClient(String hostPort) {
        this.hostPort = hostPort;
    }


    private void startZK() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
    }


    public void listStat() throws InterruptedException, KeeperException {
        //master
        try {
            Stat stat = new Stat();
            byte[] masterData = new byte[0];
            masterData = zk.getData("/master", false, stat);
            Date startTime = new Date(stat.getCtime());
            System.out.println("Master: " + new String(masterData) + " since " + startTime);
        } catch (KeeperException e) {
            if (e instanceof KeeperException.NoNodeException) {
                System.out.println("No Master.");
            } else {
                e.printStackTrace();
            }
        }

        //workers
        for (String w : zk.getChildren("/workers", false)) {
            byte[] data = zk.getData("/workers/" + w, false, null);
            System.out.println("\t" + w + ": " + new String(data));
        }

        //tasks
        for (String t : zk.getChildren("/tasks", false)) {
            byte[] data = zk.getData("/tasks/" + t, false, null);
            System.out.println("\t" + t + ": " + new String(data));
        }
    }


    @Override
    public void process(WatchedEvent event) {
        System.out.println(event);
    }


    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        AdminClient c = new AdminClient(args[0]);

        c.startZK();

        c.listStat();

        Thread.sleep(3000);
    }
}
