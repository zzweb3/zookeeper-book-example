package org.apache.zookeeper.book.mine.step1;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;

/**
 * author zhouwei.guo
 * date 2017/3/3.
 */
public class Worker implements Watcher {
    private static final Logger LOG = LoggerFactory.getLogger(Worker.class);

    private ZooKeeper zk;
    private String hostPort;

    private Random random = new Random(this.hashCode());
    private String serverId = Integer.toHexString(random.nextInt());

    private String status;
    private String name;

    public Worker(String hostPort) {
        this.hostPort = hostPort;
    }


    public void startZK() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
    }


    @Override
    public void process(WatchedEvent event) {
        System.out.println("xxxxxxxxxx" + event.toString() + ", " + hostPort);
        LOG.info(event.toString() + ", " + hostPort);
    }


    public void register() {
        zk.create("/workers/worker-" + serverId,
                "Idle".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL,
                createWorkerCallback, null);
    }


    AsyncCallback.StringCallback createWorkerCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    register();
                    break;

                case OK:
                    LOG.info("Registered successfully: " + serverId);
                    break;

                case NODEEXISTS:
                    LOG.info("Already registered: " + serverId);
                    break;
                default:
                    LOG.error("something went wrong: " + KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };


    AsyncCallback.StatCallback statusUpdateCallback = new AsyncCallback.StatCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    updateStatus((String) ctx);
                    return;

            }
        }
    };


    private void updateStatus(String status) {
        if (this.status.equals(status)) {
            zk.setData("/workers/worker-" + serverId, status.getBytes(), -1, statusUpdateCallback, status);
        }
    }


    public void setStatus(String status) {
        this.status = status;
        updateStatus(status);
    }


    public void printWorkerStatus() {
        zk.getData("/workers/worker-" + serverId, false, dataCallback, null);
    }


    AsyncCallback.DataCallback dataCallback = new AsyncCallback.DataCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    printWorkerStatus();
                    break;

                case OK:
                    System.out.println(new String(data));
                    break;

                default:
                    System.out.println("something went wrong.");
            }
        }
    };

    public static void main(String[] args) throws IOException, InterruptedException {
        Worker w = new Worker(args[0]);
        w.startZK();

        w.register();

        w.printWorkerStatus();

        w.setStatus("test");

        w.printWorkerStatus();

        w.updateStatus("houhou");
        w.printWorkerStatus();


        Thread.sleep(6000);
    }
}
