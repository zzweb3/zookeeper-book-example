package org.apache.zookeeper.book.mine.step1;

import org.apache.zookeeper.*;

import java.io.IOException;

/**
 * author zhouwei.guo
 * date 2017/3/7.
 */
public class Client implements Watcher {

    private ZooKeeper zk;
    private String hostPort;

    public Client(String hostPort) {
        this.hostPort = hostPort;
    }


    public void startZK() throws IOException {
        zk = new ZooKeeper(this.hostPort, 15000, this);
    }


    public String queueCommand(String command) throws KeeperException, InterruptedException {
        while (true) {

            try {
                String name = zk.create("/tasks/task=",
                                        command.getBytes(),
                                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                                        CreateMode.PERSISTENT_SEQUENTIAL);
                return name;
            } catch (KeeperException.ConnectionLossException e) {

            } catch (KeeperException.NodeExistsException e) {
                // TODO: 2017/3/8 有序节点不会出现重复的
                e.printStackTrace();
            }
        }
    }


    @Override
    public void process(WatchedEvent event) {
        System.out.println(event);
    }


    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        System.out.println(args[0]);
        Client c = new Client(args[0]);

        c.startZK();

        String name = c.queueCommand(args[1]);
        System.out.println(name);

    }
}
