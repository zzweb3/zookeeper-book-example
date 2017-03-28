package org.apache.zookeeper.book.mine.step2;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.*;

import java.io.IOException;

/**
 * author zhouwei.guo
 * date 2017/3/28.
 */
public class Client implements Watcher{

	private String hostPort;
	private ZooKeeper zk;

	private volatile boolean connected = false;
	private volatile boolean expired = false;

	public Client(String hostPort) {
		this.hostPort = hostPort;
	}

	public boolean isExpired() {
		return expired;
	}

	public boolean isConnected() {
		return connected;
	}

	private void statZK() throws IOException {
		zk = new ZooKeeper(this.hostPort, 15000, this);
	}

	@Override
	public void process(WatchedEvent event) {
		if (EventType.None == event.getType()) {
			switch (event.getState()) {
				case SyncConnected:
					connected = true;

					break;
				case Disconnected:
					connected = false;

					break;
				case Expired:
					connected = false;
					expired = true;

					break;
				default:
					break;

			}
		}
	}


	public static void main(String[] args) throws Exception {
		Client c = new Client(args[0]);
		c.statZK();

		while (!c.isConnected()) {
			Thread.sleep(100);
		}


	}
}
