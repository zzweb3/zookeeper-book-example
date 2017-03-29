package org.apache.zookeeper.book.mine.step2;

import lombok.Data;
import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher.Event.*;
import org.apache.zookeeper.AsyncCallback.*;
import org.apache.zookeeper.KeeperException.*;
import org.apache.zookeeper.data.Stat;
import org.omg.IOP.TAG_ALTERNATE_IIOP_ADDRESS;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

/**
 * author zhouwei.guo
 * date 2017/3/28.
 */
public class Client implements Watcher, Closeable{

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
	public void close() throws IOException {
		System.out.println("Client closing.");
		try {
			zk.close();
		} catch (InterruptedException e) {
			System.out.println("Zookeeper interrupted while closeing.");
			e.printStackTrace();
		}
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

	private void submitTask(String task, TaskObject taskCtx) {
		taskCtx.setTask(task);
		zk.create("/tasks/task-",
				task.getBytes(),
				ZooDefs.Ids.OPEN_ACL_UNSAFE,
				CreateMode.PERSISTENT_SEQUENTIAL,
				createTaskCallback,
				taskCtx);
	}

	StringCallback createTaskCallback = new StringCallback() {
		@Override
		public void processResult(int rc, String path, Object ctx, String name) {
			switch (Code.get(rc)) {
				case CONNECTIONLOSS:
					submitTask(((TaskObject)ctx).getTask(), (TaskObject) ctx);

					break;
				case OK:
					System.out.println("My created task name: " + name);
					((TaskObject)ctx).setTaskName(name);
					watchStatus(name.replace("/tasks/", "/status/"), (TaskObject) ctx);

					break;
				default:
					System.out.println("Something went wrong." + KeeperException.create(Code.get(rc), path));

			}
		}
	};

	private ConcurrentHashMap<String, TaskObject> ctxMap = new ConcurrentHashMap<>();

	private void watchStatus(String path, TaskObject ctx) {
		ctxMap.put(path, ctx);
		zk.exists(path,
				statusWatcher,
				exitsCallback,
				ctx);
	}

	StatCallback exitsCallback = new StatCallback() {
		@Override
		public void processResult(int rc, String path, Object ctx, Stat stat) {
			switch (Code.get(rc)) {
				case CONNECTIONLOSS:
					watchStatus(path, (TaskObject) ctx);

					break;
				case OK:
					if (stat != null) {
						zk.getData(path,
								false,
								getDataCallback,
								ctx);
					}

					break;
				case NONODE:

					break;
				default:
					System.out.println("Something went wrong. check is the status node exists: " + KeeperException.create(Code.get(rc), path));

			}

		}
	};


	Watcher statusWatcher = new Watcher() {

		@Override
		public void process(WatchedEvent event) {
			if (EventType.NodeCreated == event.getType()) {
				assert event.getPath().contains("/status/task-");
				assert ctxMap.containsKey(event.getPath());

				zk.getData(event.getPath(),
						false,
						getDataCallback,
						ctxMap.get(event.getPath()));

			}

		}
	};

	DataCallback getDataCallback = new DataCallback() {

		@Override
		public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
			switch (Code.get(rc)) {
				case CONNECTIONLOSS:
					zk.getData(path,
							false,
							getDataCallback,
							ctxMap.get(path));

					break;
				case OK:
					String taskResult = new String(data);
					System.out.println("Task " + path + ", " + taskResult);

					assert ctxMap != null;
					((TaskObject)ctx).setStatus(taskResult.contains("done"));

					zk.delete(path,
							-1,
							taskDeleteCallback,
							null);
					ctxMap.remove(path);

					break;
				case NONODE:
					System.out.println("status node is gone!");

					break;
				default:
					System.out.println("Something went wrong. " + KeeperException.create(Code.get(rc), path));
			}
		}
	};


	VoidCallback taskDeleteCallback = new VoidCallback() {
		@Override
		public void processResult(int rc, String path, Object ctx) {
			switch (Code.get(rc)) {
				case CONNECTIONLOSS:
					zk.delete(path,
							-1,
							taskDeleteCallback,
							null);

					break;
				case OK:
					System.out.println("Successfully deleted " + path);

					break;
				default:
					System.out.println("Something went wrong ." + KeeperException.create(Code.get(rc), path));
			}
		}
	};


	@Data
	static class TaskObject {
		private String task;
		private String taskName;
		private boolean done = false;
		private boolean successful = false;

		private CountDownLatch latch = new CountDownLatch(1);

		void setStatus (boolean status) {
			successful = status;
			done = true;
			latch.countDown();
		}

		void waitUntilDone() {
			try {
				latch.await();
			} catch (InterruptedException e) {
				System.out.println("InterruptedException while waiting for task to get done. ");
				e.printStackTrace();
			}
		}


	}

	public static void main(String[] args) throws Exception {
		Client c = new Client(args[0]);
		c.statZK();

		while (!c.isConnected()) {
			Thread.sleep(100);
		}

		TaskObject task1 = new TaskObject();
		TaskObject task2 = new TaskObject();

		c.submitTask("Sample task", task1);
		c.submitTask("Another sample task", task2);

		task1.waitUntilDone();
		task2.waitUntilDone();


	}
}
