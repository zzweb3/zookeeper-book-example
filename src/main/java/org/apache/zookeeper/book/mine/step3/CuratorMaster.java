package org.apache.zookeeper.book.mine.step3;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.book.recovery.RecoveredAssignments;
import org.apache.zookeeper.book.recovery.RecoveredAssignments.*;


import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

/**
 * author zhouwei.guo
 * date 2017/3/30.
 */
public class CuratorMaster implements Closeable, LeaderSelectorListener{

	private String myId;
	private CuratorFramework client;

	private final LeaderSelector leaderSelector;

	private final PathChildrenCache workersCache;
	private final PathChildrenCache tasksCache;

	private CountDownLatch leaderLatch = new CountDownLatch(1);
	private CountDownLatch closeLatch = new CountDownLatch(1);


	public
	CuratorMaster(String myId, String hostPort, RetryPolicy retryPolicy) {
		this.myId = myId;
		this.client = CuratorFrameworkFactory.newClient(hostPort, retryPolicy);
		this.leaderSelector = new LeaderSelector(this.client, "/master", this);
		this.workersCache = new PathChildrenCache(this.client, "/workers", true);
		this.tasksCache = new PathChildrenCache(this.client, "/tasks", true);
	}

	CountDownLatch recoveryLatch = new CountDownLatch(0);

	@Override
	public void takeLeadership(CuratorFramework curatorFramework) throws Exception {

		System.out.println("Mastership participants: " + myId + ". " + leaderSelector.getParticipants());

		/*
		 * Register listeners
		 */
		client.getCuratorListenable().addListener(masterListener);
		client.getUnhandledErrorListenable().addListener(errorsListener);

		/*
		 * Start workersCache
		 */
		workersCache.getListenable().addListener(workersCacheListener);
		workersCache.start();

		RecoveredAssignments recoveredAssignments = new RecoveredAssignments(client.getZookeeperClient().getZooKeeper());
		recoveredAssignments.recover(new RecoveryCallback() {
			@Override
			public void recoveryComplete(int rc, List<String> tasks) {
				try {
					if (RecoveryCallback.FAILED == rc) {
						System.out.println("Recovery of assigned tasks fails.");
					} else {
						recoveryLatch = new CountDownLatch(tasks.size());
						assignTasks(tasks);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}

				new Thread(new Runnable() {
					@Override
					public void run() {
						try {
							/*
							 * Wait until recovery is complete
							 */
							recoveryLatch.await();

							/*
							 * Start tasks cache
							 */
							tasksCache.getListenable().addListener(tasksCacheListener);
							tasksCache.start();

						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}).start();
			}
		});

	}

	/**
	 * Called when there is a state change in the connection
	 *
	 * @param client   the client
	 * @param newState the new state
	 */
	@Override
	public void stateChanged(CuratorFramework client, ConnectionState newState) {
		switch (newState) {
			case CONNECTED:
				System.out.println("CONNECTED");

				break;
			case RECONNECTED:
				System.out.println("RECONNECTED");

				break;
			case SUSPENDED:
				System.out.println("SUSPENDED");

				break;
			case LOST:
				System.out.println("LOST");
				try {
					close();
				} catch (IOException e) {
					e.printStackTrace();
				}

				break;
			case READ_ONLY:


				break;
		}
	}


	CuratorListener masterListener = new CuratorListener() {
		@Override
		public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception {
			switch (event.getType()) {
				case CHILDREN:
					System.out.println("CHILDREN");
					System.out.println("Path: " + event.getPath());
					// TODO: 2017/4/1 worker or task ???
					if (event.getPath().contains("/assign")) {
						System.out.println("Successfully got a list of assignments: " +	event.getChildren().size() + " tasks.");

						/*
						 * Delete the assignments of the absent worker
						 */
						for (String task : event.getChildren()) {
							deleteAssignment(event.getPath() + "/" + task);
						}

						/*
						 * Delete the znode representing the absent worker in the assignments
						 */
						deleteAssignment(event.getPath());

						/*
						 * Reassign the tasks
						 */
						assignTasks(event.getChildren());

					} else {
						System.out.println("Unexpected event: " + event.getPath());
					}

					break;
				case CREATE:
					System.out.println("CREATE");
					/*
					 * Result of a  create operation when assigning
					 */
					if (event.getPath().contains("/assign")) {
						System.out.println("Task assign corrently: " + event.getName());
						deleteTask(event.getPath().substring(event.getPath().lastIndexOf('-') + 1));
					}

					break;
				case DELETE:
					System.out.println("DELETE");
					/*
					* We delete znode in two occasions.
					* 1- When reasssign tasks due to a faulty worker;
					* 2- Once we have assigned a task, we remove it from the list of pending tasks;
					*/
					if (event.getPath().contains("/tasks")) {
						System.out.println("Result of delete operation: " + event.getResultCode() + ". " + event.getPath());
					} else if (event.getPath().contains("/assign")) {
						System.out.println("Task correctly deleted: " + event.getPath());
					}

					break;
				case WATCHED:
					System.out.println("WATCHED");


					break;
				default:
					System.out.println("Default case: " + event.getType() );
			}
		}

	};

	PathChildrenCacheListener workersCacheListener = new PathChildrenCacheListener() {
		@Override
		public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) {
			if (PathChildrenCacheEvent.Type.CHILD_REMOVED == event.getType()) {
				/*
				 * Obtain just the worker's name
				 */
				try {
					getAbsentWorkerTasks(event.getData().getPath().replaceFirst("/workers/", ""));
				} catch (Exception e) {
					System.out.println("Exception while trying to re-assign tasks. " + e.getMessage());
					e.printStackTrace();
				}
			}
		}
	};


	PathChildrenCacheListener tasksCacheListener = new PathChildrenCacheListener() {
		@Override
		public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) {
			if (PathChildrenCacheEvent.Type.CHILD_ADDED == event.getType()) {
				try {
					assignTask(event.getData().getPath().replaceFirst("/tasks/", ""), event.getData().getData());
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	};


	private void getAbsentWorkerTasks(String worker) throws Exception {
		/*
		 * Get assigned tasks
		 */
		client.getChildren().inBackground().forPath("/assign/"+ worker);
	}


	private void assignTasks(List<String> tasks) throws Exception {
		for (String task : tasks) {
			assignTask(task, client.getData().forPath("/tasks/" + task));
		}
	}

	/*
	 * Random variable wu use to select a worker to perform a pending task.
	 */
	Random rand = new Random(System.currentTimeMillis());

	private void assignTask(String task, byte[] data) throws Exception {
		/*
		 * Choose worker at random
		 */
		List<ChildData> workersList = workersCache.getCurrentData();

		System.out.println("Assigning task: " + task + ", data: " + new String(data));

		String designatedWorker = workersList.get(rand.nextInt(workersList.size())).getPath().replaceFirst("/workers/", "");

		/*
		 * Assign task to randomly chosen worker.
		 */
		String path = "/assign/" + designatedWorker + "/" + task;
		createAssignment(path, data);
	}

	private void createAssignment(String path, byte[] data) throws Exception {
		/*
		 * The default ACL is ZooDefs.Ids#OPEN_ACL_UNSAFE
		 */
		client.create().withMode(CreateMode.PERSISTENT).inBackground().forPath(path, data);
	}


	private void deleteTask(String number) throws Exception {
		System.out.println("Deleting task: " + number);
		client.delete().inBackground().forPath("/tasks/task-" + number);
		recoveryLatch.countDown();
	}


	UnhandledErrorListener errorsListener = new UnhandledErrorListener() {
		@Override
		public void unhandledError(String message, Throwable e) {
			e.printStackTrace();
			try {
				close();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}
	};



	private void deleteAssignment(String path) throws Exception {
		/*
		* delete assignments
		*/
		client.delete().inBackground().forPath(path);
	}


	private void startZK() {
		client.start();
	}


	private void bootstrap() throws Exception {
		client.create().forPath("/workers", new byte[0]);
		client.create().forPath("/assign", new byte[0]);
		client.create().forPath("/tasks", new byte[0]);
		client.create().forPath("/status", new byte[0]);
	}

	private void runForMaster() {
		leaderSelector.setId(myId);
		//leaderSelector.autoRequeue();
		leaderSelector.start();
	}

	@Override
	public void close() throws IOException {
		System.out.println("Closeing");
		closeLatch.countDown();
		leaderSelector.close();
		client.close();
	}

	public static void main(String[] args) {
		try {
			CuratorMaster master = new CuratorMaster(args[0], args[1], new ExponentialBackoffRetry(1000, 5) );
			master.startZK();
			master.bootstrap();
			master.runForMaster();

			Thread.sleep(60000);

			//清除znode
			ZooKeeper zk = master.client.getZookeeperClient().getZooKeeper();
			Op opMaster = Op.delete("/master", -1);
			zk.multi(Arrays.asList(opMaster));

			List<Op> tasksOps = Arrays.asList();
			List<String> tasksChildren = zk.getChildren("/tasks", false);
			for (String child : tasksChildren) {
				tasksOps.add(Op.delete("/tasks/"+child, -1));
			}
			zk.multi(tasksOps);

			zk.delete("/assign", -1);
			zk.delete("/status", -1);
			zk.delete("/workers", -1);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
