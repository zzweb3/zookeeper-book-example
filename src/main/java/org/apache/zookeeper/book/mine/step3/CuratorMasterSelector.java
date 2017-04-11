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
import org.apache.zookeeper.book.recovery.RecoveredAssignments;
import org.apache.zookeeper.book.recovery.RecoveredAssignments.*;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

/**
 * author zhouwei.guo
 * date 2017/4/7.
 */
public class CuratorMasterSelector implements Closeable, LeaderSelectorListener{

	private String myId;
	private CuratorFramework client;
	private final LeaderSelector leaderSelector;
	private final PathChildrenCache workersCache;
	private final PathChildrenCache tasksCache;

	/*
	 * Random variable we use to select a worker to perform a pending task.
	 */
	private Random rand = new Random(System.currentTimeMillis());

	private CountDownLatch recoveryLatch = new CountDownLatch(0);
	private CountDownLatch closeLatch = new CountDownLatch(0);


	public CuratorMasterSelector(String myId, String hostPort, RetryPolicy retryPolicy) {
		this.myId = myId;
		this.client = CuratorFrameworkFactory.newClient(hostPort, retryPolicy);

		this.leaderSelector = new LeaderSelector(this.client, "/master", this);
		this.workersCache = new PathChildrenCache(this.client, "/workers", true);
		this.tasksCache = new PathChildrenCache(this.client, "/tasks", true);
	}

	@Override
	public void takeLeadership(CuratorFramework client) throws Exception {
		System.out.println("Mastership participants: " + myId + leaderSelector.getParticipants());

		/*
		 * Start workersCache
		 */
		workersCache.getListenable().addListener(workersCacheListener);
		workersCache.start();

		final RecoveredAssignments recoveredAssignments = new RecoveredAssignments(client.getZookeeperClient().getZooKeeper());
		recoveredAssignments.recover(new RecoveredAssignments.RecoveryCallback() {
			@Override
			public void recoveryComplete(int rc, List<String> tasks) {
				try {
					if (RecoveryCallback.FAILED == rc) {
						System.out.println("Recovery of assigned tasks failed.");
					} else {
						recoveryLatch = new CountDownLatch(tasks.size());
						assignTasks(tasks);

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
								 * Starts tasks cache
								 */
								tasksCache.getListenable().addListener(tasksCacheListener);
								tasksCache.start();

							} catch (Exception e) {
								e.printStackTrace();
							}
						}
					}).start();

				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});

		closeLatch.countDown();
	}

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
					System.out.println("Exception while closing. " + e.getMessage());
					e.printStackTrace();
				}
				break;
			case READ_ONLY:
				System.out.println("READ_ONLY");

				break;
		}
	}


	@Override
	public void close() throws IOException {
		closeLatch.countDown();
		leaderSelector.close();
		client.close();
	}

	private void startZK() {
		client.start();
	}

	private void bootStrap() throws Exception {
		client.create().forPath("/workers", new byte[0]);
		client.create().forPath("/assign", new byte[0]);
		client.create().forPath("/tasks", new byte[0]);
		client.create().forPath("/status", new byte[0]);
	}


	private void runForMaster() {
		/*
		 * Register listeners
		 */
		client.getCuratorListenable().addListener(masterListener);
		client.getUnhandledErrorListenable().addListener(errorsListener);

		/*
		 * Starting master
		 */
		leaderSelector.setId(myId);
		leaderSelector.start();
	}


	CuratorListener masterListener = new CuratorListener() {
		@Override
		public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception {
			System.out.println("Event path: " + event.getPath());
			switch (event.getType()) {
				case CHILDREN:
					if (event.getPath().contains("/assign")) {
						System.out.println("Succesfully got a list of assignments:" + event.getChildren().size() + " task.");

						/*
						 * Delete the assignments of the absent worker
						 */
						for (String task : event.getChildren()) {
							deleteAssignment(event.getPath() + "/" + task);
						}

						/*
						 * Delete the znode representing the absent worker
						 */
						deleteAssignment(event.getPath());

						/*
						 * Reassign the tasks
						 */
						assignTasks(event.getChildren());

					} else {
						System.out.println("Unexcepted event: " + event.getPath());
					}

					break;
				case CREATE:
					if (event.getPath().contains("/assign")) {
						System.out.println("Task assigned correctly: " + event.getName());
						deleteTask(event.getPath().substring(event.getPath().lastIndexOf('-') + 1));
					}

					break;
				case DELETE:
					if (event.getPath().contains("/assign")) {
						System.out.println("Task correctly delete: " + event.getPath());
					}

					break;
				case WATCHED:


					break;
				default:
					System.out.println("Default case: " + event.getType());
			}
		}

	};


	UnhandledErrorListener errorsListener = new UnhandledErrorListener() {
		@Override
		public void unhandledError(String message, Throwable t) {
			System.out.println("Unrecoverable error: " + t.getMessage());
			try {
				close();
			} catch (IOException e) {
				System.out.println("Exception when closing. " + e.getMessage());
				e.printStackTrace();
			}
		}
	};


	PathChildrenCacheListener workersCacheListener = new PathChildrenCacheListener() {
		@Override
		public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
			if (PathChildrenCacheEvent.Type.CHILD_REMOVED == event.getType()) {
				/*
				 * Obtain just the worker's name
				 */
				getAbsentWorkerTasks(event.getData().getPath().replaceFirst("/workers/", ""));
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
					System.out.println("Exception when assigning task. " + e.getMessage());
					e.printStackTrace();
				}
			}
		}
	};


	private void deleteAssignment(String path) throws Exception {
		System.out.println("Deleting assignment: " + path);
		/*
		 * Delete assignment
		 */
		client.delete().inBackground().forPath(path);
	}

	private void assignTasks(List<String> tasks) throws Exception {
		for (String task : tasks) {
			assignTask(task, client.getData().forPath("/tasks/" + task));
		}
	}

	private void assignTask(String task, byte[] data) throws Exception {
		/*
		 * Choose worker at random
		 */
		List<ChildData> workerList = workersCache.getCurrentData();
		String designatedWorker = workerList.get(rand.nextInt(workerList.size())).getPath().replaceFirst("/workers/", "");

		/*
		 * Assign task to randomly chosen worker
		 */
		String path = "/assign/" + designatedWorker + "/" + task;
		createAssignment(path, data);
	}

	private void createAssignment(String path, byte[] data) throws Exception {
		client.create().withMode(CreateMode.PERSISTENT).inBackground().forPath(path, data);
	}


	private void getAbsentWorkerTasks(String worker) throws Exception {
		/*
		 * Get assigned tasks
		 */
		client.getChildren().inBackground().forPath("/assign/" + worker);
	}

	private void deleteTask(String number) throws Exception {
		client.delete().inBackground().forPath("/tasks/task-" + number);
		recoveryLatch.countDown();
	}

	/**
	 * Main
	 */
	public static void main(String[] args) {
		try {
			CuratorMasterSelector master = new CuratorMasterSelector(args[0], args[1], new ExponentialBackoffRetry(1000, 5));
			master.startZK();
			master.bootStrap();
			master.runForMaster();

			Thread.sleep(600000);

		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
