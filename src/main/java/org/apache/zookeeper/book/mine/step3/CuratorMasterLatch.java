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
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.book.recovery.RecoveredAssignments;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

/**
 * author zhouwei.guo
 * date 2017/4/6.
 */
public class CuratorMasterLatch implements Closeable, LeaderLatchListener{

	private String myId;
	private CuratorFramework client;
	private final LeaderLatch leaderLatch;

	private final PathChildrenCache workersCache;
	private final PathChildrenCache tasksCache;

	/*
	 * Random variable we use to select a worker to perform a pending task.
	 */
	private Random rand = new Random(System.currentTimeMillis());

	CountDownLatch recoveryLatch = new CountDownLatch(0);


	/**
	 * Creates a new Curator client, setting the retry policy to ExponentialBackoffRetry
	 *
	 * @param myId
	 * @param hostPort
	 * @param retry
	 */
	public CuratorMasterLatch(String myId, String hostPort, RetryPolicy retry) {
		this.myId = myId;
		this.client = CuratorFrameworkFactory.newClient(hostPort, retry);
		this.leaderLatch = new LeaderLatch(this.client, "/master", myId);
		this.workersCache = new PathChildrenCache(this.client, "/workers", true);
		this.tasksCache = new PathChildrenCache(this.client, "/tasks", true);
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

	private void runForMaster() throws Exception {
		/*
		 * Register listeners
		 */
		client.getCuratorListenable().addListener(masterLitener);
		client.getUnhandledErrorListenable().addListener(errorsListener);

		/*
		 * Start master election
		 */
		leaderLatch.addListener(this);
		leaderLatch.start();
	}

	CuratorListener masterLitener = new CuratorListener() {
		@Override
		public void eventReceived(CuratorFramework client, CuratorEvent event) {
			System.out.println("Event path: " + event.getPath());
			try{
				switch (event.getType()) {
					case CHILDREN:
						System.out.println("CHILDREN");
						if (event.getPath().contains("/assign")) {
							System.out.println("Successfully got a list of assigments: " + event.getChildren().size() + " tasks");

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
						}

						break;
					case CREATE:
						System.out.println("CREATE");
						/*
						 * Result of a create operation when assigning a task
						 */
						if (event.getPath().contains("/assign")) {
							System.out.println("Task assign correctly: " + event.getName());
							deleteTask(event.getPath().substring(event.getPath().lastIndexOf('-') + 1));
						}

						break;
					case DELETE:
						System.out.println("CREATE");
						/*
						 * We delete znodes in two accasions
						 * 1- When reassigning tasks due to faulty worker
						 * 2- Once we have assigned a task. we remove it from the list of pending tasks.
						*/
						if (event.getPath().contains("/tasks")) {
							System.out.println("Result of delete operation: " + event.getResultCode() + ". " + event.getPath());
						} else if (event.getPath().contains("/assign")) {
							System.out.println("Task correctly deleted: " + event.getPath());
						}

						break;
					case WATCHED:
						System.out.println("CREATE");


						break;
					default:
						System.out.println("Default case: " + event.getType());

				}
			} catch (Exception e) {
				e.printStackTrace();
				try {
					close();
				} catch (IOException e1) {
					e1.printStackTrace();
				}
			}

		}

	};


	UnhandledErrorListener errorsListener = new UnhandledErrorListener() {
		@Override
		public void unhandledError(String message, Throwable t) {
			System.out.println(t.getMessage());
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
		public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) {
			if (PathChildrenCacheEvent.Type.CHILD_REMOVED == event.getType()) {
				try {
					getAbsentWorkerTasks(event.getData().getPath().replaceFirst("/workers/", ""));
				} catch (Exception e) {
					System.out.println("Exception when assigning task. " + e.getMessage());
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
					System.out.println("Exception when assigning task. " + e.getMessage());
					e.printStackTrace();
				}
			}
		}
	};


	private void deleteTask(String number) throws Exception {
		System.out.println("Deleting task: " + number);
		client.delete().inBackground().forPath("/tasks/task-" + number);
		recoveryLatch.countDown();
	}

	private void getAbsentWorkerTasks(String worker) throws Exception {
		/*
		 * Get assigned tasks
		 */
		client.getChildren().inBackground().forPath("/assign/" + worker);
	}

	private void deleteAssignment(String path) throws Exception {
		System.out.println("Deleting assignment: " + path);
		client.delete().inBackground().forPath(path);
	}

	private void assignTasks(List<String> tasks) throws Exception {
		for (String task : tasks) {
			assignTask(task, client.getData().forPath("/tasks/" + task));
		}
	}

	private void assignTask(String task, byte[] data) throws Exception {
		/*
		 * Choose worker at random.
		 */
		List<ChildData> workersList = workersCache.getCurrentData();
		System.out.println("Assigning task: " + task + ", data: " + new String(data));

		String designatedWorker = workersList.get(rand.nextInt(workersList.size())).getPath().replaceFirst("/workers/", "");

		/*
		 * Assign task to randomly chosen worker
		 */
		String path = "/assign/" + designatedWorker + "/" + task;
		createAssignment(path, data);

	}

	private void createAssignment(String path, byte[] data) throws Exception {
		/*
		 * The default ACL is ZooDefs.Ids#OPEN+ACL_UNSAFE
		 */
		client.create().withMode(CreateMode.PERSISTENT).inBackground().forPath(path, data);

	}

	@Override
	public void isLeader() {
		try {
			/*
			 * Start workersCache
			 */
			workersCache.getListenable().addListener(workersCacheListener);
			workersCache.start();

			RecoveredAssignments recoveredAssignments = new RecoveredAssignments(client.getZookeeperClient().getZooKeeper());
			recoveredAssignments.recover(new RecoveredAssignments.RecoveryCallback() {
				@Override
				public void recoveryComplete(int rc, List<String> tasks) {
					try {
						if (RecoveredAssignments.RecoveryCallback.FAILED == rc) {
							System.out.println("Recovery of assigned tasks failed.");
						} else {
							System.out.println("Assigning recoveryed tasks");
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
									 * Start tasks cache
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


		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void notLeader() {
		try {
			close();
		} catch (IOException e) {
			System.out.println("Exception while closing." + e.getMessage());
			e.printStackTrace();
		}
	}

	@Override
	public void close() throws IOException {
		leaderLatch.close();
		client.close();
	}


	public static void main(String[] args) {
		try {
			CuratorMasterLatch master = new CuratorMasterLatch(args[0], args[1], new ExponentialBackoffRetry(1000, 5));
			master.startZK();
			master.bootstrap();
			master.runForMaster();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
