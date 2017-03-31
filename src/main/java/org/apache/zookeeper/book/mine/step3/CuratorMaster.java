package org.apache.zookeeper.book.mine.step3;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.concurrent.CountDownLatch;

/**
 * author zhouwei.guo
 * date 2017/3/30.
 */
public class CuratorMaster implements LeaderSelectorListener{

	private String myId;
	private CuratorFramework client;

	private final LeaderSelector leaderSelector;

	private final PathChildrenCache workersCache;
	private final PathChildrenCache tasksCache;

	private CountDownLatch leaderLatch = new CountDownLatch(1);
	private CountDownLatch closeLatch = new CountDownLatch(1);


	public CuratorMaster(String myId, String hostPort, RetryPolicy retryPolicy) {
		this.myId = myId;
		this.client = CuratorFrameworkFactory.newClient(hostPort, retryPolicy);
		this.leaderSelector = new LeaderSelector(this.client, "/master", this);
		this.workersCache = new PathChildrenCache(this.client, "/workers", true);
		this.tasksCache = new PathChildrenCache(this.client, "/tasks", true);
	}

	@Override
	public void takeLeadership(CuratorFramework curatorFramework) throws Exception {
		client.getCuratorListenable().addListener(masterListener);
	}

	/**
	 * Called when there is a state change in the connection
	 *
	 * @param client   the client
	 * @param newState the new state
	 */
	@Override
	public void stateChanged(CuratorFramework client, ConnectionState newState) {

	}


	CuratorListener masterListener = new CuratorListener() {
		@Override
		public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception {
			switch (event.getType()) {
				case CHILDREN:
					if (event.getPath().contains("/assign")) {
						System.out.println("Successfully got a list of assignments: " +
										event.getChildren().size() +
										" tasks.");
					

					}

					break;
				case CREATE:


					break;
				case DELETE:


					break;
				case WATCHED:


					break;
				default:
					System.out.println("Default case: " + event.getType() );
			}
		}
	};



	public static void main(String[] args) throws Exception {

		CuratorMaster master = new CuratorMaster(args[0], args[1], new ExponentialBackoffRetry(1000, 5) );


	}
}
