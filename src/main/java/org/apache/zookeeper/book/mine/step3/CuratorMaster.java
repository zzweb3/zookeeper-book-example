package org.apache.zookeeper.book.mine.step3;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;

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

	public CuratorMaster(String myId, String hostPort, RetryPolicy retryPolicy) {
		this.myId = myId;
		this.client = CuratorFrameworkFactory.newClient(hostPort, retryPolicy);
		this.leaderSelector = new LeaderSelector(this.client, "/master", this);
		this.workersCache = new PathChildrenCache(this.client, "/workers", true);
		this.tasksCache = new PathChildrenCache(this.client, "/tasks", true);
	}

	@Override
	public void takeLeadership(CuratorFramework curatorFramework) throws Exception {

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


	public static void main(String[] args) throws Exception {

		CuratorMaster master = new CuratorMaster(args[0], args[1], new ExponentialBackoffRetry(1000, 5) );


	}
}
