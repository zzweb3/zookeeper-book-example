package org.apache.zookeeper.book.mine.step3;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * author zhouwei.guo
 * date 2017/4/6.
 */
public class CuratorMasterLatch {

	private String myId;
	private CuratorFramework client;
	private LeaderLatch leaderLatch;

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

	}

	public static void main(String[] args) {
		CuratorMasterLatch master = new CuratorMasterLatch(args[0], args[1], new ExponentialBackoffRetry(1000, 5));
	}
}
