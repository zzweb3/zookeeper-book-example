package org.apache.zookeeper.book.mine.step3;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.zookeeper.CreateMode;

/**
 * author zhouwei.guo
 * date 2017/3/30.
 */
public class CuratorMaster {




	public static void main(String[] args) throws Exception {
		CuratorFramework zkc = CuratorFrameworkFactory.newClient("", null);
		zkc.create().withMode(CreateMode.EPHEMERAL).inBackground().forPath("/myPath", new byte[0]);

	}
}
