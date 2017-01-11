package com.xbz.bigdata.zookeeper.demo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;

/**
 * 
 * @author 许宝众
 *
 */
public class DemoClient implements Runnable{
	public static final String conStr="hadoop01:2181";
	public static final String SERVERS_PATH="/servers";
	public static final int sessionTimeOut=1000*10*60;
	public static boolean isConnected=false;
	public static volatile List<String> serverList; 
	public static void main(String[] args) throws Exception {
		new Thread(new DemoClient()).start();
		System.in.read();
	}

	@Override
	public void run() {
		ZooKeeper zookeeper=null;
		try{
			zookeeper = new ZooKeeper(conStr, sessionTimeOut, new Watcher(){
				
				
				@Override
				public void process(WatchedEvent event) {
					System.out.println(event);
					if(event.getState()==KeeperState.SyncConnected){
						isConnected=true;
					}
				}
			});
			while(!isConnected){
				Thread.sleep(1000);
				System.out.println("等待连接成功...");
			}
		}catch(Exception e){
			throw new RuntimeException(e);
		}
		System.out.println("连接成功");
		System.out.println("获取服务列表");
		try {
			refreshServerList(zookeeper);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		System.out.println("获取服务列表完毕");
	}
	/**
	 * 刷新服务列表
	 * @throws Exception 
	 */
	private void refreshServerList(final ZooKeeper zookeeper) throws Exception{
		List<String> childrenList = zookeeper.getChildren(SERVERS_PATH, new Watcher(){
			@Override
			public void process(WatchedEvent event) {
				//监控/servers的子节点数量变化
				if(event.getType()==EventType.NodeChildrenChanged){
					try {
						refreshServerList(zookeeper);
						zookeeper.exists(SERVERS_PATH, true);
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
					System.out.println("获取服务列表完毕");
				}
			}
		});
		if(childrenList!=null){
			serverList=new ArrayList<String>(childrenList.size());
			for (String childPath : childrenList) {
				byte[] b=zookeeper.getData(SERVERS_PATH+"/"+childPath,false,null);
				serverList.add(new String(b,"UTF-8"));
			}
		}
		System.out.println(serverList);
	}
	
}
