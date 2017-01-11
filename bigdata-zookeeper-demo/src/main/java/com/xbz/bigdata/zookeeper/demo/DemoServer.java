package com.xbz.bigdata.zookeeper.demo;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Shell.ExitCodeException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * 模拟服务器端
 * @author 许宝众
 *
 */
public class DemoServer implements Runnable{
	/**服务器名称**/
	private String serverName;
	/**服务器IP**/
	private String serverIP;
	/**服务器端口**/
	private String serverPort;
	/**zookeeper地址**/
	public static final String conStr="hadoop01:2181";
	public static final String SERVERS_PATH="/servers";
	public static final String SERVERS_PATH_DESCRIBE="服务器管理父节点";
	public static final int sessionTimeOut=1000*10*60;
	public static  boolean isConnected=false;
	public DemoServer(String serverName, String serverIP, String serverPort) {
		super();
		this.serverName = serverName;
		this.serverIP = serverIP;
		this.serverPort = serverPort;
	}
	@Override
	public void run() {
		//注册服务器
		try {
			registerServer();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		System.out.println("服务注册成功");
	}
	public static void main(String[] args) throws Exception {
		if(args.length!=3){
			System.err.println("参数不合法");
			System.err.println("参数分别为：[服务器名称] [服务器IP] [服务器端口]");
			System.exit(-1);
		}
		new Thread(new DemoServer(args[0],args[1],args[2])).start();
		System.in.read();
	}
	/**
	 * 向zookeeper注册服务器
	 * @throws Exception 
	 */
	private void registerServer() throws Exception{
		
		ZooKeeper zookeeper = new ZooKeeper(conStr, sessionTimeOut, new Watcher(){

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
		System.out.println("连接成功");
		Stat exists = zookeeper.exists(SERVERS_PATH, true);
		//节点不存在
		if(exists==null){
			//创建一个持久的节点
			zookeeper.create(SERVERS_PATH, SERVERS_PATH_DESCRIBE.getBytes("utf-8"), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
		//注册服务，创建子节点信息
		String serverInfo="serverName:"+serverName;
		serverInfo+="\b"+"serverIP:"+serverIP;
		serverInfo+="\b"+"serverPort:"+serverPort;
		System.out.println("准备前往zookeeper注册服务-->"+serverInfo);
		/*
		 * Ids.CREATOR_ALL_ACL : 创建者用户所有访问权限
		 * Ids.OPEN_ACL_UNSAFE : 匿名访问支持，并拥有所有控制权限
		 * Ids.READ_ACL_UNSAFE : 匿名只读权限
		 */
		//临时节点，服务器离线会自动删除节点
		String actPath = zookeeper.create(SERVERS_PATH+"/"+serverName, serverInfo.getBytes("utf-8"), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		System.out.println("服务注册成功-->"+actPath);
	} 
}
