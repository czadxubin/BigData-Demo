package com.xbz.bigdata.netty.demo;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.DefaultChannelPipeline;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

public class DemoNioServer {
	public static void main(String[] args) {
		NioServerSocketChannelFactory channelFactory=
				new NioServerSocketChannelFactory(
						Executors.newCachedThreadPool(),
						Executors.newCachedThreadPool());
		ServerBootstrap serverBootStrap=new ServerBootstrap(channelFactory);
		serverBootStrap.setPipelineFactory(new ChannelPipelineFactory() {
			
			@Override
			public ChannelPipeline getPipeline() throws Exception {
				return Channels.pipeline(new CustomServerChannelHandler());
			}
		});
		serverBootStrap.setOption("child.tcpNoDelay", true);
		serverBootStrap.setOption("child.keepAlive", true);
		serverBootStrap.bind(new InetSocketAddress(8080));
		System.out.println("启动成功");
	}
}
