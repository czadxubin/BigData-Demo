package com.xbz.bigdata.netty.demo;

import java.nio.charset.Charset;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;

public class CustomClientChannelHandler extends SimpleChannelHandler{
	
	@Override
	public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e)
			throws Exception {
		Channel channel = ctx.getChannel();
		if(channel.isWritable()){
			ChannelBuffer resMsg = ChannelBuffers.copiedBuffer("client is request sign in".getBytes());
			channel.write(resMsg);
		}
	}
	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
			throws Exception {
		System.out.println("接收数据...");
		Object message = e.getMessage();
		String revMsg=null;
		if(message instanceof ChannelBuffer){
			ChannelBuffer revBuffer=(ChannelBuffer) message;
			revMsg=revBuffer.toString(Charset.defaultCharset());
		}
		System.out.println("Cient messageReceived-->"+revMsg);
	}

}
