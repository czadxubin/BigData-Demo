package com.xbz.bigdata.netty.demo;

import java.net.SocketAddress;
import java.nio.charset.Charset;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;

public class CustomServerChannelHandler extends SimpleChannelHandler{

	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
			throws Exception {
		Object message = e.getMessage();
		String revMsg=null;
		if(message instanceof ChannelBuffer){
			ChannelBuffer revBuffer=(ChannelBuffer) message;
			revMsg=revBuffer.toString(Charset.defaultCharset());
		}
		SocketAddress remoteAddress = e.getRemoteAddress();
		System.out.println("Client remoteAddress-->"+remoteAddress);
		System.out.println("Server received Message-->"+revMsg);
		System.out.println("----------------------------------------------------------");
		ChannelBuffer resMsg = ChannelBuffers.copiedBuffer("permit sign in".getBytes());
		Channel channel = e.getChannel();
		if(channel.isWritable()){
			ChannelFuture channelFuture = channel.write(resMsg,remoteAddress);
			channelFuture.sync();
			channel.close();
		}else{
			System.out.println("channel is can't writable");
		}
	}
	@Override
	public void handleDownstream(ChannelHandlerContext ctx, ChannelEvent e)
			throws Exception {
		// TODO Auto-generated method stub
		super.handleDownstream(ctx, e);
	}
}
