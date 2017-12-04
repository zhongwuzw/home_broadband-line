package com.chinamobile.iphelper;
import org.apache.mina.filter.codec.demux.DemuxingProtocolCodecFactory;


public class ServerProtocolCodecFactory extends DemuxingProtocolCodecFactory {
	public ServerProtocolCodecFactory(){
		super.addMessageEncoder(HttpResponseMessage.class, HttpResponseEncoder.class);
		super.addMessageDecoder(HttpRequestDecoder.class);
	}
}