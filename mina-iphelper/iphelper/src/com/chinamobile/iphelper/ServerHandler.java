
/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
*    http://www.apache.org/licenses/LICENSE-2.0
*
*  Unless required by applicable law or agreed to in writing,
*  software distributed under the License is distributed on an
*  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
*  KIND, either express or implied.  See the License for the
*  specific language governing permissions and limitations
*  under the License.
*
*/
package com.chinamobile.iphelper;

import org.apache.log4j.Logger;
import org.apache.mina.core.future.IoFutureListener;
import org.apache.mina.core.service.IoHandler;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.core.session.IoSession;

/**
 * An {@link IoHandler} for HTTP.
 *
 * @author The Apache MINA Project (dev@mina.apache.org)
 * @version $Rev: 588178 $, $Date: 2007-10-25 18:28:40 +0900 (?, 25 10? 2007) $
 */
public class ServerHandler extends IoHandlerAdapter {

	static Logger logger = Logger.getLogger(ServerHandler.class);

	@Override
	public void sessionOpened(IoSession session) {
		// set idle time to 60 seconds
		session.getConfig().setIdleTime(IdleStatus.BOTH_IDLE, 60);
	}

	@Override
	public void messageReceived(IoSession session, Object message) {
		// Check that we can service the request context
		Logger.getLogger(ServerHandler.class).debug(session.toString());
		HttpResponseMessage response = null;
		String context = ((HttpRequestMessage) message).getContext().trim();
		response = IpLocation.getIPLoc(context, session, message);
		if (response != null) {
			session.write(response).addListener(IoFutureListener.CLOSE);
		} else {
			session.closeNow();
		}
		Logger.getLogger(ServerHandler.class).debug(session);
	}

	@Override
	public void sessionIdle(IoSession session, IdleStatus status) {
		// IoSessionLogger.getLogger(session).info("Disconnecting the idle.");
		session.closeNow();
	}

	@Override
	public void exceptionCaught(IoSession session, Throwable cause) {
		// IoSessionLogger.getLogger(session).warn(cause);
		session.closeNow();
	}
}
