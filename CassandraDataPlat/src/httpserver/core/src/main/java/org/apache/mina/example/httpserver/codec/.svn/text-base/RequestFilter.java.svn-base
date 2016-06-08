package org.apache.mina.example.httpserver.codec;

import java.io.IOException;

import org.apache.mina.core.session.IoSession;

import com.chinamobile.httpserver.response.ActionRespFactory;
import com.chinamobile.httpserver.response.CassandraRespFactory;
import com.chinamobile.httpserver.response.CssRespFactory;
import com.chinamobile.httpserver.response.FileRespFactory;
import com.chinamobile.httpserver.response.HtmlRespFactory;
import com.chinamobile.httpserver.response.HttpRespFactory;
import com.chinamobile.httpserver.response.JsRespFactory;
import com.chinamobile.httpserver.response.JsonRespFactory;
import com.chinamobile.httpserver.response.UploadRespFactory;

public class RequestFilter {
	static public HttpResponseMessage doFilter(String context,
			IoSession session, Object message) {

		//System.out.println("context-----------"+context);
		
		if (context.toLowerCase().endsWith(".cas")) {
			try {
				return CassandraRespFactory.getResponse(context).execute(session,
						message);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}else if (context.toLowerCase().startsWith("ctp/reportinfo/v3/transaction/")) {
			try {
				return FileRespFactory.getResponse("transaction_d").execute(
						message,
						context.substring(context.lastIndexOf("/") + 1));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}else if (context.toLowerCase().endsWith("png")
				|| context.toLowerCase().endsWith("png/")) {
			try {
				return FileRespFactory.getResponse("requestpic").execute(
						message,
						context.substring(context.lastIndexOf("/") + 1));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else if (context.toLowerCase().endsWith("upload")
				|| context.toLowerCase().endsWith("upload/")) {
			try {
				return UploadRespFactory.getResponse(context).execute(message);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else if (context.toLowerCase().endsWith("json")
				|| context.toLowerCase().endsWith("json/")) {
			try {
				return JsonRespFactory.getResponse(context).execute(session,
						message);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else if (context.toLowerCase().endsWith("action")
				|| context.toLowerCase().endsWith("action/")) {
			try {
				return ActionRespFactory.getResponse(context).execute(session,
						message);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else if (context.toLowerCase().endsWith(".js")
				|| context.toLowerCase().endsWith(".js/")) {
			try {
				return JsRespFactory.getResponse("requestjs").execute(session,
						context.substring(context.lastIndexOf("/") + 1));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else if (context.toLowerCase().endsWith(".css")
				|| context.toLowerCase().endsWith(".css/")) {
			try {
				return CssRespFactory.getResponse("requestcss").execute(session,
						context.substring(context.lastIndexOf("/") + 1));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else if (context.toLowerCase().endsWith(".html")
				|| context.toLowerCase().endsWith(".html/")) {
			try {
				return HtmlRespFactory.getResponse("requesthtml").execute(session,
						context.substring(context.lastIndexOf("/") + 1));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			try {
				return HttpRespFactory.getResponse(context).execute(message);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		return null;
	}
}
