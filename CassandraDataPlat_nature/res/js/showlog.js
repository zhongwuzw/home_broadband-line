/**
*Start the main process.
*@author wjwd
*/
window.run = function (){
    function resetCarDisp(){
    		getCarMsg();
    }
    function resetUeDisp(){
    		getUeMsg();
    }
    setInterval(function(){
					resetCarDisp();
        },1000);
    setInterval(function(){
					resetUeDisp();
        },500);
}

/**
*Get the real-time Testing Cars' positions from the Server(MINA HTTP Server).
*@author wjwd
*/
function getCarMsg() {
	$.ajax({
		type : "POST",
		url : "../carmsgdetail.json",
		dataType : "json",
		timeout : 3000,
		async : true,
		error : function(XMLHttpRequest, textStatus, errorThrown) {
//			alert("错误");
		},
		success : function(data) {
			if(data != null){
				var str = JSON.stringify(data);
				var dc="<div class=\"lte_show_list2_name_frame_text3\">"+str+"<br></div>\r\n";
			//	console.log("dc==>"+dc);
					$("#content").append(dc);
			}
		}
	});
}

/**
*Get the real-time Testing UEs' meassges from the Server(MINA HTTP Server).
*@author wjwd
*/
function getUeMsg() {
	$.ajax({
		type : "POST",
		url : "../uemsgdetail.json",
		dataType : "json",
		timeout : 3000,
		async : true,
		error : function(XMLHttpRequest, textStatus, errorThrown) {
//			alert("错误");
		},
		success : function(data) {
			if(data != null){
					var str = JSON.stringify(data);
					var dc="<div class=\"lte_show_list2_name_frame_text3\">"+str+"<br></div>\r\n";
			//	console.log("dc==>"+dc);
					$("#content").append(dc);
			}
		}
	});
}

run();