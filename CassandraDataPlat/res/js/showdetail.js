var jsondata;
var pointall;
var dataCatch=[];
//for nodes controlling
var nodeArray = new Array();
//for UEs controlling
var uesArray = new Array();

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
        },200);
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
		timeout : 2000,
		async : true,
		error : function(XMLHttpRequest, textStatus, errorThrown) {
//			alert("错误");
		},
		success : function(data) {
			if(data != null){
					jsondata = data;
					gpsxy(data.gps.E,data.gps.N);
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
		timeout : 2000,
		async : true,
		error : function(XMLHttpRequest, textStatus, errorThrown) {
//			alert("错误");
		},
		success : function(data) {
			if(data != null){
					var uesinstance = getUes(data.car,data.name,data.count);
					var jsonarray = data.ues;
					var ueindex;
					for(ueindex=0; ueindex<jsonarray.length; ueindex++){
							var ueinstance = new ue(jsonarray[ueindex].id,jsonarray[ueindex].type,jsonarray[ueindex].name,jsonarray[ueindex].status.capability,jsonarray[ueindex].time,jsonarray[ueindex].status.uri,jsonarray[ueindex].status.type,jsonarray[ueindex].status.upload,jsonarray[ueindex].status.download);

							uesinstance.setue(ueinstance);
					}
					uesinstance.showinfo();
			}
		}
	});
}

/**
*Define the construction of UEs(as a ue in the car).
*@author wjwd
*/
function ue(id,type,name,capability,time,status_uri,status_type,status_upload,status_download){
		this.id = id;
		this.type = type;
		this.uename = name;
		this.capability = capability;
		this.time = time;
		this.status_uri = status_uri;
		this.status_type = status_type;
		this.status_upload = status_upload;
		this.status_download = status_download;
}

function ues(){
		this.uid;
		this.name;
		this.count;
		this.ueArray = new Array();
}

ues.prototype.setue=function(ue){
		if(this.ueArray.length==0){
				this.ueArray[0] = ue;
		} else {
				var x;
				for(x in this.ueArray){
						if(this.ueArray[x].id == ue.id){
								this.ueArray[x] = ue;
								break;
						}
				}
				if(this.ueArray[x].id != ue.id){
					  var a = parseInt(x) + 1;
						this.ueArray[a] = ue;
				}	
		}
}

ues.prototype.showinfo=function(){
		var nodeinstance = getNode(this.uid,this.name,this.count);
		nodeinstance.showinfo();
}

/**
*Define the construction of Node(as a car on the map).
*@author wjwd
*/
function node(){
		this.uid;
		this.name;
		this.count;
		this.lng;
		this.lat;
		this.height;
		this.speed;
		this.index;
		this.time;
}

node.prototype.excu=function(name,lng,lat,height,speed,count,time){
		this.name = name;
		this.lng = lng;
		this.lat = lat;
		this.height = height;
		this.speed = speed;
		this.count = count;
		this.time = time; 
}

node.prototype.showinfo=function(){
		var a = parseInt(this.index)+1;
		$("#m"+a).show();
		$("#n"+a).empty().html(this.name);
		getCarHtml(this);
}

/**
*Get the node related to the node_id(uid).
*@author wjwd
*/
function getNode(node_id,name,count) {
		if(nodeArray.length == 0){
				nodeArray[0] = new node();
				nodeArray[0].uid = node_id;
				nodeArray[0].name = name;
				nodeArray[0].count = count;
				nodeArray[0].index = 0;
				return nodeArray[0];
		}
		var x;
		for(x in nodeArray){
				if(nodeArray[x].uid == node_id){
						break;
				}
		}
		if(nodeArray[x].uid == node_id){
				nodeArray[x].index = x;
				nodeArray[x].name = name;
				nodeArray[x].count = count;
				return nodeArray[x];
		} else {
			  var a = parseInt(x) + 1;
				nodeArray[a] = new node();
				nodeArray[a].uid = node_id;
				nodeArray[a].index = a;
				nodeArray[a].name = name;
				nodeArray[a].count = count;
				return nodeArray[a];
		}
}

/**
*Get the UEs related to the node_id(uid) of testing car.
*@author wjwd
*/
function getUes(node_id,name,count) {
		if(uesArray.length == 0){
				uesArray[0] = new ues();
				uesArray[0].uid = node_id;
				uesArray[0].name = name;
				uesArray[0].count = count;
				return uesArray[0];
		}
		var x;
		for(x in uesArray){
				if(uesArray[x].uid == node_id){
						break;
				}
		}
		if(uesArray[x].uid == node_id){
				return uesArray[x];
		} else {
				var b = parseInt(x) + 1;
				uesArray[b] = new ues();
				uesArray[b].uid = node_id;
				uesArray[b].name = name;
				uesArray[b].count = count;
				return uesArray[b];
		}
}

/**
*Translate the GPS position info into Baidu Map positions info.
*@author wjwd
*/
gpsxy = function (point_x,point_y){
    var xx = point_x;
    var yy = point_y;
    var gpsPoint = new BMap.Point(xx,yy);
//    BMap.Convertor.translate(gpsPoint,0,translateCallback);

		if(gpsPoint.lng!=0 && gpsPoint.lat !=0){
				var nodeinstance = getNode(jsondata.car,jsondata.name,jsondata.count);
				nodeinstance.excu(jsondata.name,gpsPoint.lng,gpsPoint.lat,jsondata.gps.H,jsondata.gps.S,jsondata.count,jsondata.time);
				
				nodeinstance.showinfo();
		}
}

function getrid(myarray,dx){
    if(isNaN(dx)||dx>myarray.length){return false;}
    for(var i=0,n=0;i<myarray.length;i++)
    {
        if(myarray[i]!=myarray[dx])
        {
            myarray[n++]=myarray[i]
        }
    }
    myarray.length-=1
}

function getCarHtml(node){
	
		var dn=node.count;
		var html='<div class="lte_show_list2_name">\r\n';
		html+='<div class="lte_show_list2_name_frame1">测试车:\r\n';
		html+='</div>\n';
		html+='<div class="lte_show_list2_name_frame_text1">';
		html+=node.name;
		html+='</div>\n';
		html+='<div class="lte_show_list2_name_frame2">GPS:\r\n';
		html+='</div>\n';
		html+='<div class="lte_show_list2_name_frame_text2">经度-E ';
		html+=parseFloat(node.lng).toFixed(6);
		html+='°   纬度-N ';
		html+=parseFloat(node.lat).toFixed(6);
		html+='°    高度-';
		html+=parseFloat(node.height).toFixed(2);
		html+='m   速率-';
		html+=parseFloat(node.speed).toFixed(2);
		html+='km/h \r\n';
		html+='</div>\r\n';
		html+='<div class="lte_show_list2_name_frame3">终端数量:\r\n';
		html+='</div>\r\n';
		html+=' <div class="lte_show_list2_name_frame_text3">';
		html+=dn;
		html+='</div>\r\n';
		html+='<div class="lte_show_list2_name_frame4">时间戳:\r\n';
		html+='</div>\r\n';
		html+='<div class="lte_show_list2_name_frame_text4">\r\n';
		var timestamp = new Date(node.time);
		html+=timestamp.format("yyyy/MM/dd hh:mm:ss");
		html+='</div>\r\n';
		html+='</div>\r\n';
		html+='<div class="lte_show_list2_frame">\r\n';
		var uesInstance = getUes(node.uid,node.name,node.count);
		if(uesInstance.ueArray.length != 0){
				for(var i=0;i<dn;i++){
						html+='<div class="lte_show_list2_text">\r\n';
						html+='<ul class="lte_show_list2_pro">\r\n';
						html+=' <li class="lte_show_list2_pro_p">';
						html+=uesInstance.ueArray[i].id;
						
						html+='</li>\r\n'; 
						html+=' <li class="lte_show_list2_pro_p">';
						html+=uesInstance.ueArray[i].type;
						html+='</li>\r\n'; 
						html+=' <li class="lte_show_list2_pro_p">';
						html+=uesInstance.ueArray[i].capability;
						html+='</li>\r\n'; 
						html+=' <li class="lte_show_list2_pro_p">';
						if(uesInstance.ueArray[i].capability.indexOf("layer_3_ICMP") != -1){
								html+='-----</li>\r\n'; 
								html+=' <li class="lte_show_list2_pro_p">';
								html+=parseFloat(uesInstance.ueArray[i].status_download).toFixed(4);
								html+='  ms</li>\r\n'; 
						} else {
								html+=parseFloat(uesInstance.ueArray[i].status_upload*8/1024).toFixed(4);
								html+='  Kbps</li>\r\n'; 
								html+=' <li class="lte_show_list2_pro_p">';
								html+=parseFloat(uesInstance.ueArray[i].status_download*8/1024).toFixed(4);
								html+='  Kbps</li>\r\n'; 	
						}
						html+=' <li class="lte_show_list2_pro_p">';
						var timestamp1 = new Date(uesInstance.ueArray[i].time);
						html+=timestamp1.format("yyyy/MM/dd hh:mm:ss");
						html+='</li>\r\n'; 
						html+='</ul>\r\n';
						html+='</div>\r\n';
				}
				
				for(var m=dn;m<5;m++){
						html+='<div class="lte_show_list2_text2">\r\n';
						html+='</div>\r\n';
				}
		}else{
				for(var m=0;m<5;m++){
						html+='<div class="lte_show_list2_text2">\r\n';
						html+='</div>\r\n';
				}
		}
		
		html+=' </div>\r\n';
		html+=' </div>\r\n';
		var index = parseInt(node.index)+1;
		$("#d"+index).empty().html(html);
}
 
function change(index){
 	if(index==0){
 		var isCheckAll=false;
 		
 		for(var j=0;j<dataCatch.length;j++){
 			if(dataCatch[j]==0){
 				isCheckAll=true;
 				break;
 			}
 		}
 		
 		if(!isCheckAll){
		 		for(var i=0;i<=nodeArray.length;i++){
			 			document.getElementById('c'+i).style.backgroundImage="url(images/lte_show_list_frame_icon2.png)";
			 			dataCatch.push(i);
			 			if(i!=0)
			 				$("#d"+i).show();
		 		}
 		
 		}else{
	 			dataCatch=[];
	 			for(var i=0;i<=nodeArray.length;i++){
			 			document.getElementById('c'+i).style.backgroundImage="url(images/lte_show_list_frame_icon1.png)";
			 			if(i!=0)
			 				$("#d"+i).hide();
	 			}
 		}
 	}else{
 		var isExe=false;
 		for(var i=0;i<dataCatch.length;i++){
 			if(dataCatch[i]==index){
 				isExe=true;
 				getrid(dataCatch,i);
 				break;
 			}
 		}
 		if(isExe){
 			document.getElementById('c'+index).style.backgroundImage="url(images/lte_show_list_frame_icon1.png)";
 			$("#d"+index).hide();
 		}else{
 			dataCatch.push(index);
 			document.getElementById('c'+index).style.backgroundImage="url(images/lte_show_list_frame_icon2.png)";
 			$("#d"+index).show();
 		}
 	}
 }	

Date.prototype.format = function(format){
		var o =	{
				"M+" : this.getMonth()+1, //month
				"d+" : this.getDate(), //day
				"h+" : this.getHours(), //hour
				"m+" : this.getMinutes(), //minute
				"s+" : this.getSeconds(), //second
				"q+" : Math.floor((this.getMonth()+3)/3), //quarter
				"S" : this.getMilliseconds() //millisecond
		}
		
		if(/(y+)/.test(format)){
				format = format.replace(RegExp.$1, (this.getFullYear()+"").substr(4 - RegExp.$1.length));
		}
		
		for(var k in o)	{
				if(new RegExp("("+ k +")").test(format)) {
						format = format.replace(RegExp.$1, RegExp.$1.length==1 ? o[k] : ("00"+ o[k]).substr((""+ o[k]).length));
				}
		}
		return format;
}

run();
