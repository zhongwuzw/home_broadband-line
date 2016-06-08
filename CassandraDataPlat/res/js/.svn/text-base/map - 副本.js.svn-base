var jsondata;
var map = new BMap.Map("container");
map.centerAndZoom(new BMap.Point(116.404, 39.915), 15);
map.addControl(new BMap.NavigationControl());  

//for infoWindows controlling
var opts = {
  width : 290,     // 信息窗口宽度
  height: 130,     // 信息窗口高度
  title : ""  // 信息窗口标题
};

//for nodes controlling
var node_focus = "default";
var nodeArray = new Array();
var colorArray = new Array();
var infoWndStatus = new Array();
//["blue","red","black","yellow","grey"];
colorArray[0] = "blue";
colorArray[1] = "red";
colorArray[2] = "black";
colorArray[3] = "yellow";
colorArray[4] = "grey";
//for UEs controlling
var uesArray = new Array();

/**
*Start the main process.
*@author wjwd
*/
window.run = function (){

/**
*		function resetCarDisp(){
    		getCarMsg();
		}
    
    function resetUeDisp(){
    		getUeMsg();
    }
*/

/**    setInterval(function(){
*					resetCarDisp();
*        },1000);
*/
    setInterval(function(){
					resetUeDisp();
        },500);
        
    resetCarDisp();
}

function resetCarDisp(){
    		getCarMsg();
}

function resetUeDisp(){
    		getUeMsg();
}
/**
*Get the real-time Testing Cars' positions from the Server(MINA HTTP Server).
*@author wjwd
*/
function getCarMsg() {
	$.ajax({
		url : "../carmsgshow.json",
		dataType : "json",
		timeout : 3000,
		async : true,
		error : function(XMLHttpRequest, textStatus, errorThrown) {
//			alert("错误");
				resetCarDisp();
		},
		success : function(data) {
			if(data != null){
					jsondata = data;
					gpsxy(data.gps.E,data.gps.N);
			} else {
					gpsxy(0.0000,0.0000);
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
		url : "../uemsgshow.json",
		dataType : "json",
		timeout : 3000,
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
//					alert("node_focus == data.car  "+node_focus+"=="+data.car);
					if(node_focus == data.car){
							uesinstance.showinfo();;
					}
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
		}
		var x;
		for(x in this.ueArray){
				if(this.ueArray[x].id == ue.id){
						this.ueArray[x] = ue;
						break;
				}
		}
		if(this.ueArray[x].id == ue.id){
		} else {
			  var a = parseInt(x) + 1;
				this.ueArray[a] = ue;
		}
}
ues.prototype.dispinfo=function(){
		var y;
		for(y in this.ueArray){
				$("#ueinfo"+y).show();
				$("#uenoinfo"+y).hide();
		}
		var b = parseInt(y) + 1;
		while(b<3){
				$("#ueinfo"+this.index).hide();
				$("#uenoinfo"+this.index).show();
				b++;
		}
}

ues.prototype.showinfo=function(){
		var y;
		for(y in this.ueArray){
				$("#uename"+y).empty().html(this.ueArray[y].uename);
				$("#ueid"+y).empty().html(this.ueArray[y].id);
				$("#uetype"+y).empty().html(this.ueArray[y].type);
				$("#uecapability"+y).empty().html(this.ueArray[y].capability);
				if(this.ueArray[y].capability.indexOf("ICMP") != -1){
					  $("#uploaddesc"+y).empty().html("");
						$("#ueupload"+y).empty().html("");
						$("#downloaddesc"+y).empty().html("平均时延：");
						$("#uedownload"+y).empty().html(parseFloat(this.ueArray[y].status_download).toFixed(4)+"  ms");
				} else {
						$("#ueupload"+y).empty().html(parseFloat(this.ueArray[y].status_upload*8/1024).toFixed(4)+"  Kbps");
						$("#uedownload"+y).empty().html(parseFloat(this.ueArray[y].status_download*8/1024).toFixed(4)+"  Kbps");
				}
				var timestamp = new Date(this.ueArray[y].time);
				$("#uetime"+y).empty().html(timestamp.format("yyyy/MM/dd hh:mm:ss"));
		}
		var b = parseInt(y) + 1;
		while(b<3){
				$("#ueinfo"+this.index).hide();
				$("#uenoinfo"+this.index).show();
				b++;
		}
}
/**
*Define the construction of Node(as a car on the map).
*@author wjwd
*/
function node(){
		this.uid;
		this.name;
		this.count;
		this.content;
		this.icon;
		this.carMarker;
		this.infoWindow;
		this.polyLine;
		this.color;
		this.startPoint;
		this.endPoint;
		this.index;
}

node.prototype.init=function(index,lng,lat,count){
		this.startPoint = new BMap.Point(lng,lat);
		this.endPoint = this.startPoint;
		this.color = colorArray[index];
		this.count = count;
		//init the icon of carMarkers.
		this.icon =  new BMap.Icon("lte_car"+index+".png", new BMap.Size(70, 70), {    //小车图片
				imageOffset: new BMap.Size(0, 0)    //图片的偏移量。为了是图片底部中心对准坐标点。
		});
		
		//init the carMarkers.
		this.carMarker = new BMap.Marker(this.startPoint,{icon:this.icon});
    map.addOverlay(this.carMarker);
    this.carMarker.setPosition(this.startPoint);
		
		//init the infoWindow of carMarkers.
		this.infoWindow = new BMap.InfoWindow("测试信息", opts);  // 创建信息窗口对象
		infoWndStatus[index] = false;
		this.infoWindow.addEventListener("clickclose", function(){          
		   infoWndStatus[index] = false;  
		});
		
		this.carMarker.addEventListener("mouseover", function(){  
			 if(infoWndStatus[index] == false){
			 		nodeArray[index].carMarker.openInfoWindow(nodeArray[index].infoWindow);
			 }           
		});
		
		this.carMarker.addEventListener("mouseout", function(){      
			 if(infoWndStatus[index] == false){
			 		nodeArray[index].carMarker.closeInfoWindow();  
			 }    
		});
		
		this.carMarker.addEventListener("click", function(){  
			 infoWndStatus[index] = true;
		   nodeArray[index].carMarker.openInfoWindow(nodeArray[index].infoWindow);  
		   var uesinstance = getUes(nodeArray[index].uid,nodeArray[index].name,nodeArray[index].count);
//		   alert("uesinstance.ueArray.length "+uesinstance.ueArray.length);
			 if(nodeArray[index].uid == node_focus){
//			 			alert("nodeArray[index].uid == node_focus  "+nodeArray[index].uid+"  "+node_focus);
			 			uesinstance.dispinfo();
			 }
		});
}

node.prototype.excu=function(name,lng,lat,height,speed,count,time,gpslng,gpslat){
		this.name = name;
		this.endPoint = new BMap.Point(lng,lat);
		this.count = count; 
		var timestamp = new Date(time);
		this.content  = "测试车："+name+"<br>时间："+timestamp.format("yyyy/MM/dd hh:mm:ss")+"<br>经度："+parseFloat(gpslng).toFixed(6)+"°  纬度："+parseFloat(gpslat).toFixed(6)+"°<br>海拔："+parseFloat(height).toFixed(2)+"  m<br>速度："+parseFloat(speed).toFixed(2)+"  km/h<br>终端个数："+count;
		this.polyLine = new BMap.Polyline([
						this.startPoint,  
						this.endPoint  
				],  
				{strokeColor:this.color, strokeWeight:5, strokeOpacity:0.5}  
		);
		this.startPoint = this.endPoint;
		map.addOverlay(this.polyLine);  
	  this.infoWindow.setContent(this.content);
	  this.infoWindow.redraw();
	  this.carMarker.setPosition(this.endPoint);
}

node.prototype.showinfo=function(){
		$("#choosecar"+this.index).show();
		$("#carname"+this.index).show();
		$("#noinfo"+this.index).hide();
		$("#carname"+this.index).empty().html(this.name);
}

/**
*Get the node related to the node_id(uid).
*@author wjwd
*/
function getNode(node_id,lng,lat,count) {
		if(nodeArray.length == 0){
				nodeArray[0] = new node();
				nodeArray[0].uid = node_id;
				nodeArray[0].init(0,lng,lat,count);
				nodeArray[0].index = 0;
				$("#choosecar0").empty().html("<div class=\"lte_frame_list_iocn2\"></div>");
				node_focus = nodeArray[0].uid;
				map.centerAndZoom(nodeArray[0].endPoint, 16);
				return nodeArray[0];
		}
		var x;
		for(x in nodeArray){
				if(nodeArray[x].uid == node_id){
						break;
				}
		}
		if(nodeArray[x].uid == node_id){
				return nodeArray[x];
		} else {
			  var a = parseInt(x) + 1;
				nodeArray[a] = new node();
				nodeArray[a].uid = node_id;
				nodeArray[a].index = a;
				nodeArray[a].init(a,lng,lat,count);
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
				return uesArray[b];
		}
}

/**
*Choose and Control Car Marker.
*@author wjwd
*/
function chooseCar(tag,index) {
		var cancelchoose;
		if(nodeArray[index].uid != node_focus){
			if(node_focus != "default"){
				var x;
				for(x in nodeArray){
						if(nodeArray[x].uid == node_focus){
								break;
						}
				}
				cancelchoose = "#choosecar"+x;	
				$(cancelchoose).empty().html("<div class=\"lte_frame_list_iocn1\"></div>");
			} 
			if(index<nodeArray.length){
				var tagid = "#"+$(tag).attr("id");
				$(tagid).empty().html("<div class=\"lte_frame_list_iocn2\"></div>");
				node_focus = nodeArray[index].uid;
		
				var i=0;
				while(i<3){
						$("#ueinfo"+i).hide();
						$("#uenoinfo"+i).show();
						i++;
				}
				
				map.centerAndZoom(nodeArray[index].endPoint, 16);
			}
		} else {
				var tagid = "#"+$(tag).attr("id");
				$(tagid).empty().html("<div class=\"lte_frame_list_iocn2\"></div>");
				node_focus = nodeArray[index].uid;
				map.centerAndZoom(nodeArray[index].endPoint, 16);
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
    if(gpsPoint.lng!=0 && gpsPoint.lat!=0){
	    	var callbackName = 'cbk_' + Math.round(Math.random() * 10000);    //随机函数名
		    var urlstr = "http://api.map.baidu.com/ag/coord/convert?from=0&to=4&x=" + gpsPoint.lng + "&y=" + gpsPoint.lat + "&callback=BMap.Convertor." + callbackName;
				$.ajax({
					url : urlstr,
					dataType : "jsonp",
					timeout : 1000,
					async : true,
					error : function(XMLHttpRequest, textStatus, errorThrown) {
				//		alert("ZHUANHUAN错误");
						resetCarDisp();
					},
					success : function(data) {
						//alert(data.x+"   "+data.y);
						var point = new BMap.Point(data.x, data.y);
						var x = point.lng;
						point.lng = x.toFixed(6);
						var y = point.lat;
						point.lat = y.toFixed(6);
				    if(point.lng!=0 && point.lat !=0){
								var nodeinstance = getNode(jsondata.car,point.lng,point.lat,jsondata.count);
								nodeinstance.excu(jsondata.name,point.lng,point.lat,jsondata.gps.H,jsondata.gps.S,jsondata.count,jsondata.time,jsondata.gps.E,jsondata.gps.N);
								nodeinstance.showinfo();
						}
						resetCarDisp();
					}
				});
			} else {
					resetCarDisp();
			}
}

function gotocar2(){
 	window.location.href='../uploaddetail';
}

function gotolog(){
 	window.location.href='../uploadlog';
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
