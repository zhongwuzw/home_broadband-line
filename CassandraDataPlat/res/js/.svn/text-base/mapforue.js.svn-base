var isEng = true;
var map;  

$(document).ready(function() {
	map = new BMap.Map("container1");
	map.centerAndZoom(new BMap.Point(116.404, 39.915), 15);
	map.setZoom(5);
	map.addControl(new BMap.NavigationControl());  
	map.enableScrollWheelZoom(true);
	//run();
});

//for infoWindows controlling
var opts = {
  width : 290,     // 信息窗口宽度
  height: 130,     // 信息窗口高度
  title : ""  // 信息窗口标题
}

//for nodes controlling
var nodeArray = new Array();
var infoWndStatus = new Array();

function msgshow(str){
    Ext.Msg.show({
        title:'Failure',
        modal:false,
        msg:str,
        buttons:Ext.Msg.OK,
        width:180
    });
}

function myrefresh(){
   window.location.reload();
}

Ext.onReady(function(){
	
});

/**
*Define the construction of Node(as a car on the map).
*@author wjwd
*/
function node(){
		this.uid;
		this.content;
		this.icon;
		this.imgstat;
		this.carMarker;
		this.infoWindow;
		this.startPoint;
}

node.prototype.init=function(index,lng,lat,device_state){
		var imgstat = 0;
		if(device_state == "idle"){
			imgstat = 0;
		}else if(device_state == "run"){
			imgstat = 1;
		}if(device_state == "stop"){
			imgstat = 2;
		}
		this.startPoint = new BMap.Point(lng,lat);
		//init the icon of carMarkers.
		this.icon =  new BMap.Icon("node_"+imgstat+".png", new BMap.Size(70, 70), {    //小车图片
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
		});
}

node.prototype.excu=function(content){
		if(!isEng){
			this.content  = content;
		}else{
			this.content  = content;
		}
	  this.infoWindow.setContent(this.content);
	  this.infoWindow.redraw();
	  this.carMarker.setPosition(this.startPoint);
}

/**
*Get the node related to the node_id(uid).
*@author wjwd
*/
function getNode(node_id,lng,lat,imgstat) {
		if(nodeArray.length == 0){
				nodeArray[0] = new node();
				nodeArray[0].uid = node_id;
				nodeArray[0].init(0,lng,lat,imgstat);
				nodeArray[0].index = 0;
				nodeArray[0].imgstat = imgstat;
				return nodeArray[0];
		}
		var x;
		for(x in nodeArray){
				if(nodeArray[x].uid == node_id){
						break;
				}
		}
		if(nodeArray[x].uid == node_id){
				nodeArray[x] = new node();
				nodeArray[x].uid = node_id;
				nodeArray[x].init(x,lng,lat,imgstat);
				nodeArray[x].index = x;
				nodeArray[x].imgstat = imgstat;
				return nodeArray[x];
		} else {
			  var a = parseInt(x) + 1;
				nodeArray[a] = new node();
				nodeArray[a].uid = node_id;
				nodeArray[a].index = a;
				nodeArray[a].imgstat = imgstat;
				nodeArray[a].init(a,lng,lat,imgstat);
				return nodeArray[a];
		}
}

/**
*Translate the GPS position info into Baidu Map positions info.
*@author wjwd
*/
gpsxy = function (node_id,point_x,point_y,index,content){
    var xx = point_x;
    var yy = point_y;
    var gpsPoint = new BMap.Point(xx,yy);
    if(gpsPoint.lng!=0 && gpsPoint.lat!=0){
	    	var callbackName = 'cbk_' + Math.round(Math.random() * 10000);    //随机函数名
		    var urlstr = "http://api.map.baidu.com/ag/coord/convert?from=0&to=4&x=" + gpsPoint.lng + "&y=" + gpsPoint.lat + "&callback=BMap.Convertor." + callbackName;
				$.ajax({
					url : urlstr,
					dataType : "jsonp",
					timeout : 700,
					async : true,
					error : function(XMLHttpRequest, textStatus, errorThrown) {
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
								var nodeinstance = getNode(node_id,point.lng,point.lat,index);
								nodeinstance.excu(content);
						}
					}
				});
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