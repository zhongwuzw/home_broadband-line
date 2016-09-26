
//增加正则表达式
  String.prototype.getQueryString = function(name){
     var reg = new RegExp("(^|&|\\?)"+ name +"=([^&]*)(&|$)"), r;
     if (r=this.match(reg)) return unescape(r[2]); 
     return null;
  };
  _authkey = location.search.getQueryString("key");
  apiKey = location.search.getQueryString("apiKey");
  //personName;
  //判断用户是否登陆 
  if(_authkey==null || _authkey=="null" || _authkey=="" || _authkey=="undefined" || _authkey==undefined){
	  location.href="loginPage_sso.html";
  }
  $.ajax({
	  type : "post",  
      async : false, //同步执行  
      url:'sysUser/validLogin',
      data : {key:_authkey,apiKey:apiKey},  
      dataType : "json", //返回数据形式为json  
      success : function(res) { 
    	  if (res && res.status != 2) {
    		  //alert(res.username);
				//alert("success");
    		  personName=res.personName;
    		  userName = res.username;
		  }else {
				location.href="loginPage_sso.html";
				//alert("user为null");
		  }
      },  
      error : function(errorMsg) {  
    	  location.href="loginPage_sso.html";
    	  //alert("返回json格式出错");
      }  
	});
  
  $("span[class='hidden-xs']").html(personName);
//获取当前年月
  var myDate = new Date();
  var year = myDate.getFullYear().toString();
  var monthh = myDate.getMonth()+1;
  var dayy = myDate.getDate();
  if (monthh < 10 && monthh != 0) {
  	monthh = '0' + monthh;
  }
  if (dayy < 10 && dayy != 0) {
  	dayy = '0' + dayy;
  }
  var thisDate = year+"/"+monthh+"/"+dayy;//Date:当前年月 (报告中的发布日期)
  var yearMonth = year+monthh.toString();
  
  var month = monthh.toString(); //获取当前日期的月份
  var year2 = year; //获取当前日期的年份
  var month2 = parseInt(month) - 1;
  if (month2 == 0) {
      year2 = parseInt(year2) - 1;
      month2 = 12;
  }
  if (month2 < 10 && month2 != 0) {
      month2 = '0' + month2;
  }
  var preYearMonth = year2.toString()+month2.toString(); //当前月份的上月
  
  
  
  var oriXOpen = XMLHttpRequest.prototype.open;
  XMLHttpRequest.prototype.open = function(method, url, asncFlag, user, password) {
  	// code to trace or intercept
  	if(_authkey){
  		if(url.indexOf('?') < 0){
  			url += "?key=" + _authkey;
  		}else{
  			url += '&key=' + _authkey;
  		}
  	}
  	oriXOpen.call(this, method, url, asncFlag, user, password);
  };

  $.ajax({
	  type : "post",  
      async : false, //同步执行  
      url:'provincequalitymap/getProvinceName',
      data : {key:_authkey,apiKey:apiKey},  
      dataType : "json", //返回数据形式为json  
      success : function(res) { 
    	  nationwideOrProvince = res.groupname;
      },  
      error : function(errorMsg) {  
    	  alert("全国/省份--返回数据出错！");
      }  
});
  
//饼图颜色集合定义
var pie_color = [
        {color: "#f56954",highlight: "#f56954"}, //red
        {color: "#00a65a",highlight: "#00a65a"}, //green
        {color: "#f39c12",highlight: "#f39c12"}, //yellow
        {color: "#00c0ef",highlight: "#00c0ef"}, //blue
        {color: "#E2BAFF",highlight: "#E2BAFF"}, //浅紫粉
        {color: "#951AFF",highlight: "#951AFF"}, //浅紫色
        {color: "#222D32",highlight: "#222D32"}, //黑色
        {color: "#8F0990",highlight: "#8F0990"}, //紫色
        {color: "#D2D6DE",highlight: "#D2D6DE"}, //灰色
        {color: "#3c8dbc",highlight: "#3c8dbc"}, 
        {color: "#FDA7FE",highlight: "#FDA7FE"},
        {color: "#FF570F",highlight: "#FF570F"},
        {color: "#A7A600",highlight: "#A7A600"}, 
        {color: "#99FFCC",highlight: "#99FFCC"},
        
        {color: "#669966",highlight: "#669966"},
        {color: "#663300",highlight: "#663300"},
        {color: "#003300",highlight: "#003300"},
        {color: "#FFFF99",highlight: "#FFFF99"},
        {color: "#7FFFD4",highlight: "#7FFFD4"} //Aquamarine
    ];

//barClass集合定义
var barClass_color = [
        {barclass:"progress-bar progress-bar-aqua"}, //blue
        {barclass:"progress-bar progress-bar-red"}, //red
        {barclass:"progress-bar progress-bar-green"}, //green
        {barclass:"progress-bar progress-bar-yellow"}, //yellow
        {barclass:"progress-bar progress-bar-qianzifen"} //浅紫粉
    ];

//导航栏页面跳转url定位，并附加key和apikey
function targetFunc(url,id){
    url = (url + "?key=" + _authkey + "&apiKey=" + apiKey);
	$("#"+id).attr('href', url);
	$("#"+id).click();
}
//导航栏页面跳转url定位，并附加key和apikey和probetype探针类型
function targetProbeFunc(url,id,probetype){
	probetype = encodeURI(probetype);
	probetype = encodeURI(probetype);  
    url = (url + "?key=" + _authkey + "&apiKey=" + apiKey + "&probetype=" + probetype);
	$("#"+id).attr('href', url);
	$("#"+id).click();
}
//测试报告跳转到报告页面
function targetFunc_report(url,id,yearmonth,province,broadtype,groupid){
	province = encodeURI(province);
	province = encodeURI(province);  
	
	broadtype = encodeURI(broadtype);
	broadtype = encodeURI(broadtype);
	url = (url + "?key=" + _authkey + "&apiKey=" + apiKey+ "&reportDate=" + yearmonth+ "&reportProvinceName=" + province+ "&reportBroadType=" + broadtype+ "&groupid=" + groupid);
	$("#"+id).attr('href', url);
	$("#"+id).click();
}


/**
 * 点击用户名时退出
 */
function loginout(){
	if(confirm("确定要退出系统吗？")){
		$.ajax({
			  type : "post",  
		      async : false, //同步执行  
		      url:'sysUser/logout',
		      data : {key:_authkey,apiKey:apiKey},  
		      dataType : "json", //返回数据形式为json  
		      success : function(res) { 
		    	  setTimeout(function() {
						location.href="loginPage_sso.html";
				  }, 1000);
		      },  
		      error : function(errorMsg) {  
		    	  location.href="loginPage_sso.html";
		    	  //alert("返回json格式出错");
		      }  
		});
    }else{
    	
    }
}



