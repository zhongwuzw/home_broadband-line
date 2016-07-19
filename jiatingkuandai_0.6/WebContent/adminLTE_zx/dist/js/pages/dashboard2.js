
$(function () {
  'use strict';

  //-------------
  //- 用户数趋势 CHART -
  //-------------
  var userTendencyChartCanvas = $("#userTendencyChart").get(0).getContext("2d");
  var userTendencyChart = new Chart(userTendencyChartCanvas);
  var userTendencyChartData = {
    labels: [],
    datasets: [
      {
        label: "Electronics",
        fillColor: "rgb(210, 214, 222)",
        strokeColor: "rgb(210, 214, 222)",
        pointColor: "rgb(210, 214, 222)",
        pointStrokeColor: "#c1c7d1",
        pointHighlightFill: "#fff",
        pointHighlightStroke: "rgb(220,220,220)",
        data: []
      },
      {
        label: "Digital Goods",
        fillColor: "rgba(60,141,188,0.9)",
        strokeColor: "rgba(60,141,188,0.8)",
        pointColor: "#3b8bba",
        pointStrokeColor: "rgba(60,141,188,1)",
        pointHighlightFill: "#fff",
        pointHighlightStroke: "rgba(60,141,188,1)",
        data: []
      }
    ]
  };

  var userTendencyChartOptions = {
    //Boolean - If we should show the scale at all
    showScale: true,
    //Boolean - Whether grid lines are shown across the chart
    scaleShowGridLines: false,
    //String - Colour of the grid lines
    scaleGridLineColor: "rgba(0,0,0,.05)",
    //Number - Width of the grid lines
    scaleGridLineWidth: 1,
    //Boolean - Whether to show horizontal lines (except X axis)
    scaleShowHorizontalLines: true,
    //Boolean - Whether to show vertical lines (except Y axis)
    scaleShowVerticalLines: true,
    //Boolean - Whether the line is curved between points
    bezierCurve: true,
    //Number - Tension of the bezier curve between points
    bezierCurveTension: 0.3,
    //Boolean - Whether to show a dot for each point
    pointDot: false,
    //Number - Radius of each point dot in pixels
    pointDotRadius: 4,
    //Number - Pixel width of point dot stroke
    pointDotStrokeWidth: 1,
    //Number - amount extra to add to the radius to cater for hit detection outside the drawn point
    pointHitDetectionRadius: 20,
    //Boolean - Whether to show a stroke for datasets
    datasetStroke: true,
    //Number - Pixel width of dataset stroke
    datasetStrokeWidth: 2,
    //Boolean - Whether to fill the dataset with a color
    datasetFill: true,
    //String - A legend template
    legendTemplate: "<ul class=\"<%=name.toLowerCase()%>-legend\"><% for (var i=0; i<datasets.length; i++){%><li><span style=\"background-color:<%=datasets[i].lineColor%>\"></span><%=datasets[i].label%></li><%}%></ul>",
    //Boolean - whether to maintain the starting aspect ratio or not when responsive, if set to false, will take up entire container
    maintainAspectRatio: true,
    //Boolean - whether to make the chart responsive to window resizing
    responsive: true
  };

  userTendencyChart.Line(userTendencyChartData, userTendencyChartOptions);


  //-------------
  //- 本月平台测试次数占比 CHART -
  //-------------
  var pieChartCanvas = $("#pieChart").get(0).getContext("2d");
  var pieChart = new Chart(pieChartCanvas);
  var PieData = [];
  //PieData[0].label="111";
  var pieOptions = {
    //Boolean - Whether we should show a stroke on each segment
    segmentShowStroke: true,
    //String - The colour of each segment stroke
    segmentStrokeColor: "#fff",
    //Number - The width of each segment stroke
    segmentStrokeWidth: 1,
    //Number - The percentage of the chart that we cut out of the middle
    percentageInnerCutout: 50, // This is 0 for Pie charts
    //Number - Amount of animation steps
    animationSteps: 100,
    //String - Animation easing effect
    animationEasing: "easeOutBounce",
    //Boolean - Whether we animate the rotation of the Doughnut
    animateRotate: true,
    //Boolean - Whether we animate scaling the Doughnut from the centre
    animateScale: false,
    //Boolean - whether to make the chart responsive to window resizing
    responsive: true,
    // Boolean - whether to maintain the starting aspect ratio or not when responsive, if set to false, will take up entire container
    maintainAspectRatio: false,
    //String - A legend template
    legendTemplate: "<ul class=\"<%=name.toLowerCase()%>-legend\"><% for (var i=0; i<segments.length; i++){%><li><span style=\"background-color:<%=segments[i].fillColor%>\"></span><%if(segments[i].label){%><%=segments[i].label%><%}%></li><%}%></ul>",
    //String - A tooltip template
    tooltipTemplate: "<%=value %> <%=label%>"
  };
  //pieChart.Doughnut(PieData, pieOptions);
  
  var dataStatus = []; 
//本月各省测试次数
  $.ajax({  
      type : "post",  
      async : false, //同步执行  
      url:'provincequalitymap/findByMonth',
      data : {month:yearMonth},  
      dataType : "json", //返回数据形式为json  
      success : function(result) {  
    	  dataStatus = result.data;
    	  if(result.data.length>0){
    		  for(var i=1;i<4;i++){
        		  $("#top"+i+" h5[class='description-header']").text(result.data[i-1].testtimes);
        		  $("#top"+i+" span[class='description-text']").text(result.data[i-1].province);
        		  
        	  }
    	  }else{
    		  //top3的显示
    	  }
    	  
      },  
      error : function(errorMsg) {  
          alert("本月各省测试次数--返回数据出错");  
      }  
  });
  
  $('#world-map-markers').vectorMap({  
	  map: 'china_zh',  
	  backgroundColor: false,  
	  color: "#BBBBBB",  
	  hoverColor: false,  
	  //显示各地区名称和活动  
	  onLabelShow: function (testtimes, label, code) {  
		  $.each(dataStatus, function (i, items) {  
			  if (code == items.province) {  
				  label.html(items.province + items.testtimes);  
			  }  
		  });  
	  },  
	  //鼠标移入省份区域，改变鼠标状态  
	  onRegionOver: function(testtimes, code){  
		  $.each(dataStatus, function (i, items) {  
			  if ((code == items.province) && (items.testtimes != '')) {  
				  $('#world-map-markers').css({cursor:'pointer'});  
			  }  
		  });  
	  },  
	  //鼠标移出省份区域，改回鼠标状态  
	  onRegionOut: function(testtimes, code){  
		  $.each(dataStatus, function (i, items) {  
			  if ((code == items.province) && (items.testtimes != '')) {  
				  $('#world-map-markers').css({cursor:'auto'});  
			  }  
		  });  
	  },  
	  //点击有活动的省份区域，打开对应活动列表页面  
	  /*onRegionClick: function(event, code){  
　　　　　　$.each(dataStatus, function (i, items) {  
　　　　　　　　if ((code == items.id) && (items.event != '')) {  
　　　　　　　　　　window.location.href = items.url;  
　　　　　　　　}  
　　　　　　});  
　　　　}　　 */ 
  });  
  //改变有活动省份区域的颜色  
  $.each(dataStatus, function (i, items) {  
	  if (items.testtimes != ''){
		  var josnStr = "{" + items.province + ":'#00FF00'}";  
		  $('#world-map-markers').vectorMap('set', 'colors', eval('(' + josnStr + ')'));  
	  }  
  });  

  //-----------------
  //- SPARKLINE BAR -
  //-----------------
  $('.sparkbar').each(function () {
    var $this = $(this);
    $this.sparkline('html', {
      type: 'bar',
      height: $this.data('height') ? $this.data('height') : '30',
      barColor: $this.data('color')
    });
  });

  //-----------------
  //- SPARKLINE PIE -
  //-----------------
  $('.sparkpie').each(function () {
    var $this = $(this);
    $this.sparkline('html', {
      type: 'pie',
      height: $this.data('height') ? $this.data('height') : '90',
      sliceColors: $this.data('color')
    });
  });

  //------------------
  //- SPARKLINE LINE -
  //------------------
  $('.sparkline').each(function () {
    var $this = $(this);
    $this.sparkline('html', {
      type: 'line',
      height: $this.data('height') ? $this.data('height') : '90',
      width: '100%',
      lineColor: $this.data('linecolor'),
      fillColor: $this.data('fillcolor'),
      spotColor: $this.data('spotcolor')
    });
  });
  
  //累计用户数
  $.ajax({  
      type : "post",  
      async : false, //同步执行  
      url:'kpiqualitymap/getAccumulativnum',
      data : {month:yearMonth},  
      dataType : "json", //返回数据形式为json  
      success : function(result) {  
    	  $("#accumulativnum").html(result.accumulativ_num);
      },  
      error : function(errorMsg) {  
          alert("累计用户数--返回数据出错");  
      }  
  });
  //本月参测省份数
  $.ajax({  
      type : "post",  
      async : false, //同步执行  
      url:'kpiqualitymap/getProvincenum',
      data : {month:yearMonth},  
      dataType : "json", //返回数据形式为json  
      success : function(result) {  
    	  $("#provincenum").html(result.test_province_num);
      },  
      error : function(errorMsg) {  
          alert("本月参测省份数--返回数据出错");  
      }  
  });
  //本月新增用户数
  $.ajax({  
      type : "post",  
      async : false, //同步执行  
      url:'kpiqualitymap/getOrgnum',
      data : {month:yearMonth},  
      dataType : "json", //返回数据形式为json  
      success : function(result) {  
    	  $("#orgnum").html(result.org_num);
      },  
      error : function(errorMsg) {  
          alert("本月新增用户数--返回数据出错");  
      }  
  });
  //渠道数
  /*$.ajax({  
      type : "post",  
      async : false, //同步执行  
      url:'kpiqualitymap/getChannelnum',
      data : {month:yearMonth},  
      dataType : "json", //返回数据形式为json  
      success : function(result) {  
    	  $("#channelnum").html(result.distribution_channel_num);
      },  
      error : function(errorMsg) {  
          alert("渠道数--返回数据出错");  
      }  
  });*/
  
  //本月测试次数
  $.ajax({  
      type : "post",  
      async : false, //同步执行  
      url:'kpiqualitymap/getTesttimes',
      data : {month:yearMonth},  
      dataType : "json", //返回数据形式为json  
      success : function(result) {  
    	  $("#thismonthTesttimes").html(result.thismonthnum);
      },  
      error : function(errorMsg) {  
          alert("本月测试次数--返回数据出错");  
      }  
  });
  
  
  
  /////////////////// /////用户数趋势start//////////////////////////////
  $.ajax({  
      type : "post",  
      async : false, //同步执行  
      url:'kpiqualitymap/getUserTendencyData',
      data : {month:yearMonth},  
      dataType : "json", //返回数据形式为json  
      success : function(result) {  
    	  userTendencyChartData.labels = result.lables;
    	  userTendencyChartData.datasets[0].data = result.accumulativ_num;
    	  userTendencyChartData.datasets[1].data = result.new_user_num ;
    	  userTendencyChart.Line(userTendencyChartData, userTendencyChartOptions);
      },  
      error : function(errorMsg) {  
          alert("用户数趋势--返回数据出错");  
      }  
  });
  
  $.ajax({  
      type : "post",  
      async : false, //同步执行  
      url:'provincequalitymap/getBroadbandTypeData', //
      data : {month:yearMonth},  
      dataType : "json", //返回数据形式为json  
      success : function(result) {  
    	  var prePc="0";
    	  for(var i=0;i<result.data.length;i++){
    		  if(i>4){break;}
    		  prePc = parseInt(result.data[i].thisMonth)/parseInt(result.data[i].accumulat)*100+"%";
    		  //屏蔽嵌入式和探针
    		  if(result.data[i].type.indexOf("软探针") >= 0 || result.data[i].type.indexOf("嵌入式") >= 0)
    			  continue;
    		  $("#broadbandType").append(
    				  "<div class='progress-group' >"+
    				  "  <span class='progress-text'>"+result.data[i].type+"</span>"+
    				  "  <span class='progress-number'>"+result.data[i].thisMonth+"/"+result.data[i].accumulat+"</span>"+
    				  "  <div class='progress sm'>"+
    				  "    <div class='"+barClass_color[i].barclass+"' style='width: "+prePc+"'></div>"+
    				  "  </div>"+
    				  "</div>"
    		  );
    		  
    	  }
      },  
      error : function(errorMsg) {  
          alert("用户数趋势right--返回数据出错");  
      }  
  });
/////////////////// /////用户数趋势 end //////////////////////////////
  //终端总数(月)
  $.ajax({  
      type : "post",  
      async : false, //同步执行  
      url:'kpiqualitymap/getTerminalNum',
      data : {month:yearMonth},  
      dataType : "json", //返回数据形式为json  
      success : function(result) {  
    	  $("#terminal_num span[class='description-percentage']").text(result.percent);
    	  $("#terminal_num h5[class='description-header']").text(result.terminal_num);
    	  if(result.percent.substring(0,result.percent.length-1)>=0){
    		  $("#terminal_num span[class='description-percentage']").css("color","green");
    	  }else{
    		  $("#terminal_num span[class='description-percentage']").css("color","red");
    	  }
      },  
      error : function(errorMsg) {  
          alert("终端总数(月)--返回数据出错");  
      }  
  });
  //累计注册用户数
  $.ajax({  
      type : "post",  
      async : false, //同步执行  
      //url:'kpiqualitymap/getRegusernameNum',
      url:'kpiapiqualitymap/getRegusernameNum',
      data : {month:yearMonth,userName:userName},  
      dataType : "json", //返回数据形式为json  
      success : function(result) {  
    	  $("#regusername_num span[class='description-percentage']").text(result.percent);
    	  $("#regusername_num h5[class='description-header']").text(result.regusername_num);
    	  if(result.percent.substring(0,result.percent.length-1)>=0){
    		  $("#regusername_num span[class='description-percentage']").css("color","green");
    	  }else{
    		  $("#regusername_num span[class='description-percentage']").css("color","red");
    	  }
      },  
      error : function(errorMsg) {  
          alert("累计注册用户数--返回数据出错");  
      }  
  });
  //累计使用用户数
  $.ajax({  
      type : "post",  
      async : false, //同步执行  
      //url:'kpiqualitymap/getCustomersNum',
      url:'kpiapiqualitymap/getCustomersNum',
      data : {month:yearMonth,userName:userName},  
      dataType : "json", //返回数据形式为json  
      success : function(result) {  
    	  $("#customers_num span[class='description-percentage']").text(result.percent);
    	  $("#customers_num h5[class='description-header']").text(result.customers_num);
    	  if(result.percent.substring(0,result.percent.length-1)>=0){
    		  $("#customers_num span[class='description-percentage']").css("color","green");
    	  }else{
    		  $("#customers_num span[class='description-percentage']").css("color","red");
    	  }
      },  
      error : function(errorMsg) {  
          alert("累计使用用户数--返回数据出错");  
      }  
  });
  //上月启动次数
  $.ajax({  
      type : "post",  
      async : false, //同步执行  
      url:'kpiqualitymap/getTesttimes',
      data : {month:preYearMonth},  
      dataType : "json", //返回数据形式为json  
      success : function(result) {  
    	  $("#newlyincrease_num span[class='description-percentage']").text(result.percent);
    	  $("#newlyincrease_num h5[class='description-header']").text(result.thismonthnum);
    	  if(result.percent.substring(0,result.percent.length-1)>=0){
    		  $("#newlyincrease_num span[class='description-percentage']").css("color","green");
    	  }else{
    		  $("#newlyincrease_num span[class='description-percentage']").css("color","red");
    	  }
      },  
      error : function(errorMsg) {  
          alert("上月启动次数--返回数据出错");  
      }  
  });
  
//////////////////////////   start///////////////////////////////
/*//拼table 表头不固定 根据数据库的数据自动变化
  function appendIndexTable(result,classname,titlename){
	  //表数据
	  var tableData = [];
	//表头
	  var jsonDataHead = {value0:"",value1:"",value2:"",value3:"",value4:""};
	  //第一行
	  var jsonDataTr1 = {value0:"",value1:"",value2:"",value3:"",value4:""};
	  //第二行
	  var jsonDataTr2 = {value0:"",value1:"",value2:"",value3:"",value4:""};
	  for(var i=0;i<result.data.length;i++){
		  jsonDataHead["value"+i]=result.data[i].probetype;
		  if(result.data[i].thisdata=="null"||result.data[i].thisdata==null){
			  jsonDataTr1["value"+i]="/";
		  }else{
			  jsonDataTr1["value"+i]=result.data[i].thisdata;
		  }
		  if(result.data[i].lastdata == "null" || result.data[i].lastdata==null){
			  jsonDataTr2["value"+i]="/";
		  }else{
			  jsonDataTr2["value"+i]=result.data[i].lastdata;
		  }
	  }
	  tableData.push(jsonDataHead);
	  tableData.push(jsonDataTr1);
	  tableData.push(jsonDataTr2);
	  
		  $("div[class='"+classname+"'] table").append(
			   "<thead>"+
	          		"<tr>"+
	          		    "<th style='width: 22%'>"+titlename+"</th>"+
	          			"<th style='width: 15%'>"+tableData[0].value0+"</th>"+
	          			"<th style='width: 20%'>"+tableData[0].value1+"</th>"+
	          			"<th style='width: 15%'>"+tableData[0].value2+"</th>"+
	          			"<th style='width: 15%'>"+tableData[0].value3+"</th>"+
	          			"<th style='width: 15%'>"+tableData[0].value4+"</th>"+
	          		"</tr>"+
	          	"</thead>"+
	          	"<tbody>"+
	          		"<tr>"+
	          			"<td>本月</td>"+
	          			"<td>"+tableData[1].value0+"</td>"+
	          			"<td>"+tableData[1].value1+"</td>"+
	          			"<td>"+tableData[1].value2+"</td>"+
	          			"<td>"+tableData[1].value3+"</td>"+
	          			"<td>"+tableData[1].value4+"</td>"+
	          		"</tr>"+
	          		"<tr>"+
	          			"<td>上月</td>"+
	          			"<td>"+tableData[2].value0+"</td>"+
	          			"<td>"+tableData[2].value1+"</td>"+
	          			"<td>"+tableData[2].value2+"</td>"+
	          			"<td>"+tableData[2].value3+"</td>"+
	          			"<td>"+tableData[2].value4+"</td>"+
	          		"</tr>"+
	          	"</tbody>"
				  
		  );
  }*/
  
//拼table 表头固定 
  function appendIndexTable(result,classname){
	  for(var i=0;i<result.data.length;i++){
		  if(result.data[i].probetype == "Android"){
			  if(result.data[i].thisdata=="null"||result.data[i].thisdata==null){
				  $("div[class='"+classname+"'] table tbody").children("tr").eq(0).children("td").eq(1).html("/");
			  }else{
				  $("div[class='"+classname+"'] table tbody").children("tr").eq(0).children("td").eq(1).html(result.data[i].thisdata);
			  }
			  if(result.data[i].lastdata == "null" || result.data[i].lastdata==null){
				  $("div[class='"+classname+"'] table tbody").children("tr").eq(1).children("td").eq(1).html("/");
			  }else{
				  $("div[class='"+classname+"'] table tbody").children("tr").eq(1).children("td").eq(1).html(result.data[i].lastdata);
			  }
		  }else if(result.data[i].probetype == "iOS"){
			  if(result.data[i].thisdata=="null"||result.data[i].thisdata==null){
				  $("div[class='"+classname+"'] table tbody").children("tr").eq(0).children("td").eq(2).html("/");
			  }else{
				  $("div[class='"+classname+"'] table tbody").children("tr").eq(0).children("td").eq(2).html(result.data[i].thisdata);
			  }
			  if(result.data[i].lastdata == "null" || result.data[i].lastdata==null){
				  $("div[class='"+classname+"'] table tbody").children("tr").eq(1).children("td").eq(2).html("/");
			  }else{
				  $("div[class='"+classname+"'] table tbody").children("tr").eq(1).children("td").eq(2).html(result.data[i].lastdata);
			  }
		  }else if(result.data[i].probetype == "PC"){
			  if(result.data[i].thisdata=="null"||result.data[i].thisdata==null){
				  $("div[class='"+classname+"'] table tbody").children("tr").eq(0).children("td").eq(3).html("/");
			  }else{
				  $("div[class='"+classname+"'] table tbody").children("tr").eq(0).children("td").eq(3).html(result.data[i].thisdata);
			  }
			  if(result.data[i].lastdata == "null" || result.data[i].lastdata==null){
				  $("div[class='"+classname+"'] table tbody").children("tr").eq(1).children("td").eq(3).html("/");
			  }else{
				  $("div[class='"+classname+"'] table tbody").children("tr").eq(1).children("td").eq(3).html(result.data[i].lastdata);
			  }
		  }else if(result.data[i].probetype == "嵌入式"){
			  if(result.data[i].thisdata=="null"||result.data[i].thisdata==null){
				  $("div[class='"+classname+"'] table tbody").children("tr").eq(0).children("td").eq(4).html("/");
			  }else{
				  $("div[class='"+classname+"'] table tbody").children("tr").eq(0).children("td").eq(4).html(result.data[i].thisdata);
			  }
			  if(result.data[i].lastdata == "null" || result.data[i].lastdata==null){
				  $("div[class='"+classname+"'] table tbody").children("tr").eq(1).children("td").eq(4).html("/");
			  }else{
				  $("div[class='"+classname+"'] table tbody").children("tr").eq(1).children("td").eq(4).html(result.data[i].lastdata);
			  }
		  }else if(result.data[i].probetype == "软探针"){
			  if(result.data[i].thisdata=="null"||result.data[i].thisdata==null){
				  $("div[class='"+classname+"'] table tbody").children("tr").eq(0).children("td").eq(5).html("/");
			  }else{
				  $("div[class='"+classname+"'] table tbody").children("tr").eq(0).children("td").eq(5).html(result.data[i].thisdata);
			  }
			  if(result.data[i].lastdata == "null" || result.data[i].lastdata==null){
				  $("div[class='"+classname+"'] table tbody").children("tr").eq(1).children("td").eq(5).html("/");
			  }else{
				  $("div[class='"+classname+"'] table tbody").children("tr").eq(1).children("td").eq(5).html(result.data[i].lastdata);
			  }
		  }else{
			  
		  }

	  }
	  
  }
  //下载速率
  $.ajax({  
      type : "post",  
      async : false, //同步执行  
      url:'groupidhttpdownloadmap/getHttpDownloadRate',
      data : {month:yearMonth},  
      dataType : "json", //返回数据形式为json  
      success : function(result) {  
    	  appendIndexTable(result,"info-box bg-yellow");
      },  
      error : function(errorMsg) {  
          alert("下载速率--返回数据出错");  
      }  
  });

  //上传速率
  $.ajax({  
      type : "post",  
      async : false, //同步执行  
      url:'groupidhttpuploadmap/getHttpUploadRate',
      data : {month:yearMonth},  
      dataType : "json", //返回数据形式为json  
      success : function(result) {  
    	  appendIndexTable(result,"info-box bg-green");
      },  
      error : function(errorMsg) {  
          alert("上传速率--返回数据出错");  
      }  
  });
  
  //页面时延
  $.ajax({  
      type : "post",  
      async : false, //同步执行  
      url:'groupidwebbrowsing/getAvgDelay',
      data : {month:yearMonth},  
      dataType : "json", //返回数据形式为json  
      success : function(result) {  
    	  appendIndexTable(result,"info-box bg-red");
      },  
      error : function(errorMsg) {  
          alert("页面时延--返回数据出错");  
      }  
  });
  
//90%页面时延
  $.ajax({  
      type : "post",  
      async : false, //同步执行  
      url:'groupidwebbrowsing/getPageAvgNinetyDelay',
      data : {month:yearMonth},  
      dataType : "json", //返回数据形式为json  
      success : function(result) {  
    	  appendIndexTable(result,"info-box bg-orange");
      },  
      error : function(errorMsg) {  
          alert("90%页面时延--返回数据出错");  
      }  
  });

  //页面成功率
  $.ajax({  
      type : "post",  
      async : false, //同步执行  
      url:'groupidwebbrowsing/getPageStandardRate',
      data : {month:yearMonth},  
      dataType : "json", //返回数据形式为json  
      success : function(result) {  
    	  appendIndexTable(result,"info-box bg-aqua");
      },  
      error : function(errorMsg) {  
          alert("页面成功率--返回数据出错");  
      }  
  });  
//视频时延
  $.ajax({  
      type : "post",  
      async : false, //同步执行  
      url:'groupidvideomap/getVideoDelay',
      data : {month:yearMonth},  
      dataType : "json", //返回数据形式为json  
      success : function(result) {  
    	  appendIndexTable(result,"info-box bg-light-blue");
      },  
      error : function(errorMsg) {  
          alert("视频时延--返回数据出错");  
      }  
  });  
//视频卡顿次数
  $.ajax({  
      type : "post",  
      async : false, //同步执行  
      url:'groupidvideomap/getCacheCount',
      data : {month:yearMonth},  
      dataType : "json", //返回数据形式为json  
      success : function(result) {  
    	  appendIndexTable(result,"info-box bg-olive");
      },  
      error : function(errorMsg) {  
          alert("视频卡顿次数--返回数据出错");  
      }  
  });  
  
//视频缓冲时长占比
  $.ajax({  
      type : "post",  
      async : false, //同步执行  
      url:'groupidvideomap/getAvgBufferProportion',
      data : {month:yearMonth},  
      dataType : "json", //返回数据形式为json  
      success : function(result) {  
    	  appendIndexTable(result,"info-box bg-purple");
      },  
      error : function(errorMsg) {  
          alert("视频缓冲时长占比--返回数据出错");  
      }  
  });  
  

//页面成功率
  $.ajax({  
      type : "post",  
      async : false, //同步执行  
      url:'groupidwebbrowsing/getPageBrowseSuccess',
      data : {month:yearMonth},  
      dataType : "json", //返回数据形式为json  
      success : function(result) {  
    	  appendIndexTable(result,"info-box bg-gray");
      },  
      error : function(errorMsg) {  
          alert("页面成功率--返回数据出错");  
      }  
  });  
  
//视频播放成功率
  $.ajax({  
      type : "post",  
      async : false, //同步执行  
      url:'groupidvideomap/getVideoPlaySuccess',
      data : {month:yearMonth},  
      dataType : "json", //返回数据形式为json  
      success : function(result) {  
    	  appendIndexTable(result,"info-box bg-lightgreen");
      },  
      error : function(errorMsg) {  
          alert("视频播放成功率--返回数据出错");  
      }  
  }); 
  
//http下载成功率
  $.ajax({  
      type : "post",  
      async : false, //同步执行  
      url:'groupidhttpdownloadmap/getHttpDownloadSuccessRate',
      data : {month:yearMonth},  
      dataType : "json", //返回数据形式为json  
      success : function(result) {  
    	  appendIndexTable(result,"info-box bg-darkgreen");
      },  
      error : function(errorMsg) {  
          alert("http下载成功率--返回数据出错");  
      }  
  }); 
  
  
//////////////////////////  end///////////////////////////////
    
//本月平台测试次数占比
  $.ajax({  
      type : "post",  
      async : false, //同步执行  
      url:'terminalOs/getMonthPlatTesttimeProportion',
      data : {month:yearMonth},  
      dataType : "json", //返回数据形式为json  
      success : function(result) {  
    	  for(var i=0;i<result.data.length;i++){
    		  var jsonObj={value:"",color:"",highlight:"",label:""};
    		  jsonObj.value=result.data[i].value;
    		  jsonObj.color=pie_color[i].color;
    		  jsonObj.highlight=pie_color[i].highlight;
    		  jsonObj.label=result.data[i].lable;
    		  PieData.push(jsonObj);
    		  $("ul[class='chart-legend clearfix']").append("<li><i class='fa fa-circle-o' style='color:"+pie_color[i].color+"'></i> "+result.data[i].lable+"</li>");
    	  }
    	  pieChart.Doughnut(PieData, pieOptions);
    	  
    	  
      },  
      error : function(errorMsg) {  
          alert("本月平台测试次数占比--返回数据出错");  
      }  
  });
  //拼测试报告tableBody
  function appendTable(result){
	  for(var i=0;i<result.data.length;i++){
		  $("#example1 tbody").append(
				  "<tr>"+
				  	//"<td style='display:none'>"+result.data[i].id+","+result.data[i].groupid+"</td>"+
				  	"<td>"+result.data[i].month+"</td>"+
				  	"<td>"+result.data[i].province+"</td>"+
				  	"<td>"+result.data[i].broadband_type+"</td>"+
				  	"<td>"+result.data[i].testtimes+"</td>"+
				  	/*"<td><a class='fa fa-download' href='adminLTE_zx/pages/examples/invoice.html'></td>"+*/
				  	"<td><a class='fa fa-download' onclick='targetFunc_report(\"adminLTE_zx/pages/examples/invoice.html\",\"testreport"+i+"\",\""+result.data[i].month+"\",\""+result.data[i].province+"\",\""+result.data[i].broadband_type+"\",\""+result.data[i].groupid+"\")' id='testreport"+i+"' href=''></td>"+
				  "</tr>"
		  );
	  }
  }
  //测试报告
  $.ajax({  
      type : "post",  
      async : false, //同步执行  
      url:'provincequalitymap/findAll',
      data : {month:preYearMonth},  
      dataType : "json", //返回数据形式为json  
      success : function(result) {  
    	  if(result.data.length>0){
    		  appendTable(result);
    	  }else{
    		  alert("测试报告--没有数据记录！");
    	  }
    	  
      },  
      error : function(errorMsg) {  
          alert("测试报告--返回数据出错");  
      }  
  });
  
  
});
