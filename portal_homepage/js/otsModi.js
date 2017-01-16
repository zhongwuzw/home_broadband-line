/* ots附加 js 2016-12-9 ph  */
$(function(){
	//双滚动条bug修复
	$(".nicescroll-rails").remove();
	//方案左导航
	new navLeft.detailNavClk(".J_nav_left_list_case","project.html");
	//首页左导航
	new navLeft.detailNavClk(".J_nav_left_list_index","index_con.html");
	//产品左导航
	new navLeft.detailNavClk(".J_nav_left_list_pro","production.html");
});
//==详情左导航公共类==
var navLeft={};
navLeft.detailNavClk = function(obj,goBackUrl){
	this.obj = null;
	this.goBackUrl = null;
	this.init(obj,goBackUrl);
}
navLeft.detailNavClk.prototype={
	init: function(obj,goBackUrl){
		var self = this;
		self.obj = $(obj);
		//默认打开左导航
		self.defaultDetialCon(self,self.obj);
		//点击打开左导航
		self.obj.find("li").each(function(){
			//一级
		   $(this).find("h4").bind("click",function(){
				var thisHand = this;
				//有open标记的关闭
				if($(thisHand).hasClass("open")){
					$(thisHand).parent().removeClass("cur");
					$(thisHand).siblings(".J_sub_nav_list").addClass("hide");
					$(thisHand).attr("style","");
					$(thisHand).removeClass("open");
				}else{
					//无open标记的展开
					self.navClk(self,self.obj,thisHand);
				}
			});
		   
		});
		//所在位置文字加链接
		var curPosObj = self.obj.parent().parent().siblings(".J_navPosBox");
		var pareObj = self.obj.closest(".J_mainWrap");
		self.goBackPos(curPosObj,pareObj,goBackUrl);
		
	},
	//一级导航展开
	navClk:function(self,navObj,navHand){
		//其它菜单项
		navObj.find("li").removeClass("cur");
		navObj.find("li").find(".J_sub_nav_list").addClass("hide");
		navObj.find("li h4").attr("style","background:url(img/detail/morearrow.png) right center no-repeat;");
		//清掉默认的当前二级背景色
		navObj.find("li p").removeClass("cur_subNavP");
		//当前菜单项
		$(navHand).parent().addClass("cur");
		$(navHand).siblings(".J_sub_nav_list").removeClass("hide");
		$(navHand).attr("style","background:url(img/detail/morearrow-up.png) right center no-repeat;");
		//其它去掉展开标记
		navObj.find("li h4").removeClass("open");
		//加展开标记
		$(navHand).addClass("open");
		//所在位置
		var curPosObj = navObj.parent().parent().siblings(".J_navPosBox");
		var curText = $(navHand).text();
		self.currentPos(curPosObj,curText);
		//展开二级
		var rightCon = navObj.parent().siblings().find(".J_art_con");
		self.subNavClk(navHand,rightCon);
	},
	//二级导航展开
	subNavClk:function(subObj,rightCon){
		$(subObj).next().find("p a").each(function(){
			$(this).bind("click",function(){
				var showIdStr = $(this).attr("href");
				var showId = showIdStr.substring(1,showIdStr.length);
				//其它区块隐藏
				$(rightCon).addClass("hide");
				//当前区块显示
				$("#"+showId).removeClass("hide");
				//=二级导航变色=
				//其它
				$(this).parent().siblings().removeClass("cur_subNavP");
				//当前
				$(this).parent().addClass("cur_subNavP");
			});
		});
	},
	//所在位置
	currentPos:function(curPosObj,tt){
		//换文字
		$(curPosObj).find("span:last-child").text(tt);
	},
	//所在位置加链接
	goBackPos:function(curPosObj,pareObj,goBackUrl){
		$(curPosObj).find("a").bind("click",function(){
			pareObj.load(goBackUrl);		 
		});
	},
	//默认打开左导航
	defaultDetialCon:function(self,navObj){
		var str1 = location.href;
		if(str1.indexOf("?")>-1){
			var str2 = str1.substr(str1.indexOf("?")+1).split("=");
			var defaultShowId;
			//单击导航时记地址有#号，按#号后面标记显示右侧内容
			if(str2[1].indexOf("#")>-1){
				defaultShowId = str2[1].substr(str2[1].indexOf("#")+1);
			}else{
				defaultShowId = str2[1];
			}
		}
		
		var curLi = $("#tab_"+defaultShowId).closest("li");
		//当前一级菜单项
		var posCurH4 = curLi.find("h4");
		//当前二级菜单项
		$("#tab_"+defaultShowId).parent().addClass("cur_subNavP");
		//右侧其它隐藏
		$(".J_art_con").addClass("hide");
		//右侧当前显示
		$("#"+defaultShowId).removeClass("hide");
		//左导航一级展开
		self.navClk(self,navObj,posCurH4);
	}
	
}
//==详情左导航公共类 完==
//顶导航链接
function navTopCur(numb){
	$(".J_otsNav li").removeClass("current");
	$(".J_otsNav li:eq("+numb+")").addClass("current");
}
/*主导航*/
function headerNav(obj,url){
	$(obj).siblings().removeClass("current");
	$(obj).addClass("current");

	$(".J_mainWrap").load(url);
	//手机版导航点击后隐藏导航
	$(obj).parent().parent().removeClass("in");
}
//详情页主导航
function headerNavDtl(obj,url){
	$(obj).siblings().removeClass("current");
	$(obj).addClass("current");
	window.open("index.html","_parent");
	//$(".J_mainWrap").load(url);
	//手机版导航点击后隐藏导航
	$(obj).parent().parent().removeClass("in");
}
//下载描述panel
function showDownloadPanel(obj){
	setArticleZindex(obj);
	$(obj).parent().siblings(".pos_son").removeClass("hide");
}
function showDownloadPanel_2(obj){
	setArticleZindex(obj);
	$(obj).removeClass("hide");
}
function hideDownloadPanel(obj){
	delArticleZindex(obj);
	$(obj).parent().siblings(".pos_son").addClass("hide");
}
function hideDownloadPanel_2(obj){
	delArticleZindex(obj);
	$(obj).addClass("hide");
}
/*解决层级压制兼容性问题*/
function setArticleZindex(obj){
	$(".pos_pare").not($(obj).parents(".pos_pare")).addClass("zindex_0");
}
function delArticleZindex(obj){
	$(".pos_pare").not($(obj).parents(".pos_pare")).removeClass("zindex_0");
}


