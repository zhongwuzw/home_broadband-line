/* ots附加 js 2016-12-9 ph  */
$(function(){
	//双滚动条bug修复
	$(".nicescroll-rails").remove();
});
/*主导航*/
function headerNav(obj,url){
	
	$(obj).siblings().removeClass("current");
	$(obj).addClass("current");

	$(".J_mainWrap").load(url);
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
