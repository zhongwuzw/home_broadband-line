<!DOCTYPE html>
<!--[if lt IE 7]>      <html class="no-js lt-ie9 lt-ie8 lt-ie7"> <![endif]-->
<!--[if IE 7]>         <html class="no-js lt-ie9 lt-ie8"> <![endif]-->
<!--[if IE 8]>         <html class="no-js lt-ie9"> <![endif]-->
<!--[if gt IE 8]><!--> <html class="no-js"> <!--<![endif]-->
    <head>
		<meta http-equiv="X-UA-Compatible" content="IE=Edge,chrome=1" >
		<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
		<meta name="description" content="Open Testing System">
        <meta name="China Mobile" content="China Mobile">
		
        <title>Open Testing System</title>
		<!-- Mobile Specific Meta
		================================================== -->
        <meta name="viewport" content="width=device-width, initial-scale=1">
		
		<!-- Favicon -->
		<link rel="shortcut icon" type="image/x-icon" href="img/favicon.png" />
		
		<!-- CSS
		================================================== -->
		<!-- Fontawesome Icon font -->
        <link rel="stylesheet" href="css/font-awesome.min.css">
		<!-- bootstrap.min css -->
        <link rel="stylesheet" href="css/bootstrap.min.css">
		<!-- Animate.css -->
        <link rel="stylesheet" href="css/animate.css">
		<!-- Owl Carousel -->
        <link rel="stylesheet" href="css/owl.carousel.css">
		<!-- Grid Component css -->
        <link rel="stylesheet" href="css/component.css">
		<!-- Slit Slider css -->
        <link rel="stylesheet" href="css/slit-slider.css">
		<!-- Main Stylesheet -->
        <link rel="stylesheet" href="css/main.css">
		<!-- Media Queries -->
        <link rel="stylesheet" href="css/media-queries.css">
		<!-- 页面特殊样式表 -->
        <link rel="stylesheet" href="css/page.css">	
		<!--红版样式modify_red.css-->
		<!--湖水蓝版样式modify_blue.css-->
		<!--灰版样式modify_gray.css-->
		<!--link rel="stylesheet" href="css/modify_gray.css"-->
		<!-- Modernizer Script for old Browsers -->		
		
        <script src="js/modernizr-2.6.2.min.js"></script>
    </head>
	
    <body id="body">
	    <!--
	    Start 加载图标
	    ==================================== -->
		<div id="loading-mask">
			<div class="loading-img">
				<img alt="Meghna Preloader" src="img/preloader.gif"  />
			</div>
		</div>
        <!--
        End 加载图标
        ==================================== -->


		<!--/#首页 section-->
		
        <!-- 
        start 头导航
        ==================================== -->
		
        <header id="navigation" class="navbar navbar-inverse">
			<div class="container">
                <div class="navbar-header col-sm-5">
                    <!-- responsive nav button -->
					<button type="button" class="navbar-toggle" data-toggle="collapse" data-target=".navbar-collapse">
                        <span class="sr-only">Toggle navigation</span>
                        <span class="icon-bar"></span>
                        <span class="icon-bar"></span>
                        <span class="icon-bar"></span>
                    </button>
					<!-- /responsive nav button -->
					
					<!-- logo -->
                    <a class="navbar-brand col-md-8 col-xs-9" href="#body">
						<h1 id="logo">
							<img src="img/logo.png" width="100%" alt="Meghna" />
						</h1>
					</a>
					<!-- /logo -->
                </div>
				
				<!-- main nav -->
				
                <nav class="collapse navbar-collapse navbar-right" role="Navigation">
                    <ul id="nav" class="nav navbar-nav J_otsNav">
						<li onClick="headerNav(this,'index_con.html')"><a href="#">首 页</a></li>
                        <li onClick="headerNav(this,'project.html')"><a href="#">方 案</a></li>
                        <li onClick="headerNav(this,'production.html')"><a href="#">产 品</a></li>
                        <li onClick="headerNav(this,'download.html')"><a href="#">下 载</a></li>
						<li class="current" onClick="headerNav(this,'support.html')"><a href="#">支 持</a></li>
                        <li onClick="headerNav(this,'aboutus.html')"><a href="#">关 于</a></li>
                    </ul>
                </nav>
				
				<!-- /main nav -->
				
            </div>
        </header>
		<!-- 
        end 头导航
        ==================================== -->
		<!--start 主体页面-->
		<div class="J_mainWrap">
			<!-- Start FAQ section
		=========================================== -->
		
		<section id="pricing" class="bg-one bg_modi">
			<div class="container">
				<div class="row">
					
					<!-- section title -->
				    <div class="title text-center wow fadeInDown" data-wow-duration="500ms">
			        	<h2>常见问题 <span class="color">FAQ</span></h2>
				        <div class="border"></div>
				    </div>
				    <!-- /section title -->
					
					<article class="col-md-3 col-sm-6 col-xs-12 text-center wow fadeInUp" data-wow-duration="200ms">
						<div class="pricing">
							
							<div class="price-title">
								<h3>业务质量测试工具</h3>
								<p>SQI Tools</p>
							</div>

							<ul>
								<li>什么是SQI</li>
								<li>测试类型</li>
								<li>适用场景</li>
								<li>部署方式</li>
								<li>服务策略</li>
								<li>&nbsp;</li>
							</ul>

							<a class="btn btn-transparent" href="#">更多</a>
							
						</div>
					</article>

					<article class="col-md-3 col-sm-6 col-xs-12 text-center wow fadeInUp" data-wow-duration="500ms" data-wow-delay="400ms">
						<div class="pricing">
						
							<div class="price-title">
								<h3>业务性能测试工具</h3>
								<p>PT Tool</p>
							</div>

							<ul>
								<li>什么是PT</li>
								<li>测试类型</li>
								<li>协议支持</li>
								<li>适用场景</li>
								<li>部署方式</li>
								<li>服务策略</li>
							</ul>

							<a class="btn btn-transparent" href="#">更多</a>
							
						</div>
					</article>
					<article class="col-md-3 col-sm-6 col-xs-12 text-center wow fadeInUp" data-wow-duration="500ms" data-wow-delay="600ms">
						<div class="pricing">
							
							<div class="price-title">
								<h3>可信数据连接云</h3>
								<p>T-DLC Service</p>
							</div>

							<ul>
								<li>什么是T-DLC</li>
								<li>存储要求</li>
								<li>数据安全</li>
								<li>部署方式</li>
								<li>服务策略</li>
								<li>&nbsp;</li>
							</ul>

							<a class="btn btn-transparent" href="#">更多</a>

						</div>
					</article>

					<article class="col-md-3 col-sm-6 col-xs-12 text-center wow fadeInUp" data-wow-duration="500ms" data-wow-delay="750ms">
						<div class="pricing kill-margin-bottom">
						
							<div class="price-title">
								<h3>测试大数据可视化</h3>
								<p>DVoT Service</p>
							</div>

							<ul>
								<li>什么是DVoT</li>
								<li>数据类型</li>
								<li>展示维度</li>
								<li>部署方式</li>
								<li>服务策略</li>
								<li>&nbsp;</li>
							</ul>

							<a class="btn btn-transparent" href="#">更多</a>
							
						</div>
					</article>
					<!-- end single pricing table -->
				    
					
				</div>       <!-- End row -->
			</div>   	<!-- End container -->
		</section>   <!-- End FAQ section -->
		
	
		<!-- Srart Contact Us
		=========================================== -->		
		<section id="contact-us">
			<div class="container">
				<div class="row">
					
					<!-- section title -->
					<div class="title text-center wow fadeIn" data-wow-duration="500ms">
						<h2>联系 <span class="color">我们</span></h2>
						<div class="border"></div>
					</div>
					<!-- /section title -->
					
					<!-- Contact Details -->
					<div class="contact-info col-md-6 wow fadeInUp" data-wow-duration="500ms">
						<p>OTS云测试系列工具由中国移动通信有限公司研究院研发出品，如希望获得更多信息，欢迎随时与我们联系。</p>
						<div class="contact-details">
							<div class="con-info clearfix">
								<i class="fa fa-home fa-lg"></i>
								<span>地址：北京市西城区宣武门西大街32号</span>
							</div>
							
							<div class="con-info clearfix">
								<i class="fa fa-phone fa-lg"></i>
								<span>电话客服：13810162494 或 13522068083</span>
							</div>
							
							<div class="con-info clearfix">
								<i class="fa fa-fax fa-lg"></i>
								<span>飞信客服：999938096</span>
							</div>
							
							<div class="con-info clearfix">
								<i class="fa fa-envelope fa-lg"></i>
								<span>邮箱地址：ots@chinamobile.com</span>
							</div>
							<div class="con-info clearfix">
								<img src="img/support/qrcode.jpg"/>
							</div>
						</div>
					</div>
					<!-- / End Contact Details -->
						
					<!-- Contact Form -->
					<div class="contact-form col-md-6 wow fadeInUp" data-wow-duration="500ms" data-wow-delay="300ms">
						<form id="contact-form" method="post" action="sendmail.php" role="form">
						
							<div class="form-group">
								<input type="text" placeholder="发信人" class="form-control" name="name" id="name">
							</div>
							
							<div class="form-group">
								<input type="email" placeholder="邮箱" class="form-control" name="email" id="email">
							</div>
							
							<div class="form-group">
								<input type="text" placeholder="主题" class="form-control" name="subject" id="subject">
							</div>
							
							<div class="form-group">
								<textarea rows="6" placeholder="内容" class="form-control" name="message" id="message"></textarea>	
							</div>
							
							<div id="mail-success" class="success">
								邮件发送中！～
							</div>
							
							<div id="mail-fail" class="error">
								对不起，邮件发送失败，请稍后重试！～
							</div>
							
							<div id="cf-submit">
								<input type="submit" id="contact-submit" class="btn btn-transparent" value="发 送">
							</div>						
							
						</form>
					</div>
					<!-- ./End Contact Form -->
				
				</div> <!-- end row -->
			</div> <!-- end container -->
			
		</section> <!-- end section -->
		</div>
		<!--end 主体页面-->
		<!--start 底-->
		<footer id="footer" class="bg-one">
			<div class="container">
			    <div class="row wow fadeInUp" data-wow-duration="500ms">
					<div class="col-lg-12">
						
						<!-- Footer 友情链接 -->
						<div class="social-icon">
							<!--ul>
								<li><a href="#"><i class="fa fa-facebook"></i></a></li>
								<li><a href="#"><i class="fa fa-twitter"></i></a></li>
								<li><a href="#"><i class="fa fa-google-plus"></i></a></li>
								<li><a href="#"><i class="fa fa-youtube"></i></a></li>
								<li><a href="#"><i class="fa fa-linkedin"></i></a></li>
								<li><a href="#"><i class="fa fa-dribbble"></i></a></li>
								<li><a href="#"><i class="fa fa-pinterest"></i></a></li>
							</ul-->
						</div>
						<!--/. End Footer 友情链接 -->

						<!-- copyright -->
						<div class="copyright text-center">
							<p> Copyright &copy; 2009-2017 中国移动通信研究院 All Rights Reserved.</p>
						</div>
						<!-- /copyright -->
						
					</div> <!-- end col lg 12 -->
				</div> <!-- end row -->
			</div> <!-- end container -->
		</footer> <!-- end 底 -->
		
		<!-- 返回顶部
		============================== -->
		<a href="javascript:;" id="scrollUp">
			<i class="fa fa-angle-up fa-2x"></i>
		</a>

		
		<!-- 
		javascript引用
		=====================================-->
		<!-- Main jQuery -->
		<script src="js/jquery-1.11.0.min.js"></script>
		<!--ots附加 主导航-->
		<!--script src="js/otsModi.js"></script-->
		<!-- Bootstrap 3.1 -->
		<script src="js/bootstrap.min.js"></script>
		<!--home Slitslider -->
		<script src="js/jquery.slitslider.js"></script>
		<!--script src="js/jquery.ba-cond.min.js"></script-->
		<!--home Parallax -->
		<script src="js/jquery.parallax-1.1.3.js"></script>
		<!--home Owl Carousel -->
		<script src="js/owl.carousel.min.js"></script>
		<!--home Portfolio Filtering -->
		<script src="js/jquery.mixitup.min.js"></script>
		<!--home Custom Scrollbar 鼠标滚轮特效-->
		<script src="js/jquery.nicescroll.min.js"></script>
		<!--home Jappear js -->
		<script src="js/jquery.appear.js"></script>
		<!-- Pie Chart 产品特点-->
		<!--script src="js/easyPieChart.js"></script-->
		<!--home jQuery Easing 返回顶部和这里有关-->
		<script src="js/jquery.easing-1.3.pack.js"></script>
		<!--home Highlight menu item -->
		<script src="js/jquery.nav.js"></script>
		<!--home Sticky Nav -->
		<script src="js/jquery.sticky.js"></script>
		<!--home Number Counter Script -->
		<script src="js/jquery.countTo.js"></script>
		<!--home wow.min Script -->
		<script src="js/wow.min.js"></script>
		<!--home For video responsive -->
		<script src="js/jquery.fitvids.js"></script>
		<!--home Grid js -->
		<script src="js/grid.js"></script>
		<!--home Custom js -->
		<script src="js/custom.js"></script>

    </body>
</html>

