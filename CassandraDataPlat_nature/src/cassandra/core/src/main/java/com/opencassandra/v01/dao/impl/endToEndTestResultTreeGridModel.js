Ext.define('Result.model.endToEndTestResultTreeGridModel',{
	extend:'Ext.data.Model',
	alias:'widget.endToEndTestResultTreeGridModel',
	fields:[
           /* {name: 'imei',type: 'string'},
            {name: 'deviceType',type: 'string'},
            {name: 'size',type: 'string'},
            {name: 'updateTime',type: 'auto'},
            {name: 'version',type: 'auto'},
            {name: 'path',type: 'auto'},
            {name: 'taskNum',type: 'auto'},
            {name: 'caseId',type: 'auto'},
            {name: 'planId',type: 'auto'},
            {name: 'executeId',type: 'auto'},
            {name: 'planNum',type: 'auto'},
            {name: 'delay',type: 'auto'},
            {name: 'speed',type: 'auto'},
            {name: 'up_avg_speed',type: 'auto'},
            {name: 'down_avg_speed',type: 'auto'},
            {name: 'loadTime',type: 'auto'},
            {name: 'loadSpeed',type: 'auto'},
            {name: 'load_delay',type: 'auto'},
            {name: 'down_max_speed',type: 'auto'},
            {name: 'break_times',type: 'auto'},
            {name: 'break_max_delay',type: 'auto'},
            {name: 'fileSize',type: 'string'},
            {name: 'loadUrl',type: 'string'},
            {name: 'times',type: 'string'},
            {name: 'upTime',type: 'string'},
            {name: 'downTime',type: 'string'},
            {name: 'maxDelay',type: 'string'},
            {name: 'minDelay',type: 'string'},
            {name: 'avgDelay',type: 'string'},
            {name: 'testLocation',type: 'string'},
            {name: 'cellInfo',type: 'string'}*/
           
         //--------------基本字段------------------
           {name: 'imei',type: 'auto'},//IMEI号   IMEI
           {name: 'terminal_model',type: 'auto'},//终端类型   Terminal Model
           {name: 'file_name',type: 'auto'},//文件名称   Filename
           {name: 'file_size',type: 'auto'},//文件大小   File Number/Size
           {name: 'software_edition',type: 'auto'},//软件版本   Software Edition
           {name: 'test_location',type: 'auto'},//测试地点   Test Location
           {name: 'cell_info',type: 'auto'},//测试小区信息    Cell Info
           {name: 'testing_time',type: 'auto'},//测试时间  Testing Time  
         //--------------PING  PING ------------------
           {name: 'domain_address',type: 'auto'},//域名地址    Domain Address
           {name: 'max_time_delay',type: 'auto'},//最大时延    Max Time Delay
           {name: 'minimal_time_delay',type: 'auto'},//最小时延    Minimal Time Delay
           {name: 'average_time_delay',type: 'auto'},//平均时延    Average Time Delay
           {name: 'count',type: 'auto'},//次数    Count
         //-------------HTTP下载    HTTP Net Resource download -------------     
           {name: 'target_address',type: 'auto'},//目标地址    Target Address
           {name: 'net_resource_size',type: 'auto'},//目标文件大小   Net Resource Size
           {name: 'downloading_time',type: 'auto'},//下载时长   Downloading Time
           {name: 'mean_rate',type: 'auto'},//平均速率    Mean Rate
         //-------------HTTP上传    HTTP Resources Update -------------     
           {name: 'target_address',type: 'auto'},//目标地址    Target Address
           {name: 'resource_size',type: 'auto'},//目标文件大小  Resource Size
           {name: 'uploading_time',type: 'auto'},//上传时长   Uploading Time
           {name: 'mean_rate',type: 'auto'},//平均速率    Mean Rate   
        //-------------FTP下载    FTP Net Resource download -------------     
           {name: 'target_address',type: 'auto'},//目标地址    Target Address
           {name: 'net_resource_size',type: 'auto'},//目标文件大小   Net Resource Size
           {name: 'downloading_time',type: 'auto'},//下载时长    Downloading Time
           {name: 'mean_rate',type: 'auto'},//平均速率    Mean Rate          
        //-------------FTP上传    FTP Net Resource download -------------     
           {name: 'target_address',type: 'auto'},//目标地址    Target Address
           {name: 'resource_size',type: 'auto'},//目标文件大小   Resource Size
           {name: 'uploading_time',type: 'auto'},//上传时长    Uploading Time
           {name: 'mean_rate',type: 'auto'},//平均速率    Mean Rate          
        //-------------网络带宽测速    Speed Test -------------        
           {name: 'max_upstream_rate',type: 'auto'},//最大上行速率    Max Upstream Rate
           {name: 'mean_upstream_rate',type: 'auto'},//平均上行速率   Mean Upstream Rate
           {name: 'max_downstream_rate',type: 'auto'},//最大下行速率   Max Downstream Rate
           {name: 'mean_downstream_rate',type: 'auto'},//平均下行速率    Mean Downstream Rate
           {name: 'protocol',type: 'auto'},//协议    Protocol
           {name: 'threads',type: 'auto'},//线   Threads
           {name: 'e_time_delay',type: 'auto'},//时延    E-E Time Delay
           {name: 'server_ip',type: 'auto'},//ip地址    Server IP
        //-------------网页浏览    Web Browsing -------------     
           {name: 'access_address',type: 'auto'},//访问地址   Access Address
           {name: '80_loading_time',type: 'auto'},//80%加载时间   100% Loading Time
           {name: '100_loading_time',type: 'auto'},//显示100%加载时间   80% Loading Time
           {name: 'reference_rate',type: 'auto'},//参考速率   Reference Rate     
       //-------------电话  (电话)    Call -------------     
           {name: 'success_failure',type: 'auto'},//是否成功   Success /Failure
           {name: 'access_time',type: 'auto'},//   Access time、Recording
           {name: 'caller_number',type: 'auto'},//目标号码   Caller Number
           {name: 'caller_terminal_model',type: 'auto'},//  Caller Terminal Model
           {name: 'belonging_network_type',type: 'auto'},//所属网络类型   Belonging Network Type    
           {name: 'hand_over_situation',type: 'auto'},//  Hand Over Situation    
       //-------------视频测试    Video Test -------------     
           {name: 'access_address',type: 'auto'},//   Access Address
           {name: 'video_url',type: 'auto'},//   Video URL
           {name: 'buffering_time',type: 'auto'},//目标号码   Buffering Time
           {name: 'buffering_count',type: 'auto'},//  Caller Buffering Count
           {name: '80_loading_time',type: 'auto'},//   80% Loading Time   
           {name: '100_loading_time',type: 'auto'},// 100% Loading Time
           {name: 'reference_rate',type: 'auto'},// 参考速率  Reference Rate
       //-------------CSFB   CSFB -------------     
           {name: 'success_failure',type: 'auto'},//   Success /Failure
           {name: 'access_time',type: 'auto'},//   Access time
           {name: 'recording',type: 'auto'},//  Recording
           {name: 'caller_number',type: 'auto'},//  Caller Number
           {name: 'called_terminal_model',type: 'auto'},//  Called Terminal Model  
           {name: 'belonging_network_type',type: 'auto'},// Belonging Network Type
           {name: 'fallback_delay',type: 'auto'},//   Fallback Delay          
           {name: 'return_time_delay',type: 'auto'},//  Return Time Delay  
           {name: 'fallback_network_type',type: 'auto'},//   Fallback Network Type  
           {name: 'hand_over_situation',type: 'auto'},//   Hand Over Situation  
       //-------------Hand Over  Hand Over -------------     
           {name: 'time_duration',type: 'auto'},//   Time Duration
           {name: 'frequency',type: 'auto'},//   Frequency
           {name: 'max_time_delay',type: 'auto'},//  Max Time Delay
           {name: 'minimal_time_delay',type: 'auto'},//  Minimal Time Delay
           {name: 'Average Time Delay',type: 'auto'},//  Average Time Delay
                     
           
           
           {name: 'operations',type: 'auto'}//操作
           
    ]
});