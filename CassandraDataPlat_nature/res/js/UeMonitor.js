var markerClusterer;
var lastuploadtime;
Ext.require([
    'Ext.form.*',
    'Ext.data.*',
    'Ext.chart.*',
    'Ext.grid.Panel',
    'Ext.layout.container.Column'
]);


Ext.onReady(function(){
        
    //use a renderer for values in the data view.
    function perc(v) {
        return v + '%';
    }

    var form = false,
        selectedRec = false,
        //performs the highlight of an item in the bar series
        highlightCompanyPriceBar = function(storeItem) {
            var name = storeItem.get('company'),
                series = barChart.series.get(0),
                i, items, l;

            series.highlight = true;
            series.unHighlightItem();
            series.cleanHighlights();
            for (i = 0, items = series.items, l = items.length; i < l; i++) {
                if (name == items[i].storeItem.get('company')) {
                    series.highlightItem(items[i]);
                    break;
                }
            }
            series.highlight = false;
        },
        // Loads fresh records into the radar store based upon the passed company record
        updateRadarChart = function(rec) {
            radarStore.loadData([{
                'Name': '设备总个数',
                'Data': ((rand() * 10000) >> 0) / 100
            }, {
                'Name': '在线设备数',
                'Data': ((rand() * 10000) >> 0) / 100
            }, {
                'Name': '离线设备数',
                'Data': ((rand() * 10000) >> 0) / 100
            }, {
                'Name': '运行设备数',
                'Data': ((rand() * 10000) >> 0) / 100
            }, {
                'Name': '待机设备数',
                'Data': ((rand() * 10000) >> 0) / 100
            }]);
        };
        
    // sample static data for the store
    var myData = [
        ['3m Co'],
        ['Alcoa Inc'],
        ['Altria Group Inc']
    ];
    
    for (var i = 0, l = myData.length, rand = Math.random; i < l; i++) {
        var data = myData[i];
        data[1] = ((rand() * 10000) >> 0) / 100;
        data[2] = ((rand() * 10000) >> 0) / 100;
        data[3] = ((rand() * 10000) >> 0) / 100;
        data[4] = ((rand() * 10000) >> 0) / 100;
        data[5] = ((rand() * 10000) >> 0) / 100;
    }

    //create data store to be shared among the grid and bar series.
    var ds = Ext.create('Ext.data.ArrayStore', {
        fields: [
            {name: 'company'},
            {name: 'price',   type: 'float'},
            {name: 'revenue %', type: 'float'},
            {name: 'growth %',  type: 'float'},
            {name: 'product %', type: 'float'},
            {name: 'market %',  type: 'float'}
        ],
        data: myData,
        listeners: {
            beforesort: function() {
                if (barChart) {
                    var a = barChart.animate;
                    barChart.animate = false;
                    barChart.series.get(0).unHighlightItem();
                    barChart.animate = a;
                }
            },
            //add listener to (re)select bar item after sorting or refreshing the dataset.
            refresh: {
                fn: function() {
                    if (selectedRec) {
                        highlightCompanyPriceBar(selectedRec);
                    }
                },
                // Jump over the chart's refresh listener
                delay: 1
            }
        }
    });

		// The data store containing the list of states
		var deviceType = Ext.create('Ext.data.Store', {
		    fields: ['abbr', 'name'],
		    data : [
		        {"abbr":"td-lte", "name":"TD-LTE"},
		        {"abbr":"td-scdma", "name":"TD-SCDMA"},
		        {"abbr":"gsm", "name":"GSM"}
		        //...
		    ]
		});
		// The data store containing the list of states
		var deviceModel = Ext.create('Ext.data.Store', {
		    fields: ['abbr', 'name'],
		    data : [
		        {"abbr":"samsung_I9100", "name":"Samsung_I9100"},
		        {"abbr":"huawei_d2", "name":"Huawei_D2"},
		        {"abbr":"sony_mtg05", "name":"Sony_mtg05"}
		        //...
		    ]
		});
		// The data store containing the list of states
		var deviceMfr = Ext.create('Ext.data.Store', {
		    fields: ['abbr', 'name'],
		    data : [
		        {"abbr":"samsung", "name":"Samsung"},
		        {"abbr":"huawei", "name":"Huawei"},
		        {"abbr":"sony", "name":"Sony"}
		        //...
		    ]
		});
		// The data store containing the list of states
		var deviceLocation = Ext.create('Ext.data.Store', {
		    fields: ['abbr', 'name'],
		    data : [
		        {"abbr":"北京", "name":"北京"},
		        {"abbr":"上海", "name":"上海"},
		        {"abbr":"江西", "name":"江西"}
		        //...
		    ]
		});
		// The data store containing the list of states
		var deviceState = Ext.create('Ext.data.Store', {
		    fields: ['abbr', 'name'],
		    data : [
		        {"abbr":"在线", "name":"在线"},
		        {"abbr":"离线", "name":"离线"},
		        {"abbr":"异常", "name":"异常"}
		        //...
		    ]
		});
		// The data store containing the list of states
		var deviceTestState = Ext.create('Ext.data.Store', {
		    fields: ['abbr', 'name'],
		    data : [
		        {"abbr":"运行中", "name":"运行中"},
		        {"abbr":"待机", "name":"待机"},
		        {"abbr":"暂停", "name":"暂停"}
		        //...
		    ]
		});

    //create radar store.
    var radarStore = Ext.create('Ext.data.JsonStore', {
        fields: ['Name', 'Data'],
        data: [
        {
            'Name': '设备总个数',
            'Data': 100
        }, {
            'Name': '在线设备数',
            'Data': 100
        }, {
            'Name': '离线设备数',
            'Data': 100
        }, {
            'Name': '运行设备数',
            'Data': 100
        }, {
            'Name': '待机设备数',
            'Data': 100
        }]
    });
    
    //Radar chart will render information for a selected company in the
    //list. Selection can also be done via clicking on the bars in the series.
    var radarChart = Ext.create('Ext.chart.Chart', {
        margin: '0 0 0 0',
        insetPadding: 20,
        flex: 1.2,
        animate: true,
        store: radarStore,
        theme: 'Blue',
        axes: [{
            steps: 5,
            type: 'Radial',
            position: 'radial',
            maximum: 100
        }],
        series: [{
            type: 'radar',
            xField: 'Name',
            yField: 'Data',
            showInLegend: false,
            showMarkers: true,
            markerConfig: {
                radius: 4,
                size: 4,
                fill: 'rgb(69,109,159)'
            },
            style: {
                fill: 'rgb(194,214,240)',
                opacity: 0.5,
                'stroke-width': 0.5
            }
        }],
        listeners: {
                click: function(item) {
                     updateRadarChart();
                }
        }
    });
    
    //create a grid that will list the dataset items.
    var mapPanel = Ext.create('Ext.panel.Panel', {
        id: 'company-form',
        //store: ds,
        //header: false,
        title:'地理位置关联',
        contentEl:'mapFrame',  
        width:700,  
        height:300,  
        draggable:false,  
        closable:true,  
        collapsible:true,  
        resizable:false  
    });
    
    //create a bar series to be at the top of the panel.
    var barChart = Ext.create('Ext.chart.Chart', {
        height: 200,
        margin: '5 0 3 0',
        cls: 'x-panel-body-default',
        shadow: true,
        animate: true,
        store: ds,
        axes: [{
            type: 'Numeric',
            position: 'left',
            fields: ['price'],
            minimum: 0,
            hidden: true
        }, {
            type: 'Category',
            position: 'bottom',
            fields: ['company'],
            label: {
                renderer: function(v) {
                    return Ext.String.ellipsis(v, 15, false);
                },
                font: '9px Arial',
                rotate: {
                    degrees: 270
                }
            }
        }],
        series: [{
            type: 'column',
            axis: 'left',
            style: {
                fill: '#456d9f'
            },
            highlightCfg: {
                fill: '#a2b5ca'
            },
            label: {
                contrast: true,
                display: 'insideEnd',
                field: 'price',
                color: '#000',
                orientation: 'vertical',
                'text-anchor': 'middle'
            },
            listeners: {
                itemmouseup: function(item) {
                     var series = barChart.series.get(0);
                     //gridPanel.getSelectionModel().select(Ext.Array.indexOf(series.items, item));
                }
            },
            xField: 'name',
            yField: ['price']
        }]
    });
    
    /*
     * Here is where we create the main Panel
     */
    var combotpl = Ext.create('Ext.XTemplate',
        '<tpl for=".">',
            '<div style="text-align:center;">{name}</div>',
        '</tpl>'
    );
    Ext.create('Ext.panel.Panel', {
        title: '终端状态监测',
        frame: true,
        bodyPadding: 5,
        width: 1000,
        height: 720,
        style: 'margin: 0 auto',
				
        fieldDefaults: {
            labelAlign: 'left',
            msgTarget: 'side'
        },
    
        layout: {
            type: 'vbox',
            align: 'stretch'
        },
        
        items: [{
            xtype: 'container',
            layout: {type: 'hbox', align: 'stretch'},
            flex: 3,
            items: [{
            		id: 'searchaera',
                xtype: 'form',
                flex: 4,
                layout: {
                    type: 'vbox',
                    align:'stretch'
                },
                margin: '0 0 0 0',
                title: 'UE Details',
                items: [{
                    margin: '5',
                    xtype: 'fieldset',
                    flex: 1.4,
                    title:'Device Filter',
                    defaults: {
                        width: 240,
                        labelWidth: 90,
                        disabled: false,
                        // min/max will be ignored by the text field
                        maxValue: 100,
                        minValue: 0,
                        enforceMaxLength: true,
                        maxLength: 200,
                        bubbleEvents: ['change']
                    },
                    defaultType: 'combobox',
                    items: [{
                    		id: 'dt',
                    		emptyText: 'ALL',
                        fieldLabel: '设备类型',
                        //name: 'company',
                        //displayTpl: combotpl,
                        //enforceMaxLength: false,
                        store: deviceType,
										    queryMode: 'local',
										    displayField: 'name',
										    valueField: 'abbr'
                    }, {
                    		id: 'dmf',
                    		emptyText: 'ALL',
                        fieldLabel: '设备厂商',
                        //name: 'price',
                        //displayTpl: combotpl,
                        store: deviceMfr,
										    queryMode: 'local',
										    displayField: 'name',
										    valueField: 'abbr'
                    }, {
                    		id: 'dm',
                    		emptyText: 'ALL',
                        fieldLabel: '设备型号',
                        //name: 'revenue %',
                        //displayTpl: combotpl,
                        store: deviceModel,
										    queryMode: 'local',
										    displayField: 'name',
										    valueField: 'abbr'
                    }, {
                    		id: 'dl',
                    		emptyText: 'ALL',
                        fieldLabel: '设备位置',
                        //name: 'growth %',
                        //displayTpl: combotpl,
                        store: deviceLocation,
										    queryMode: 'local',
										    displayField: 'name',
										    valueField: 'abbr'
                    }, {
                    		id: 'ds',
                    		emptyText: 'ALL',
                        fieldLabel: '设备状态',
                        //name: 'product %',
                        //displayTpl: combotpl,
                        store: deviceState,
										    queryMode: 'local',
										    displayField: 'name',
										    valueField: 'abbr'
                    }, {
                    		id: 'dtt',
                    		emptyText: 'ALL',
                        fieldLabel: '测试类型',
                        //name: 'market %',
                        //displayTpl: combotpl,
                        store: deviceTestState,
										    queryMode: 'local',
										    displayField: 'name',
										    valueField: 'abbr'
                    }, {
								        xtype: 'button',
								        text : '提交',
								        listeners: {
										        click: function() {
										            // this == the button, as we are in the local scope
										            doFilter();
										        }
										    }
                    }, {
                        xtype: 'button',
								        text : '重置',
								        margin: '3 0 0 0',
								        listeners: {
										        click: function() {
										            // this == the button, as we are in the local scope
										            doReset();
										        }
										    }
                    }]
                }, radarChart],
                listeners: {
                    // buffer so we don't refire while the user is still typing
                    buffer: 200,
                    change: function(field, newValue, oldValue, listener) {
                        if (selectedRec && form) {
                            if (newValue > field.maxValue) {
                                field.setValue(field.maxValue);
                            } else {
                                if (form.isValid()) {
                                    form.updateRecord(selectedRec);
                                    updateRadarChart(selectedRec);
                                }
                            }
                        }
                    }
                }
            },mapPanel]
        },barChart],
        renderTo: Ext.getBody()
    });
    doAll();
    
    setInterval("doRefresh()",5000);
});

function doFilter(){
		map.clearOverlays();
		nodeArray = new Array();
   	var deviceTypeStr = "all";
		var deviceMfrStr = "all";
		var deviceModelStr = "all";
		var deviceLocationStr = "all";
		var deviceStateStr = "all";
		var deviceTestStateStr = "all";
		if(Ext.getCmp('dt').getValue()!=null && Ext.getCmp('dt').getValue()!=''){
				deviceTypeStr = Ext.getCmp('dt').getValue();
		}
		if(Ext.getCmp('dmf').getValue()!=null && Ext.getCmp('dmf').getValue()!=''){
				deviceMfrStr = Ext.getCmp('dmf').getValue();
		}
		if(Ext.getCmp('dm').getValue()!=null && Ext.getCmp('dm').getValue()!=''){
				deviceModelStr = Ext.getCmp('dm').getValue();
		}
		if(Ext.getCmp('dl').getValue()!=null && Ext.getCmp('dl').getValue()!=''){
				deviceLocationStr = Ext.getCmp('dl').getValue();
		}
		if(Ext.getCmp('ds').getValue()!=null && Ext.getCmp('ds').getValue()!=''){
				deviceStateStr = Ext.getCmp('ds').getValue();
		}
		if(Ext.getCmp('dtt').getValue()!=null && Ext.getCmp('dtt').getValue()!=''){
				deviceTestStateStr = Ext.getCmp('dtt').getValue();
		}
    $.ajax( {
				url : '/finddevice.action',
				contentType : "application/json",
				data: 'deviceType='+deviceTypeStr+'&deviceMfr='+deviceMfrStr+'&deviceModel='+deviceModelStr+'&deviceLocation='+deviceLocationStr+'&deviceState='+deviceStateStr+'&deviceTestState='+deviceTestStateStr+'&lastuploadtime='+lastuploadtime,
				dataType : "json",
				timeout : 3000,
				async : true,
				error : function(XMLHttpRequest, textStatus, errorThrown) {
					alert("action "+errorThrown);
					return false;
				},
				success : function(data) {
					if(data != null){
							Ext.getCmp('searchaera').setTitle("已注册终端数:  "+data.length+"个");
							var jsonarray = data;
							var ueindex;
							myData = [
					    ];
							for(ueindex=0; ueindex<jsonarray.length; ueindex++){
									var datastore= new Array();
									datastore[0] = jsonarray[ueindex].model+"_"+jsonarray[ueindex].id;
									datastore[1] = ((Math.random() * 10000) >> 0) / 100;
					        datastore[2] = ((Math.random() * 10000) >> 0) / 100;
					        datastore[3] = ((Math.random() * 10000) >> 0) / 100;
					        datastore[4] = ((Math.random() * 10000) >> 0) / 100;
					        datastore[5] = ((Math.random() * 10000) >> 0) / 100;
					        myData.push(datastore);
									var nodeinstance = getNode(jsonarray[ueindex].id,jsonarray[ueindex].longtitude,jsonarray[ueindex].latitude,jsonarray[ueindex].device_state);
									nodeinstance.excu("<div>测试运行状态"+jsonarray[ueindex].device_state+"<br>上次更新时间："+jsonarray[ueindex].last_update+"</div>");
							}
							
							//markerClusterer =BMapLib.MarkerClusterer(map);
							if(markerClusterer){
									markerClusterer.clearMarkers();
							}
							var markers = [];
							for(var i=0; i<nodeArray.length; i++){
									markers.push(nodeArray[i].carMarker);
							}
							//alert(nodeArray.length);
							//markerClusterer.addMarkers({markers:markers}) ;
							markerClusterer = new BMapLib.MarkerClusterer(map, {markers:markers});
							
							//ds.loadData(myData);
					}
				}
		});
}

function doReset(){
    Ext.getCmp('dt').reset();
    Ext.getCmp('dmf').reset();
    Ext.getCmp('dm').reset();
    Ext.getCmp('dl').reset();
    Ext.getCmp('ds').reset();
    Ext.getCmp('dtt').reset();
}

function doAll(){
		var deviceTypeStr = "all";
		var deviceMfrStr = "all";
		var deviceModelStr = "all";
		var deviceLocationStr = "all";
		var deviceStateStr = "all";
		var deviceTestStateStr = "all";
		if(Ext.getCmp('dt').getValue()!=null && Ext.getCmp('dt').getValue()!=''){
				deviceTypeStr = Ext.getCmp('dt').getValue();
		}
		if(Ext.getCmp('dmf').getValue()!=null && Ext.getCmp('dmf').getValue()!=''){
				deviceMfrStr = Ext.getCmp('dmf').getValue();
		}
		if(Ext.getCmp('dm').getValue()!=null && Ext.getCmp('dm').getValue()!=''){
				deviceModelStr = Ext.getCmp('dm').getValue();
		}
		if(Ext.getCmp('dl').getValue()!=null && Ext.getCmp('dl').getValue()!=''){
				deviceLocationStr = Ext.getCmp('dl').getValue();
		}
		if(Ext.getCmp('ds').getValue()!=null && Ext.getCmp('ds').getValue()!=''){
				deviceStateStr = Ext.getCmp('ds').getValue();
		}
		if(Ext.getCmp('dtt').getValue()!=null && Ext.getCmp('dtt').getValue()!=''){
				deviceTestStateStr = Ext.getCmp('dtt').getValue();
		}
		
    $.ajax( {
				url : '/finddevice.action',
				contentType : "application/json",
				data: 'deviceType='+deviceTypeStr+'&deviceMfr='+deviceMfrStr+'&deviceModel='+deviceModelStr+'&deviceLocation='+deviceLocationStr+'&deviceState='+deviceStateStr+'&deviceTestState='+deviceTestStateStr+'&lastuploadtime='+lastuploadtime,
				dataType : "json",
				timeout : 3000,
				async : true,
				error : function(XMLHttpRequest, textStatus, errorThrown) {
					alert("action "+errorThrown);
					return false;
				},
				success : function(data) {
					if(data != null){
							Ext.getCmp('searchaera').setTitle("已注册终端数:  "+data.length+"个");
							var jsonarray = data;
							var ueindex;
							
							myData = [
					    ];
							
							for(ueindex=0; ueindex<jsonarray.length; ueindex++){
									var datastore = new Array();;
									datastore[0] = jsonarray[ueindex].model+"_"+jsonarray[ueindex].id;
									datastore[1] = ((Math.random() * 10000) >> 0) / 100;
					        datastore[2] = ((Math.random() * 10000) >> 0) / 100;
					        datastore[3] = ((Math.random() * 10000) >> 0) / 100;
					        datastore[4] = ((Math.random() * 10000) >> 0) / 100;
					        datastore[5] = ((Math.random() * 10000) >> 0) / 100;
					        myData.push(datastore);
									var nodeinstance = getNode(jsonarray[ueindex].id,jsonarray[ueindex].longtitude,jsonarray[ueindex].latitude,jsonarray[ueindex].device_state);
									nodeinstance.excu("<div>测试运行状态"+jsonarray[ueindex].device_state+"<br>上次更新时间："+jsonarray[ueindex].last_update+"</div>");
							}
							
							//markerClusterer =BMapLib.MarkerClusterer(map);
							if(markerClusterer){
									markerClusterer.clearMarkers();
							}
							var markers = [];
							for(var i=0; i<nodeArray.length; i++){
									markers.push(nodeArray[i].carMarker);
							}
							//alert(nodeArray.length);
							//markerClusterer.addMarkers({markers:markers}) ;
							markerClusterer = new BMapLib.MarkerClusterer(map, {markers:markers});
							
							//ds.loadData(myData);
							
					}
				}
		});
}

function doRefresh(){
		var lud = new Date();
		lastuploadtime = lud.format("yyyy/MM/dd hh:mm:ss");
		//alert(lastuploadtime);
		var deviceTypeStr = "all";
		var deviceMfrStr = "all";
		var deviceModelStr = "all";
		var deviceLocationStr = "all";
		var deviceStateStr = "all";
		var deviceTestStateStr = "all";
		if(Ext.getCmp('dt').getValue()!=null && Ext.getCmp('dt').getValue()!=''){
				deviceTypeStr = Ext.getCmp('dt').getValue();
		}
		if(Ext.getCmp('dmf').getValue()!=null && Ext.getCmp('dmf').getValue()!=''){
				deviceMfrStr = Ext.getCmp('dmf').getValue();
		}
		if(Ext.getCmp('dm').getValue()!=null && Ext.getCmp('dm').getValue()!=''){
				deviceModelStr = Ext.getCmp('dm').getValue();
		}
		if(Ext.getCmp('dl').getValue()!=null && Ext.getCmp('dl').getValue()!=''){
				deviceLocationStr = Ext.getCmp('dl').getValue();
		}
		if(Ext.getCmp('ds').getValue()!=null && Ext.getCmp('ds').getValue()!=''){
				deviceStateStr = Ext.getCmp('ds').getValue();
		}
		if(Ext.getCmp('dtt').getValue()!=null && Ext.getCmp('dtt').getValue()!=''){
				deviceTestStateStr = Ext.getCmp('dtt').getValue();
		}
		
    $.ajax( {
				url : '/refreshdevice.action',
				contentType : "application/json",
				data: 'deviceType='+deviceTypeStr+'&deviceMfr='+deviceMfrStr+'&deviceModel='+deviceModelStr+'&deviceLocation='+deviceLocationStr+'&deviceState='+deviceStateStr+'&deviceTestState='+deviceTestStateStr+'&lastuploadtime='+lastuploadtime,
				dataType : "json",
				timeout : 3000,
				async : true,
				error : function(XMLHttpRequest, textStatus, errorThrown) {
					alert("action "+errorThrown);
					return false;
				},
				success : function(data) {
					if(data != null){
							var jsonarray = data;
							var ueindex;
							
							myData = [
					    ];
							
							for(ueindex=0; ueindex<jsonarray.length; ueindex++){
									var datastore = new Array();;
									datastore[0] = jsonarray[ueindex].model+"_"+jsonarray[ueindex].id;
									datastore[1] = ((Math.random() * 10000) >> 0) / 100;
					        datastore[2] = ((Math.random() * 10000) >> 0) / 100;
					        datastore[3] = ((Math.random() * 10000) >> 0) / 100;
					        datastore[4] = ((Math.random() * 10000) >> 0) / 100;
					        datastore[5] = ((Math.random() * 10000) >> 0) / 100;
					        myData.push(datastore);
									var nodeinstance = getNode(jsonarray[ueindex].id,jsonarray[ueindex].longtitude,jsonarray[ueindex].latitude,jsonarray[ueindex].device_state);
									nodeinstance.excu("<div>测试运行状态"+jsonarray[ueindex].device_state+"<br>上次更新时间："+jsonarray[ueindex].last_update+"</div>");
							}
							
							//markerClusterer =BMapLib.MarkerClusterer(map);
							if(markerClusterer){
									markerClusterer.clearMarkers();
							}
							var markers = [];
							for(var i=0; i<nodeArray.length; i++){
									markers.push(nodeArray[i].carMarker);
							}
							//alert(nodeArray.length);
							//markerClusterer.addMarkers({markers:markers}) ;
							markerClusterer = new BMapLib.MarkerClusterer(map, {markers:markers});
							
							//ds.loadData(myData);
							
					}
				}
		});
}
