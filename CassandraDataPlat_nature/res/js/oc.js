
function start_ue() {
	$.ajax( {
		type: 'PUT',
		url : 'http://192.168.64.96:18282/node/ports/node00',
		contentType : "application/json",
		data : '{"cmd":"load"}',
		dataType : "jsonp",
		timeout : 3000,
		async : true,
		error : function(XMLHttpRequest, textStatus, errorThrown) {
			// alert("load "+errorThrown);
			return false;
		},
		success : function(resp2) {
			alert("load succss!");
			$.ajax( {
					type: 'PUT',
					url : 'http://192.168.64.96:18282/node/ports/node00',
					contentType : "application/json",
					data : '{"cmd":"run"}',
					dataType : "jsonp",
					timeout : 3000,
					async : true,
					error : function(XMLHttpRequest, textStatus, errorThrown) {
						return false;
					},
					success : function(resp3) {
						alert("run succss!");
					}
			});
		}
	});
}



function stop_ue() {
	$.ajax( {
		type: 'PUT',
		url : 'http://192.168.64.96:18282/node/ports/node00',
		contentType : "application/json",
		data : '{"cmd":"stop"}',
		dataType : "jsonp",
		timeout : 3000,
		async : true,
		error : function(XMLHttpRequest, textStatus, errorThrown) {
			// alert("load "+errorThrown);
			return false;
		},
		success : function(resp2) {
			alert("load succss!");
		}
	});
}