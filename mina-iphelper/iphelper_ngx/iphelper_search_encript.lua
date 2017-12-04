--response

function new_split(str, delim, maxNb)   
   
    if string.find(str, delim) == nil then  
        return { str }  
    end  
    if maxNb == nil or maxNb < 1 then  
        maxNb = 0    -- No limit   
    end  
    local result = {}  
    local pat = "(.-)" .. delim .. "()"   
    local nb = 0  
    local lastPos   
    for part, pos in string.gfind(str, pat) do  
        nb = nb + 1  
        result[nb] = part   
        lastPos = pos   
        if nb == maxNb then break end  
    end  
  
    if nb ~= maxNb then  
        result[nb + 1] = string.sub(str, lastPos)   
    end  
    return result   
end

--single ip method
function getIP(ip)
	local json = require('json')
        local result
	local res = ngx.location.capture("/iphelper?ip="..ip)
	if res.status == 200 then  
             result = json.decode(res.body)  
        end 	 
	
	local retTbl = {}
	retTbl["isp"] = result["isp"]
	retTbl["detail"] = result["detail"];
	retTbl["city"] = result["city"];
	retTbl["province"] = result["province"];
	retTbl["country"] = result["country"];
	
	return json.encode(retTbl);
end

function getLocalIP(ip)
	local localip = getIP(ip)
	local json = require('json')
	local tbl = json.decode(localip)
	tbl["ip"] = ip
	return '"local":'..json.encode(tbl)
end

function getIPs(iplist)
	local IPsStr = ""
	local result = ""
	for i, value in pairs(iplist) do
              --ngx.log(ngx.ERR, "value>>"..value)
              --ngx.log(ngx.ERR, "result>>>>>>"..result)
	      --IPsStr = IPsStr..',"'..value..'":'..getIP(value)
	      IPsStr = IPsStr..'"'..value..'":'..getIP(value)
	end
	return IPsStr
end

function str_encrypt(str)
    local encrypt1 = ngx.encode_base64(str);
    local encrypt2 = string.sub(encrypt1,4)..string.sub(encrypt1,1,3).."=";
    return encrypt2;
end

local request = require("otsencrypt.request_ip");
--local remote_addr = ngx.var.remote_addr
local remote_addr = ngx.req.get_headers()["x_forwarded_for"]
--ngx.log(ngx.ERR, "x_forwarded_for"..remote_addr)
local request_version, request_interface, request_params  = request.getInterface(ngx.var.request_uri);
if request_version == 'v3' then
	local ips  = request.getIplist(request_params);
	local iplist = new_split(ips,'$')
	--ngx.log(ngx.ERR, "ips>>>"..ips);
	table.remove(iplist,1)
	--ngx.log(ngx.ERR, "ddddd   iplist[1]>>"..iplist[1])
	local resIPs = getIPs(iplist);
	local resLocal = getLocalIP(remote_addr);
	local response = "{"..resLocal..resIPs.."}"
	--local response = "{"..resIPs.."}"
	ngx.say(str_encrypt(response))
	--ngx.say(response)
else
	ngx.say(str_encrypt("Not Supported"));
end
