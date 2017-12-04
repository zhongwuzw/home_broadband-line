modulename = "request"

local _M = {}
local metatable = {__index = _M}

_M._VERSION = "1.0.0"


local function lua_string_split(str, split_char)
        local sub_str_tab = {};
        while (true) do
                local pos = string.find(str, split_char);
        if (not pos) then
                local size_t = table.getn(sub_str_tab)
                table.insert(sub_str_tab,size_t+1,str);
                break;
        end

        local sub_str = string.sub(str, 1, pos - 1);
        local size_t = table.getn(sub_str_tab)
        table.insert(sub_str_tab,size_t+1,sub_str);
        local t = string.len(str);
        str = string.sub(str, pos + 1, t);
        end
        return sub_str_tab;
end


local function Split(szFullString, szSeparator)
	local nFindStartIndex = 1
	local nSplitIndex = 1
	local nSplitArray = {}
	while true do
	   local nFindLastIndex = string.find(szFullString, szSeparator, nFindStartIndex)
	   if not nFindLastIndex then
	    nSplitArray[nSplitIndex] = string.sub(szFullString, nFindStartIndex, string.len(szFullString))
	    break
	   end
	   nSplitArray[nSplitIndex] = string.sub(szFullString, nFindStartIndex, nFindLastIndex - 1)
	   nFindStartIndex = nFindLastIndex + string.len(szSeparator)
	   nSplitIndex = nSplitIndex + 1
	end
	return nSplitArray
end

local function parseUrl (url)
	local t1 = nil
	--,
	t1= Split(url,',')

 	--?
	url = t1[1]
	t1=Split(t1[1],'?')

 	url=t1[2]
	--&

	t1=Split(t1[2],'&')
	local res = {}
	for k,v in pairs(t1) do
		i = 1
		t1 = Split(v,'=')
		res[t1[1]]={}
		res[t1[1]]=t1[2]
		i=i+1
	end
	return res
end

local function reverse(s)
	local r = "";
	for i = #s, 1, -1 do
		r = r .. string.sub(s, i, i);
	end
	return r;
end

local function lastPosOf(str,patten)
	local str1 = reverse(str);
	local pos2 = string.find(str1,patten);
	pos =  0 - pos2 + string.len(str1) + 1;
	return pos;
end

local function  str_decrypt(str)
	local recover1 = string.sub(str,1,(lastPosOf(str,"=")-1));
	local recover2 = string.sub(recover1,-3,-1)..string.sub(recover1,1,-4);
	local recover = ngx.decode_base64(recover2);
	return recover;
end

local function str_encrypt(str)
	local encrypt1 = ngx.encode_base64(str);
	local encrypt2 = string.sub(encrypt1,4)..string.sub(encrypt1,1,3).."=";
	return encrypt2;
end

_M.ngxlog = function(str)
	ngx.log(ngx.ERR, str);
end

local function writeLogFile(file_name,string)
    local f = assert(io.open(file_name, 'w'))
	f:write(string)
	f:close()
end




_M.getInterface = function(request_uri)
        local decoded_uri = str_decrypt(string.sub(request_uri,2));
        --ngx.say("decoded>>"..decoded_uri);
        local array = lua_string_split(decoded_uri,'?');
        
        local request_ver = string.sub(array[1],1,2);
        local request_interface = string.sub(array[1],3);
        local request_param = array[2];
        
        return request_ver, request_interface, request_param;
end

--
_M.getIplist = function(request_params)
	local pre_parsed_uri = "http://asd.com:123?"..request_params;
	local params = parseUrl(pre_parsed_uri);
	local ips = params.ip;
        return ips;
end


return _M
