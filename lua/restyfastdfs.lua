-- Copyright (C) 2012 Azure Wang
-- @link: https://github.com/azurewang/Nginx_Lua-FastDFS

local string = string
local table  = table
local bit    = bit
local ngx    = ngx
local tonumber = tonumber
local setmetatable = setmetatable
local error = error

module(...)

local VERSION = '0.1'

local FDFS_PROTO_PKG_LEN_SIZE = 8
local TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITHOUT_GROUP_ONE = 101
local TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITH_GROUP_ONE = 104
local TRACKER_PROTO_CMD_SERVICE_QUERY_UPDATE = 103
local TRACKER_PROTO_CMD_SERVICE_QUERY_FETCH_ONE = 102
local STORAGE_PROTO_CMD_UPLOAD_FILE = 11
local STORAGE_PROTO_CMD_DELETE_FILE = 12
local STORAGE_PROTO_CMD_DOWNLOAD_FILE = 14
local STORAGE_PROTO_CMD_UPLOAD_SLAVE_FILE = 21
local STORAGE_PROTO_CMD_QUERY_FILE_INFO = 22
local STORAGE_PROTO_CMD_UPLOAD_APPENDER_FILE = 23
local STORAGE_PROTO_CMD_APPEND_FILE = 24
local FDFS_FILE_EXT_NAME_MAX_LEN = 6
local FDFS_PROTO_CMD_QUIT = 82
local TRACKER_PROTO_CMD_RESP = 100

local mt = { __index = _M }

function new(self)
    return setmetatable({}, mt)
end

function set_tracker(self, host, port)
    local tracker = {host = host, port = port}
    self.tracker = tracker
end

function set_timeout(self, timeout)
    if timeout then
        self.timeout = timeout
    end
end

function set_tracker_keepalive(self, timeout, size)
    local keepalive = {timeout = timeout, size = size}
    self.tracker_keepalive = keepalive
end

function set_storage_keepalive(self, timeout, size)
    local keepalive = {timeout = timeout, size = size}
    self.storage_keepalive = keepalive
end

function int2buf(n)
    -- only trans 32bit  full is 64bit
    return string.rep("\00", 4) .. string.char(bit.band(bit.rshift(n, 24), 0xff), bit.band(bit.rshift(n, 16), 0xff), bit.band(bit.rshift(n, 8), 0xff), bit.band(n, 0xff))
end

function buf2int(buf)
    -- only trans 32bit  full is 64bit
    local c1, c2, c3, c4 = string.byte(buf, 5, 8)
    return bit.bor(bit.lshift(c1, 24), bit.lshift(c2, 16),bit.lshift(c3, 8), c4)
end

function read_fdfs_header(sock)
    local header = {}
    local buf, err = sock:receive(10)
    if not buf then
        ngx.log(ngx.ERR, "fdfs: read header error")
        sock:close()
        ngx.exit(500)
    end
    header.len = buf2int(string.sub(buf, 1, 8))
    header.cmd = string.byte(buf, 9)
    header.status = string.byte(buf, 10)
    return header
end

function fix_string(str, fix_length)
    local len = string.len(str)
    if len > fix_length then
        len = fix_length
    end
    local fix_str = string.sub(str, 1, len)
    if len < fix_length then
        fix_str = fix_str .. string.rep("\00", fix_length - len )
    end
    return fix_str
end

function strip_string(str)
    local pos = string.find(str, "\00")
    if pos then
        return string.sub(str, 1, pos - 1)
    else
        return str
    end
end

function get_ext_name(filename)
    local extname = filename:match("%.(%w+)$")
    if extname then
        return fix_string(extname, FDFS_FILE_EXT_NAME_MAX_LEN)
    else
        return nil
    end
end

function read_tracket_result(sock, header)
    if header.len > 0 then
        local res = {}
        local buf = sock:receive(header.len)
        res.group_name = strip_string(string.sub(buf, 1, 16))
        res.host       = strip_string(string.sub(buf, 17, 31)) 
        res.port       = buf2int(string.sub(buf, 32, 39))
        res.store_path_index = string.byte(string.sub(buf, 40, 40))
        return res
    else
        return nil
    end
end

function read_storage_result(sock, header)
    if header.len > 0 then
        local res = {}
        local buf = sock:receive(header.len)
        res.group_name = strip_string(string.sub(buf, 1, 16))
        res.file_name  = strip_string(string.sub(buf, 17, header.len))
        return res
    else
        return nil
    end
end

function query_upload_storage(self, group_name)
    local tracker = self.tracker
    if not tracker then
        return nil
    end
    local out = {}
    if group_name then
        -- query upload with group_name
        -- package length
        table.insert(out, int2buf(16))
        -- cmd
        table.insert(out, string.char(TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITH_GROUP_ONE))
        -- status
        table.insert(out, "\00")
        -- group name
        table.insert(out, fix_string(group_name, 16))
    else
        -- query upload without group_name
        -- package length
        table.insert(out,  string.rep("\00", FDFS_PROTO_PKG_LEN_SIZE))
        -- cmd
        table.insert(out, string.char(TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITHOUT_GROUP_ONE))
        -- status
        table.insert(out, "\00")
    end
    -- init socket
    local sock, err = ngx.socket.tcp()
    if not sock then
        return nil, err
    end
    if self.timeout then
        sock:settimeout(self.timeout)
    end
    -- connect tracker
    local ok, err = sock:connect(tracker.host, tracker.port)
    if not ok then
        return nil, err
    end
    -- send request
    local bytes, err = sock:send(out)
    -- read request header
    local hdr = read_fdfs_header(sock)
    -- read body
    local res = read_tracket_result(sock, hdr)
    -- keepalive
    local keepalive = self.tracker_keepalive
    if keepalive then
        sock:setkeepalive(keepalive.timeout, keepalive.size)
    end
    return res
end

function do_upload_appender(self, ext_name)
    local storage = self:query_upload_storage()
    if not storage then
        return nil
    end
    -- ext_name
    if ext_name then
        ext_name = fix_string(ext_name, FDFS_FILE_EXT_NAME_MAX_LEN)
    end
    -- get file size
    local file_size = tonumber(ngx.var.content_length)
    if not file_size or file_size <= 0 then
        return nil
    end
    local sock, err = ngx.socket.tcp()
    if not sock then
        return nil, err
    end
    if self.timeout then
        sock:settimeout(self.timeout)
    end
    local ok, err = sock:connect(storage.host, storage.port)
    if not ok then
        return nil, err
    end
    -- send header
    local out = {}
    table.insert(out, int2buf(file_size + 15))
    table.insert(out, string.char(STORAGE_PROTO_CMD_UPLOAD_APPENDER_FILE))
    -- status
    table.insert(out, "\00")
    -- store_path_index
    table.insert(out, string.char(storage.store_path_index))
    -- filesize
    table.insert(out, int2buf(file_size))
    -- exitname
    table.insert(out, ext_name)
    local bytes, err = sock:send(out)
    -- send file data
    local send_count = 0
    local req_sock, err = ngx.req.socket()
    if not req_sock then
        ngx.log(ngx.ERR, err)
        ngx.exit(500)
    end
        while true do
        local chunk, _, part = req_sock:receive(1024 * 32)
        if not part then
            local bytes, err = sock:send(chunk)
            if not bytes then
                ngx.log(ngx.ngx.ERR, "fdfs: send body error")
                sock:close()
                ngx.exit(500)
            end
            send_count = send_count + bytes
        else
            -- part have data, not read full end
            local bytes, err = sock:send(part)
            if not bytes then
                ngx.log(ngx.ngx.ERR, "fdfs: send body error")
                sock:close()
                ngx.exit(500)
            end
            send_count = send_count + bytes
            break
        end
    end
    if send_count ~= file_size then
        -- send file not full
        ngx.log(ngx.ngx.ERR, "fdfs: read file body not full")
        sock:close()
        ngx.exit(500)
    end
    -- read response
    local res_hdr = read_fdfs_header(sock)
    local res = read_storage_result(sock, res_hdr)
    local keepalive = self.storage_keepalive
    if keepalive then
        sock:setkeepalive(keepalive.timeout, keepalive.size)
    end
    return res
end

function do_upload(self, ext_name)
    local storage = self:query_upload_storage()
    if not storage then
        return nil
    end
    -- ext_name
    if ext_name then
        ext_name = fix_string(ext_name, FDFS_FILE_EXT_NAME_MAX_LEN)
    end
    -- get file size
    local file_size = tonumber(ngx.var.content_length)
    if not file_size or file_size <= 0 then
        return nil
    end
    local sock, err = ngx.socket.tcp()
    if not sock then
        return nil, err
    end
    if self.timeout then
        sock:settimeout(self.timeout)
    end
    local ok, err = sock:connect(storage.host, storage.port)
    if not ok then
        return nil, err
    end
    -- send header
    local out = {}
    table.insert(out, int2buf(file_size + 15))
    table.insert(out, string.char(STORAGE_PROTO_CMD_UPLOAD_FILE))
    -- status
    table.insert(out, "\00")
    -- store_path_index
    table.insert(out, string.char(storage.store_path_index))
    -- filesize
    table.insert(out, int2buf(file_size))
    -- exitname
    table.insert(out, ext_name)
    local bytes, err = sock:send(out)
    -- send file data
    local send_count = 0
    local req_sock, err = ngx.req.socket()
    if not req_sock then
        ngx.log(ngx.ERR, err)
        ngx.exit(500)
    end
    while true do
        local chunk, _, part = req_sock:receive(1024 * 32)
        if not part then
            local bytes, err = sock:send(chunk)
            if not bytes then
                ngx.log(ngx.ngx.ERR, "fdfs: send body error")
                sock:close()
                ngx.exit(500)
            end
            send_count = send_count + bytes
        else
            -- part have data, not read full end
            local bytes, err = sock:send(part)
            if not bytes then
                ngx.log(ngx.ngx.ERR, "fdfs: send body error")
                sock:close()
                ngx.exit(500)
            end
            send_count = send_count + bytes
            break
        end
    end
    if send_count ~= file_size then
        -- send file not full
        ngx.log(ngx.ngx.ERR, "fdfs: read file body not full")
        sock:close()
        ngx.exit(500)
    end
    -- read response
    local res_hdr = read_fdfs_header(sock)
    local res = read_storage_result(sock, res_hdr)
    local keepalive = self.storage_keepalive
    if keepalive then
        sock:setkeepalive(keepalive.timeout, keepalive.size)
    end
    return res
end

function query_update_storage_ex(self, group_name, file_name)
    local out = {}
    -- package length
    table.insert(out, int2buf(16 + string.len(file_name)))
    -- cmd
    table.insert(out, string.char(TRACKER_PROTO_CMD_SERVICE_QUERY_UPDATE))
    -- status
    table.insert(out, "\00")
    -- group_name
    table.insert(out, fix_string(group_name, 16))
    -- file name
    table.insert(out, file_name)
    -- get tracker
    local tracker = self.tracker
    if not tracker then
        return nil
    end
    -- init socket
    local sock, err = ngx.socket.tcp()
    if not sock then
        return nil, err
    end
    if self.timeout then
        sock:settimeout(self.timeout)
    end
    -- connect tracker
    local ok, err = sock:connect(tracker.host, tracker.port)
    if not ok then
        return nil, err
    end
    -- send request
    local bytes, err = sock:send(out)
    -- read request header
    local hdr = read_fdfs_header(sock)
    -- read body
    local res = read_tracket_result(sock, hdr)
    -- keepalive
    local keepalive = self.tracker_keepalive
    if keepalive then
        sock:setkeepalive(keepalive.timeout, keepalive.size)
    end
    return res
end

function query_update_storage(self, fileid)
    local pos = fileid:find('/')
    if not pos then
        return nil
    else
        local group_name = fileid:sub(1, pos-1)
        local file_name  = fileid:sub(pos + 1)
        local res = self:query_update_storage_ex(group_name, file_name)
        if res then
            res.file_name = file_name
        end
        return res
    end
end

function do_delete(self, fileid)
    local storage = self:query_update_storage(fileid)
    if not storage then
        return nil
    end
    local out = {}
    table.insert(out, int2buf(16 + string.len(storage.file_name)))
    table.insert(out, string.char(STORAGE_PROTO_CMD_DELETE_FILE))
    table.insert(out, "\00")
    -- group name
    table.insert(out, fix_string(storage.group_name, 16))
    -- file name
    table.insert(out, storage.file_name)
    -- init socket
    local sock, err = ngx.socket.tcp()
    if not sock then
        return nil, err
    end
    sock:settimeout(self.timeout)
    local ok, err = sock:connect(storage.host, storage.port)
    if not ok then
        return nil, err
    end
    local bytes, err = sock:send(out)
    if not bytes then
        ngx.log(ngx.ngx.ERR, "fdfs: send body error")
        sock:close()
        ngx.exit(500)
    end
    -- read request header
    local hdr = read_fdfs_header(sock)
    local keepalive = self.storage_keepalive
    if keepalive then
        sock:setkeepalive(keepalive.timeout, keepalive.size)
    end
    return hdr
end

function query_download_storage(self, fileid)
    local pos = fileid:find('/')
    if not pos then
        return nil
    else
        local group_name = fileid:sub(1, pos-1)
        local file_name  = fileid:sub(pos + 1)
        local res = self:query_download_storage_ex(group_name, file_name)
        res.file_name = file_name
        return res
    end
end

function query_download_storage_ex(self, group_name, file_name)
    local out = {}
    -- package length
    table.insert(out, int2buf(16 + string.len(file_name)))
    -- cmd
    table.insert(out, string.char(TRACKER_PROTO_CMD_SERVICE_QUERY_FETCH_ONE))
    -- status
    table.insert(out, "\00")
    -- group_name
    table.insert(out, fix_string(group_name, 16))
    -- file name
    table.insert(out, file_name)
    -- get tracker
    local tracker = self.tracker
    if not tracker then
        return nil
    end
    -- init socket
    local sock, err = ngx.socket.tcp()
    if not sock then
        return nil, err
    end
    if self.timeout then
        sock:settimeout(self.timeout)
    end
    -- connect tracker
    local ok, err = sock:connect(tracker.host, tracker.port)
    if not ok then
        return nil, err
    end
    -- send request
    local bytes, err = sock:send(out)
    -- read request header
    local hdr = read_fdfs_header(sock)
    -- read body
    local res = read_tracket_result(sock, hdr)
    -- keepalive
    local keepalive = self.tracker_keepalive
    if keepalive then
        sock:setkeepalive(keepalive.timeout, keepalive.size)
    end
    return res
end

function do_download(self, fileid)
    local storage = self:query_download_storage(fileid)
    if not storage then
        return nil
    end
    local out = {}
    -- file_offset(8)  download_bytes(8)  group_name(16)  file_name(n)
    table.insert(out, int2buf(32 + string.len(storage.file_name)))
    table.insert(out, string.char(STORAGE_PROTO_CMD_DOWNLOAD_FILE))
    table.insert(out, "\00")
    -- file_offset  download_bytes  8 + 8
    table.insert(out, string.rep("\00", 16))
    -- group name
    table.insert(out, fix_string(storage.group_name, 16))
    -- file name
    table.insert(out, storage.file_name)
    -- init socket
    local sock, err = ngx.socket.tcp()
    if not sock then
        return nil, err
    end
    sock:settimeout(self.timeout)
    local ok, err = sock:connect(storage.host, storage.port)
    if not ok then
        return nil, err
    end
    local bytes, err = sock:send(out)
    if not bytes then
        ngx.log(ngx.ERR, "fdfs: send request error" .. err)
        sock:close()
        ngx.exit(500)
    end
    -- read request header
    local hdr = read_fdfs_header(sock)
    -- read request bodya
    local data, partial
    if hdr.len > 0 then
        data, err, partial = sock:receive(hdr.len)
        if not data then
            ngx.log(ngx.ERR, "read file body error:" .. err)
            sock:close()
            ngx.exit(500)
        end
    end
    local keepalive = self.storage_keepalive
    if keepalive then
        sock:setkeepalive(keepalive.timeout, keepalive.size)
    end
    return data
end

function do_append(self, fileid)
    local storage = self:query_update_storage(fileid)
    if not storage then
        return nil
    end
    local file_name = storage.file_name
    local file_name_len = string.len(file_name)
    -- get file size
    local file_size = tonumber(ngx.var.content_length)
    if not file_size or file_size <= 0 then
        return nil
    end
    local sock, err = ngx.socket.tcp()
    if not sock then
        return nil, err
    end
    if self.timeout then
        sock:settimeout(self.timeout)
    end
    local ok, err = sock:connect(storage.host, storage.port)
    if not ok then
        return nil, err
    end
    -- send request
    local out = {}
    table.insert(out, int2buf(file_size + file_name_len + 16))
    table.insert(out, string.char(STORAGE_PROTO_CMD_APPEND_FILE))
    -- status
    table.insert(out, "\00")
    table.insert(out, int2buf(file_name_len))
    table.insert(out, int2buf(file_size))
    table.insert(out, file_name)
    local bytes, err = sock:send(out)
    -- send file data
    local send_count = 0
    local req_sock, err = ngx.req.socket()
    if not req_sock then
        ngx.log(ngx.ERR, err)
        ngx.exit(500)
    end
    while true do
        local chunk, _, part = req_sock:receive(1024 * 32)
        if not part then
            local bytes, err = sock:send(chunk)
            if not bytes then
                ngx.log(ngx.ngx.ERR, "fdfs: send body error")
                sock:close()
                ngx.exit(500)
            end
            send_count = send_count + bytes
        else
            -- part have data, not read full end
            local bytes, err = sock:send(part)
            if not bytes then
                ngx.log(ngx.ngx.ERR, "fdfs: send body error")
                sock:close()
                ngx.exit(500)
            end
            send_count = send_count + bytes
            break
        end
    end
    if send_count ~= file_size then
        -- send file not full
        ngx.log(ngx.ngx.ERR, "fdfs: read file body not full")
        sock:close()
        ngx.exit(500)
    end
    -- read response
    local res_hdr = read_fdfs_header(sock)
    local res = read_storage_result(sock, res_hdr)
    local keepalive = self.storage_keepalive
    if keepalive then
        sock:setkeepalive(keepalive.timeout, keepalive.size)
    end
    return res_hdr
end

-- _M.query_upload_storage = query_upload_storage
-- _M.do_upload_storage    = do_upload_storage
-- _M.do_delete_storage    = do_delete_storage

local class_mt = {
    -- to prevent use of casual module global variables
    __newindex = function (table, key, val)
        error('attempt to write to undeclared variable "' .. key .. '"')
    end
}

setmetatable(_M, class_mt)
