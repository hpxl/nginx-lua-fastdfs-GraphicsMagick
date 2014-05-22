nginx-lua-fastdfs-GraphicsMagick
==================
利用nginx lua 获取fastdfs的原图，存放原图到本地，根据不同规则url，例如：_60x60.jpg、_80x80.jpg，利用gm生成缩略图存放到本地。
第二次访问直接返回本地已生成的缩略图。
定时凌晨清除一段时间内未访问的图片，节省空间。

图片访问举例
----------------
1. [http://192.168.1.113/group1/M00/00/00/wKgBcVN0wDiAILQXAAdtg6qArdU189.jpg](http://192.168.1.113/group1/M00/00/00/wKgBcVN0wDiAILQXAAdtg6qArdU189.jpg)
2. [http://192.168.1.113/group1/M00/00/00/wKgBcVN0wDiAILQXAAdtg6qArdU189.jpg_80x80.jpg](http://192.168.1.113/group1/M00/00/00/wKgBcVN0wDiAILQXAAdtg6qArdU189.jpg_80x80.jpg)
3. [http://gi1.md.alicdn.com/imgextra/i1/401612253/T2ASPfXE4XXXXXXXXX_!!401612253.jpg_60x60.jpg](http://gi1.md.alicdn.com/imgextra/i1/401612253/T2ASPfXE4XXXXXXXXX_!!401612253.jpg)


参考网址
----------------
1. [https://github.com/openresty/lua-nginx-module](https://github.com/openresty/lua-nginx-module)
2. [https://github.com/azurewang/Nginx_Lua-FastDFS](https://github.com/azurewang/Nginx_Lua-FastDFS)
3. [https://github.com/azurewang/lua-resty-fastdfs](https://github.com/azurewang/lua-resty-fastdfs)
4. [http://rhomobi.com/topics/23](http://rhomobi.com/topics/23)
5. [http://bbs.chinaunix.net/thread-4133106-1-1.html](http://bbs.chinaunix.net/thread-4133106-1-1.html)
