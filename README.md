nginx-lua-fastdfs-GraphicsMagick
==================
fastdfs开源的分布式文件系统，此脚本利用nginx lua模块，动态生成图片缩略图，fastdfs只存一份原图。lua通过socket获取fastdfs的原图，并存放到本地，根据不同规则url，例如：_60x60.jpg、_80x80.jpg，类似淘宝图片url规则。利用gm命令生成本地缩略图，第二次访问直接返回本地图片。定时任务凌晨清除7天内未访问的图片，节省空间。

图片访问举例
----------------
1. [http://192.168.1.113/group1/M00/00/00/wKgBcVN0wDiAILQXAAdtg6qArdU189.jpg](http://192.168.1.113/group1/M00/00/00/wKgBcVN0wDiAILQXAAdtg6qArdU189.jpg)
2. [http://192.168.1.113/group1/M00/00/00/wKgBcVN0wDiAILQXAAdtg6qArdU189.jpg_80x80.jpg](http://192.168.1.113/group1/M00/00/00/wKgBcVN0wDiAILQXAAdtg6qArdU189.jpg_80x80.jpg)
3. [http://gi1.md.alicdn.com/imgextra/i1/401612253/T2ASPfXE4XXXXXXXXX_!!401612253.jpg_60x60.jpg](http://gi1.md.alicdn.com/imgextra/i1/401612253/T2ASPfXE4XXXXXXXXX_!!401612253.jpg_60x60.jpg)
4. [http://gi1.md.alicdn.com/imgextra/i1/401612253/T2ASPfXE4XXXXXXXXX_!!401612253.jpg_80x80.jpg](http://gi1.md.alicdn.com/imgextra/i1/401612253/T2ASPfXE4XXXXXXXXX_!!401612253.jpg_80x80.jpg)


参考网址
----------------
1. [https://github.com/openresty/lua-nginx-module](https://github.com/openresty/lua-nginx-module)
2. [https://github.com/azurewang/Nginx_Lua-FastDFS](https://github.com/azurewang/Nginx_Lua-FastDFS)
3. [https://github.com/azurewang/lua-resty-fastdfs](https://github.com/azurewang/lua-resty-fastdfs)
4. [http://rhomobi.com/topics/23](http://rhomobi.com/topics/23)
5. [http://bbs.chinaunix.net/thread-4133106-1-1.html](http://bbs.chinaunix.net/thread-4133106-1-1.html)
