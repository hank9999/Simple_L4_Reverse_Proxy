# Simple_L4_Reverse_Proxy  
一个简单的4层TCP UDP反向代理器, 带有proxy protocol支持。  
A simple 4-layer TCP UDP reverse proxy with proxy protocol support.  

本项目原本只是为了解决MC服务端(Velocity/Geyser)中的proxy protocol转换, 后发现用途广泛, 遂上传至github。  
This project was originally intended to solve the proxy protocol conversion in the MC server (Velocity/Geyser), but later I found that it had a wide range of uses, so I uploaded it to GitHub.  

## 代码审核进度 | Code review progress && 代码完善进度 | Code improvement progress  
目前已经全部审查优化完毕。  
All reviews and optimizations have been completed.  

## 配置 | Config
查看 config.yaml.example 文件。  
See the config.yaml.example file.

## 参数 | Arguments
- `-c` | `--config` | 配置文件 | Config file.
   不传入参数时, 默认加载 config.yaml 文件。  
   If no argument is passed, the config.yaml file is loaded by default.

