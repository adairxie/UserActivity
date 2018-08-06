* 安全事件关联的目标是根据被攻击的服务器IP回溯出风险设备和攻击设备，进而调整其信誉值
* 遭遇DDos攻击的server信息上传接口   

```
POST /api/securityevents/report   
[
    {
        "ip": "192.168.3.41",
        "port": 8080,
        "attack_time": "2018-08-02 00:07:50"
    },
    {
        "ip": "192.168.3.42",
        "port": 8080,
        "attack_time": "2018-08-02 00:07:50"
    }
]

HTTP/1.1 200 OK
{
    "status": 200,              # http状态码
    "message": "success"        # 描述信息
}
```

测试服务器：
address:172.16.100.111  port:8083