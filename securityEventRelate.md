* 安全事件关联的目标是根据被攻击的节点IP回溯出风险设备和攻击设备，进而调整其信誉
* 遭遇DDos和CC攻击的节点IP上传接口   

```
POST /api/securityEvents/report   
[
    "1.1.1.1",
    "2.2.2.2",
    "3.3.3.3",
    "4.4.4.4",
    "5.5.5.5",
    "6.6.6.6"
]

HTTP/1.1 200 OK
{
    "success": true,    # 错误码， true:成功， false: 失败
    "message": "OK"     # 错误描述
}   

```