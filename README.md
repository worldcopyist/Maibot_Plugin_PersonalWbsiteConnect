# MyAzure × MaiBot 个人网站连接器

将 MyAzure 网站已有的 `POST /api/azure/chat` 上游接口接入 MaiBot。插件只处理文本输入与文本输出，既不修改 MaiBot 主程序，也不访问 MaiBot 数据库。

## 链路

```text
浏览器 Azure 页面
  -> MyAzure Go 服务（登录、权限和请求长度校验）
  -> Docker 私网 HTTP /chat
  -> 本插件 MessageGateway
  -> MaiBot Host
  -> 本插件 MessageGateway
  -> /chat 返回 {"reply":"..."}，或 /chat/stream 返回 SSE 分段
```

MyAzure 会为每个登录用户提供稳定的 `conversation_id`，插件用它隔离等待中的请求和 MaiBot 会话。前端历史不会再次传给 MaiBot，避免与 MaiBot 自身上下文重复。

## 安全与边界

- 监听在 MaiBot 容器内部的 `0.0.0.0:18080`；不映射 Docker 主机端口、不配置 Nginx。
- 默认仅接受 Docker 网桥宿主机地址 `172.18.0.1`，其他来源得到 `403`。
- 只实现 `GET /health` 与 `POST /chat`；`/chat` 仅接受 JSON 文本。
- 不导入 `src.*`，不调用 `ctx.db`，不修改 MaiBot 本体、配置或数据库。
- MyAzure 端必须显式允许其管理员配置的私网 HTTP endpoint；该权限只应给这条 Docker 内网地址。

## 安装

把本仓库克隆到 MaiBot 挂载的独立插件目录：

```bash
git clone https://github.com/worldcopyist/Maibot_Plugin_PersonalWbsiteConnect.git \
  /root/maibot-deploy/MaiBot-1.2.3/data/MaiMBot/plugins/personal-website-connect
```

MaiBot Runner 会发现根目录的 `plugin.py` 与 `_manifest.json`。首次加载会生成运行时 `config.toml`；该文件被 `.gitignore` 忽略，不能提交。

本服务器版本要求在首次加载前提供 `[plugin].config_version`。复制
`config.example.toml` 为同目录 `config.toml`，并按实际 Docker 网桥地址调整；该
配置不包含密码或令牌。

MyAzure 服务器端需要将 Azure endpoint 配置为容器私网地址，例如：

```text
http://172.18.0.3:18080/chat
```

容器 IP 需要在部署时由 `docker inspect maim-bot-core` 读取，不能假定永远不变。

## 开发验证

```bash
python3 -m py_compile plugin.py
python3 -m unittest discover -s tests -v
```

部署后只读验证：

```bash
curl http://172.18.0.3:18080/health
```
