"""MyAzure 到 MaiBot 的最小文本消息网关。

该插件在 MaiBot 的插件 Runner 进程中运行。它仅提供一个内部 HTTP/JSON
入口，将输入文本交给 Host，并把同一会话的文本回复同步返回给调用方。
它不读取或写入 MaiBot 数据库，也不导入 MaiBot 主程序的 ``src`` 模块。
"""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Mapping
from typing import Any, ClassVar
from uuid import uuid4

from maibot_sdk import Field, MaiBotPlugin, MessageGateway, PluginConfigBase


GATEWAY_NAME = "personal_website_gateway"
PLATFORM = "personal_website"


class PluginSettings(PluginConfigBase):
    """Runner 需要的插件级元数据。"""

    config_version: str = Field(default="1.0.0", description="插件配置结构版本。")


class GatewaySettings(PluginConfigBase):
    """仅限服务器内部网络的网关配置。"""

    bind_host: str = Field(default="0.0.0.0", description="容器内监听地址。")
    port: int = Field(default=18080, ge=1024, le=65535, description="容器内 HTTP 端口。")
    allowed_client_ips: list[str] = Field(
        default_factory=lambda: ["172.18.0.1"],
        description="允许调用 /chat 的 Docker 网桥来源 IP。",
    )
    request_timeout_seconds: int = Field(default=45, ge=3, le=55, description="等待 MaiBot 文本回复的最长秒数。")


class PersonalWebsiteSettings(PluginConfigBase):
    """个人网站连接器完整配置。"""

    plugin: PluginSettings = Field(default_factory=PluginSettings)
    gateway: GatewaySettings = Field(default_factory=GatewaySettings)


class PersonalWebsiteGatewayPlugin(MaiBotPlugin):
    """把 MyAzure 的同步 HTTP 请求桥接到 MaiBot 消息网关。"""

    config_model: ClassVar[type[PluginConfigBase] | None] = PersonalWebsiteSettings

    def __init__(self) -> None:
        super().__init__()
        self._server: asyncio.AbstractServer | None = None
        self._pending: dict[str, asyncio.Future[str]] = {}
        self._pending_lock = asyncio.Lock()

    async def on_load(self) -> None:
        settings = self._settings()
        self._server = await asyncio.start_server(self._handle_client, settings.gateway.bind_host, settings.gateway.port)
        await self.ctx.gateway.update_state(
            GATEWAY_NAME,
            ready=True,
            platform=PLATFORM,
            account_id="myazure",
            scope="internal",
            metadata={"protocol": "http-json", "port": settings.gateway.port},
        )
        self.ctx.logger.info("个人网站连接器已监听 %s:%s", settings.gateway.bind_host, settings.gateway.port)

    async def on_unload(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
        async with self._pending_lock:
            for future in self._pending.values():
                if not future.done():
                    future.set_exception(RuntimeError("个人网站连接器已停止"))
            self._pending.clear()
        await self.ctx.gateway.update_state(GATEWAY_NAME, ready=False, platform=PLATFORM, scope="internal")

    async def on_config_update(self, scope: str, config_data: dict[str, object], version: str) -> None:
        del version
        if scope != "self":
            return
        self.set_plugin_config(config_data)
        self.ctx.logger.warning("个人网站连接器配置已更新；端口和监听地址将在插件重载后生效")

    @MessageGateway(
        name=GATEWAY_NAME,
        route_type="duplex",
        platform=PLATFORM,
        protocol="http-json",
        account_id="myazure",
        scope="internal",
        description="MyAzure 内部 HTTP/JSON 文本聊天网关",
    )
    async def deliver_to_website(
        self,
        message: dict[str, Any],
        route: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """接收 Host 出站文本，并唤醒相同网站会话的 HTTP 请求。"""
        del metadata
        del kwargs
        conversation_id = self._conversation_from_outbound(message, route or {})
        if not conversation_id:
            return {"success": False, "error": "出站消息缺少网站会话标识"}
        reply = self._extract_text(message)
        if not reply:
            return {"success": False, "error": "出站消息不包含文本"}
        async with self._pending_lock:
            future = self._pending.get(conversation_id)
            if future is None or future.done():
                return {"success": False, "error": "没有等待该网站会话的请求"}
            future.set_result(reply)
        return {"success": True, "external_message_id": f"website-reply-{uuid4().hex}"}

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            if not self._is_allowed_client(writer):
                await self._write_json(writer, 403, {"error": "仅允许服务器内部请求"})
                return
            method, path, headers = await self._read_request_head(reader)
            if method == "GET" and path == "/health":
                await self._write_json(writer, 200, {"ok": True, "gateway": GATEWAY_NAME})
                return
            if method != "POST" or path != "/chat":
                await self._write_json(writer, 404, {"error": "未找到接口"})
                return
            payload = await self._read_json_body(reader, headers)
            message = str(payload.get("message") or "").strip()
            if not message:
                await self._write_json(writer, 400, {"error": "message 不能为空"})
                return
            if len(message) > 4000:
                await self._write_json(writer, 400, {"error": "message 不能超过 4000 个字符"})
                return
            conversation_id = str(payload.get("conversation_id") or "").strip()
            if not conversation_id:
                await self._write_json(writer, 400, {"error": "conversation_id 不能为空"})
                return
            reply = await self._route_and_wait(conversation_id, message)
            await self._write_json(writer, 200, {"reply": reply})
        except asyncio.TimeoutError:
            await self._write_json(writer, 504, {"error": "等待 MaiBot 回复超时"})
        except ValueError as exc:
            await self._write_json(writer, 400, {"error": str(exc)})
        except Exception:
            self.ctx.logger.exception("个人网站连接器处理请求失败")
            await self._write_json(writer, 502, {"error": "MaiBot 网关处理失败"})
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except ConnectionError:
                pass

    async def _route_and_wait(self, conversation_id: str, text: str) -> str:
        loop = asyncio.get_running_loop()
        future: asyncio.Future[str] = loop.create_future()
        async with self._pending_lock:
            if conversation_id in self._pending:
                raise ValueError("该会话已有进行中的请求")
            self._pending[conversation_id] = future
        message_id = f"website-{uuid4().hex}"
        try:
            accepted = await self.ctx.gateway.route_message(
                gateway_name=GATEWAY_NAME,
                message=self._build_inbound_message(message_id, conversation_id, text),
                route_metadata={
                    "platform": PLATFORM,
                    "account_id": "myazure",
                    "scope": "internal",
                    "target_user_id": conversation_id,
                },
                external_message_id=message_id,
                dedupe_key=message_id,
            )
            if not accepted:
                raise RuntimeError("MaiBot 未接受网站消息")
            return await asyncio.wait_for(future, timeout=self._settings().gateway.request_timeout_seconds)
        finally:
            async with self._pending_lock:
                self._pending.pop(conversation_id, None)

    @staticmethod
    def _build_inbound_message(message_id: str, conversation_id: str, text: str) -> dict[str, Any]:
        return {
            "message_id": message_id,
            "timestamp": str(time.time()),
            "platform": PLATFORM,
            "message_info": {
                "user_info": {
                    "user_id": conversation_id,
                    "user_nickname": "MyAzure 用户",
                    "user_cardname": None,
                },
                "additional_config": {
                    "platform_io_target_user_id": conversation_id,
                    "website_conversation_id": conversation_id,
                },
            },
            "raw_message": [{"type": "text", "data": {"text": text}}],
            "is_mentioned": True,
            "is_at": True,
            "is_emoji": False,
            "is_picture": False,
            "is_command": text.startswith("/"),
            "is_notify": False,
            "session_id": "",
            "processed_plain_text": text,
            "display_message": text,
        }

    def _settings(self) -> PersonalWebsiteSettings:
        config = self.config
        if not isinstance(config, PersonalWebsiteSettings):
            raise RuntimeError("个人网站连接器配置不可用")
        return config

    def _is_allowed_client(self, writer: asyncio.StreamWriter) -> bool:
        peer = writer.get_extra_info("peername")
        host = str(peer[0]) if isinstance(peer, tuple) and peer else ""
        return host in set(self._settings().gateway.allowed_client_ips)

    @staticmethod
    async def _read_request_head(reader: asyncio.StreamReader) -> tuple[str, str, dict[str, str]]:
        raw = await reader.readuntil(b"\r\n\r\n")
        if len(raw) > 16 * 1024:
            raise ValueError("请求头过大")
        lines = raw.decode("iso-8859-1").split("\r\n")
        parts = lines[0].split(" ")
        if len(parts) != 3:
            raise ValueError("HTTP 请求行无效")
        headers: dict[str, str] = {}
        for line in lines[1:]:
            if not line or ":" not in line:
                continue
            key, value = line.split(":", 1)
            headers[key.lower()] = value.strip()
        return parts[0].upper(), parts[1].split("?", 1)[0], headers

    @staticmethod
    async def _read_json_body(reader: asyncio.StreamReader, headers: Mapping[str, str]) -> dict[str, Any]:
        try:
            length = int(headers.get("content-length", ""))
        except ValueError as exc:
            raise ValueError("Content-Length 无效") from exc
        if length < 1 or length > 32 * 1024:
            raise ValueError("请求体大小无效")
        raw = await reader.readexactly(length)
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValueError("请求体必须是 JSON") from exc
        if not isinstance(payload, dict):
            raise ValueError("请求体必须是 JSON 对象")
        return payload

    @staticmethod
    async def _write_json(writer: asyncio.StreamWriter, status: int, payload: Mapping[str, Any]) -> None:
        raw = json.dumps(dict(payload), ensure_ascii=False).encode("utf-8")
        reason = {200: "OK", 400: "Bad Request", 403: "Forbidden", 404: "Not Found", 502: "Bad Gateway", 504: "Gateway Timeout"}[status]
        writer.write(
            f"HTTP/1.1 {status} {reason}\r\nContent-Type: application/json; charset=utf-8\r\nContent-Length: {len(raw)}\r\nConnection: close\r\n\r\n".encode("ascii")
            + raw
        )
        await writer.drain()

    @staticmethod
    def _conversation_from_outbound(message: Mapping[str, Any], route: Mapping[str, Any]) -> str:
        candidates: list[Any] = [
            route.get("target_user_id"),
            route.get("user_id"),
            route.get("conversation_id"),
            route.get("session_id"),
            message.get("session_id"),
        ]
        info = message.get("message_info")
        if isinstance(info, Mapping):
            additional = info.get("additional_config")
            if isinstance(additional, Mapping):
                candidates.extend(
                    [additional.get("platform_io_target_user_id"), additional.get("website_conversation_id")]
                )
        for candidate in candidates:
            value = str(candidate or "").strip()
            if value:
                return value
        return ""

    @staticmethod
    def _extract_text(message: Mapping[str, Any]) -> str:
        for key in ("processed_plain_text", "display_message", "text", "content"):
            value = message.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        raw_message = message.get("raw_message")
        if isinstance(raw_message, list):
            pieces: list[str] = []
            for segment in raw_message:
                if not isinstance(segment, Mapping):
                    continue
                data = segment.get("data")
                if isinstance(data, Mapping) and isinstance(data.get("text"), str):
                    pieces.append(data["text"])
            if "".join(pieces).strip():
                return "".join(pieces).strip()
        return ""


def create_plugin() -> PersonalWebsiteGatewayPlugin:
    return PersonalWebsiteGatewayPlugin()
