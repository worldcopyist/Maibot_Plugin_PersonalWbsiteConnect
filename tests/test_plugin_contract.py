"""无需 MaiBot 运行时的协议契约测试。"""

from __future__ import annotations

import importlib.util
import sys
import types
import unittest
from typing import Any
from pathlib import Path


def _decorator(*_args: object, **_kwargs: object):
    def wrap(func):
        return func

    return wrap


def _load_plugin_module():
    sdk = types.ModuleType("maibot_sdk")

    class MaiBotPlugin:  # noqa: D101
        pass

    class PluginConfigBase:  # noqa: D101
        pass

    def field(*, default=None, default_factory=None, **_kwargs):
        return default_factory() if default_factory else default

    sdk.Field = field
    sdk.MaiBotPlugin = MaiBotPlugin
    sdk.MessageGateway = _decorator
    sdk.PluginConfigBase = PluginConfigBase
    sys.modules["maibot_sdk"] = sdk
    spec = importlib.util.spec_from_file_location("website_plugin", Path(__file__).parents[1] / "plugin.py")
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


PLUGIN = _load_plugin_module()


class PluginContractTests(unittest.TestCase):
    def test_manifest_has_supported_runtime_range(self):
        import json

        manifest = json.loads((Path(__file__).parents[1] / "_manifest.json").read_text())
        self.assertEqual(manifest["manifest_version"], 2)
        self.assertEqual(manifest["plugin_type"], "adapter")
        self.assertEqual(manifest["sdk"]["min_version"], "2.8.0")

    def test_inbound_message_has_gateway_contract(self):
        message = PLUGIN.PersonalWebsiteGatewayPlugin._build_inbound_message(
            "m-1", "myazure-user-7", "你好", "alice"
        )
        self.assertEqual(message["platform"], "personal_website")
        self.assertEqual(message["processed_plain_text"], "你好")
        self.assertEqual(message["message_info"]["user_info"]["user_id"], "myazure-user-7")
        self.assertEqual(message["message_info"]["user_info"]["user_nickname"], "alice")
        self.assertEqual(message["message_info"]["user_info"]["user_cardname"], "alice")
        self.assertNotIn("group_info", message["message_info"])
        self.assertEqual(message["message_info"]["additional_config"]["platform_io_target_user_id"], "myazure-user-7")
        self.assertEqual(message["message_info"]["additional_config"]["website_username"], "alice")
        self.assertEqual(message["message_info"]["additional_config"]["website_message_type"], "private")

    def test_inbound_message_normalizes_missing_or_control_nickname(self):
        self.assertEqual(PLUGIN.PersonalWebsiteGatewayPlugin._normalize_user_nickname("\n 世界猫（worldcat）\t"), "世界猫（worldcat）")
        self.assertEqual(PLUGIN.PersonalWebsiteGatewayPlugin._normalize_user_nickname(""), "网站用户")

    def test_outbound_reply_and_conversation_are_extracted(self):
        message = {
            "raw_message": [{"type": "text", "data": {"text": "来自 MaiBot 的回复"}}],
            "message_info": {"additional_config": {"platform_io_target_user_id": "myazure-user-7"}},
        }
        self.assertEqual(PLUGIN.PersonalWebsiteGatewayPlugin._extract_text(message), "来自 MaiBot 的回复")
        self.assertEqual(
            PLUGIN.PersonalWebsiteGatewayPlugin._conversation_from_outbound(message, {}),
            "myazure-user-7",
        )
        self.assertEqual(
            PLUGIN.PersonalWebsiteGatewayPlugin._conversation_candidates(
                message,
                {"target_user_id": "myazure", "conversation_id": "myazure-user-7"},
            ),
            ["myazure", "myazure-user-7"],
        )
        self.assertEqual(
            PLUGIN.PersonalWebsiteGatewayPlugin._conversation_candidates(
                {"reply_to": "website-inbound-1"},
                {},
            ),
            ["website-inbound-1"],
        )

    def test_outbound_reply_removes_only_a_leading_serialized_text_mapping(self):
        self.assertEqual(
            PLUGIN.PersonalWebsiteGatewayPlugin._extract_text(
                {"processed_plain_text": "{'text': 'hi'} 在的在的"}
            ),
            "在的在的",
        )
        self.assertEqual(
            PLUGIN.PersonalWebsiteGatewayPlugin._extract_text(
                {"processed_plain_text": '{"text": "hi"} 在的在的'}
            ),
            "在的在的",
        )
        self.assertEqual(
            PLUGIN.PersonalWebsiteGatewayPlugin._extract_text(
                {"processed_plain_text": "花括号内容：{text: hi}"}
            ),
            "花括号内容：{text: hi}",
        )

    def test_reply_is_segmented_on_sentence_boundaries_before_hard_cut(self):
        self.assertEqual(
            PLUGIN.PersonalWebsiteGatewayPlugin._segment_reply("第一句。第二句很长。第三句。", 8),
            ["第一句。", "第二句很长。", "第三句。"],
        )
        self.assertEqual(
            PLUGIN.PersonalWebsiteGatewayPlugin._segment_reply("abcdefghijkl", 5),
            ["abcde", "fghij", "kl"],
        )

    def test_gateway_round_trip_correlates_the_same_conversation(self):
        class Gateway:
            async def route_message(self, **kwargs: Any) -> bool:
                message = kwargs["message"]
                conversation_id = message["message_info"]["user_info"]["user_id"]
                await plugin.deliver_to_website(
                    {"processed_plain_text": "已收到"},
                    {"target_user_id": conversation_id},
                )
                return True

        class Context:
            gateway = Gateway()

        async def run() -> str:
            return await plugin._route_and_wait("myazure-user-7", "你好")

        plugin = PLUGIN.PersonalWebsiteGatewayPlugin()
        plugin.ctx = Context()
        settings = PLUGIN.PersonalWebsiteSettings()
        settings.gateway.request_timeout_seconds = 3
        plugin.config = settings
        self.assertEqual(__import__("asyncio").run(run()), "已收到")

    def test_gateway_prefers_a_pending_conversation_over_route_account_target(self):
        class Gateway:
            async def route_message(self, **kwargs: Any) -> bool:
                conversation_id = kwargs["message"]["message_info"]["user_info"]["user_id"]
                await plugin.deliver_to_website(
                    {
                        "processed_plain_text": "已收到",
                        "message_info": {"additional_config": {"website_conversation_id": conversation_id}},
                    },
                    {"target_user_id": "myazure"},
                )
                return True

        class Context:
            gateway = Gateway()

        async def run() -> str:
            return await plugin._route_and_wait("myazure-user-7", "你好")

        plugin = PLUGIN.PersonalWebsiteGatewayPlugin()
        plugin.ctx = Context()
        settings = PLUGIN.PersonalWebsiteSettings()
        settings.gateway.request_timeout_seconds = 3
        plugin.config = settings
        self.assertEqual(__import__("asyncio").run(run()), "已收到")

    def test_gateway_private_reply_uses_reply_to_when_target_user_id_is_not_preserved(self):
        class Gateway:
            async def route_message(self, **kwargs: Any) -> bool:
                inbound = kwargs["message"]
                await plugin.deliver_to_website(
                    {
                        "processed_plain_text": "已通过私聊回调关联",
                        "reply_to": inbound["message_id"],
                        "message_info": {"additional_config": {"platform_io_account_id": "myazure"}},
                    },
                    {"platform": "personal_website", "account_id": "myazure", "scope": "internal"},
                )
                return True

        class Context:
            gateway = Gateway()

            class logger:
                @staticmethod
                def warning(*args: Any, **kwargs: Any) -> None:
                    del args, kwargs

        async def run() -> str:
            return await plugin._route_and_wait("myazure-user-7", "你好", "alice")

        plugin = PLUGIN.PersonalWebsiteGatewayPlugin()
        plugin.ctx = Context()
        settings = PLUGIN.PersonalWebsiteSettings()
        settings.gateway.request_timeout_seconds = 3
        plugin.config = settings
        self.assertEqual(__import__("asyncio").run(run()), "已通过私聊回调关联")


if __name__ == "__main__":
    unittest.main()
