"""qg.net 适配器测试：deadline 解析、批内最早过期、响应解析。"""
import time
from datetime import UTC

import pytest

from proxy_hub.providers.base import ProxyItem
from proxy_hub.providers.qgnet import (
    QgNetProvider,
    earliest_deadline_epoch,
    parse_deadline_epoch,
)


def test_parse_deadline_epoch_beijing():
    epoch = parse_deadline_epoch("2026-06-18 14:38:23")
    assert epoch is not None
    # 北京时间 = UTC+8
    from datetime import datetime
    assert epoch == datetime(2026, 6, 18, 6, 38, 23, tzinfo=UTC).timestamp()


def test_parse_deadline_invalid():
    assert parse_deadline_epoch("not-a-date") is None
    assert parse_deadline_epoch("") is None


def test_earliest_deadline():
    now = time.time()
    items = [
        ProxyItem(server="a", deadline_epoch=now + 120),
        ProxyItem(server="b", deadline_epoch=now + 60),
        ProxyItem(server="c", deadline_epoch=0),  # 解析失败的忽略
    ]
    assert earliest_deadline_epoch(items) == now + 60
    assert earliest_deadline_epoch([]) is None


def test_txt_format_rejected():
    with pytest.raises(ValueError, match="format=txt"):
        QgNetProvider("https://share.proxy.qg.net/get?key=k&format=txt")
