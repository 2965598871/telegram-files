import asyncio
import sqlite3
import unittest
from unittest.mock import patch

from starlette.requests import Request

from app.db import init_schema, list_files
from app.file_record_ops import upsert_tdlib_file_record
from app.routers.files import telegram_chat_group_files


class GroupChatSearchTest(unittest.TestCase):
    def test_group_search_uses_single_combined_query(self) -> None:
        request = Request(
            {
                "type": "http",
                "method": "GET",
                "path": "/telegram/1/chat-group/group-1/files",
                "query_string": (
                    b"search=%2523cat&sort=date&order=desc&type=media&offline=true"
                ),
                "headers": [],
            }
        )

        with (
            patch(
                "app.routers.files.get_chat_group",
                return_value={"chatIds": [100, 200]},
            ),
            patch(
                "app.routers.files.list_files",
                return_value={
                    "files": [],
                    "count": 0,
                    "size": 0,
                    "nextFromMessageId": 0,
                },
            ) as list_files_mock,
        ):
            result = asyncio.run(
                telegram_chat_group_files(1, "group-1", request, db=object())
            )

        self.assertEqual(result["size"], 0)
        self.assertEqual(list_files_mock.call_count, 1)
        _, kwargs = list_files_mock.call_args
        self.assertEqual(kwargs["telegram_id"], 1)
        self.assertEqual(kwargs["chat_id"], 0)
        self.assertEqual(kwargs["chat_ids"], [100, 200])
        self.assertEqual(kwargs["filters"]["search"], "#cat")

    def test_list_files_search_matches_album_caption_entries(self) -> None:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        init_schema(conn)

        upsert_tdlib_file_record(
            conn,
            file_payload={
                "id": 1,
                "telegramId": 1,
                "uniqueId": "album-1-a",
                "messageId": 101,
                "chatId": 100,
                "mediaAlbumId": 5001,
                "fileName": "cover.jpg",
                "type": "photo",
                "mimeType": "image/jpeg",
                "size": 100,
                "downloadedSize": 0,
                "thumbnail": "",
                "downloadStatus": "idle",
                "date": 1710000001,
                "caption": "#猫猫碎冰冰",
                "localPath": "",
                "hasSensitiveContent": False,
                "startDate": 0,
                "completionDate": 0,
                "transferStatus": "idle",
                "extra": {},
                "threadChatId": 0,
                "messageThreadId": 0,
                "reactionCount": 0,
            },
        )
        upsert_tdlib_file_record(
            conn,
            file_payload={
                "id": 2,
                "telegramId": 1,
                "uniqueId": "album-1-b",
                "messageId": 102,
                "chatId": 100,
                "mediaAlbumId": 5001,
                "fileName": "detail.jpg",
                "type": "photo",
                "mimeType": "image/jpeg",
                "size": 100,
                "downloadedSize": 0,
                "thumbnail": "",
                "downloadStatus": "idle",
                "date": 1710000002,
                "caption": "",
                "localPath": "",
                "hasSensitiveContent": False,
                "startDate": 0,
                "completionDate": 0,
                "transferStatus": "idle",
                "extra": {},
                "threadChatId": 0,
                "messageThreadId": 0,
                "reactionCount": 0,
            },
        )

        result = list_files(
            conn,
            telegram_id=1,
            chat_id=0,
            chat_ids=[100],
            filters={
                "search": "#猫猫碎冰冰",
                "type": "media",
                "sort": "date",
                "order": "desc",
                "limit": "20",
            },
        )

        self.assertEqual(result["size"], 2)
        self.assertEqual(
            [item["uniqueId"] for item in result["files"]],
            ["album-1-b", "album-1-a"],
        )


if __name__ == "__main__":
    unittest.main()
