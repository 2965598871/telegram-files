import sqlite3
import unittest
from unittest.mock import patch

from app.db import init_schema
from app.file_record_ops import upsert_tdlib_file_record
from app.tdlib_queries import load_tdlib_chat_files


class _FakeTdlibQueryManager:
    def request(self, account_key: str, payload: dict, timeout_seconds: float):
        request_type = str(payload.get("@type") or "")
        if request_type != "getChatHistory":
            raise AssertionError(f"Unexpected TDLib request: {request_type}")

        return {
            "@type": "messages",
            "messages": [
                {
                    "@type": "message",
                    "id": 200,
                    "chat_id": 100,
                    "date": 1710000000,
                    "message_thread_id": 0,
                    "media_album_id": 0,
                    "content": {
                        "@type": "messagePhoto",
                        "caption": {"text": "same photo again"},
                        "has_spoiler": False,
                        "photo": {
                            "sizes": [
                                {
                                    "width": 640,
                                    "height": 480,
                                    "type": "x",
                                    "photo": {
                                        "id": 321,
                                        "size": 1234,
                                        "expected_size": 1234,
                                        "local": {
                                            "is_downloading_completed": False,
                                            "is_downloading_active": False,
                                            "downloaded_size": 0,
                                            "path": "",
                                        },
                                        "remote": {
                                            "id": "remote-321",
                                            "unique_id": "dup-photo-1",
                                        },
                                    },
                                }
                            ],
                            "minithumbnail": {"data": "thumb"},
                        },
                    },
                }
            ],
        }


class TdlibQueriesTest(unittest.TestCase):
    def test_load_chat_files_marks_archive_duplicates_and_filters_them(self) -> None:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        init_schema(conn)
        upsert_tdlib_file_record(
            conn,
            file_payload={
                "id": 111,
                "telegramId": 1,
                "uniqueId": "dup-photo-1",
                "messageId": 50,
                "chatId": 100,
                "mediaAlbumId": 0,
                "fileName": "existing.jpg",
                "type": "photo",
                "mimeType": "image/jpeg",
                "size": 1234,
                "downloadedSize": 1234,
                "thumbnail": "",
                "downloadStatus": "completed",
                "date": 1710000000,
                "caption": "existing",
                "localPath": "D:/downloads/existing.jpg",
                "hasSensitiveContent": False,
                "startDate": 0,
                "completionDate": 1710000100000,
                "transferStatus": "idle",
                "extra": {"width": 640, "height": 480, "type": "x"},
                "threadChatId": 0,
                "messageThreadId": 0,
                "reactionCount": 0,
            },
        )

        with patch(
            "app.tdlib_queries.load_tdlib_session_for_account", return_value=True
        ):
            result = load_tdlib_chat_files(
                _FakeTdlibQueryManager(),
                db=conn,
                telegram_id=1,
                root_path="D:/tdlib/account-1",
                chat_id=100,
                filters={"alreadyDownloaded": "true", "limit": "20"},
            )

        self.assertEqual(result["size"], 1)
        file_item = result["files"][0]
        self.assertTrue(file_item["alreadyDownloaded"])
        self.assertEqual(file_item["downloadStatus"], "completed")
        self.assertEqual(file_item["localPath"], "D:/downloads/existing.jpg")


if __name__ == "__main__":
    unittest.main()
