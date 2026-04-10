import sqlite3
import unittest
from unittest.mock import patch

from app.db import init_schema
from app.file_record_ops import upsert_tdlib_file_record
from app.maintenance import run_album_metadata_backfill, run_thumbnail_backfill


class _FakeAlbumMaintenanceManager:
    def request(self, account_key: str, payload: dict, timeout_seconds: float):
        del account_key, timeout_seconds
        if str(payload.get("@type") or "") != "getMessage":
            raise AssertionError(f"Unexpected TDLib request: {payload}")

        message_id = int(payload.get("message_id") or 0)
        if message_id != 201:
            return {"@type": "error", "message": "Message not found"}
        return {
            "@type": "message",
            "id": 201,
            "chat_id": 100,
            "date": 1710000000,
            "message_thread_id": 0,
            "media_album_id": 900,
            "content": {
                "@type": "messagePhoto",
                "caption": {"text": "#trip album"},
                "has_spoiler": False,
                "photo": {
                    "sizes": [
                        {
                            "width": 640,
                            "height": 480,
                            "type": "x",
                            "photo": {
                                "id": 111,
                                "size": 1234,
                                "expected_size": 1234,
                                "local": {
                                    "is_downloading_completed": False,
                                    "is_downloading_active": False,
                                    "downloaded_size": 0,
                                    "path": "",
                                },
                                "remote": {
                                    "id": "remote-111",
                                    "unique_id": "album-a",
                                },
                            },
                        }
                    ],
                    "minithumbnail": {"data": "thumb"},
                },
            },
        }


class _FakeThumbnailMaintenanceManager:
    def request(self, account_key: str, payload: dict, timeout_seconds: float):
        del account_key, timeout_seconds
        request_type = str(payload.get("@type") or "")
        if request_type == "getMessage":
            return {
                "@type": "message",
                "id": 301,
                "chat_id": 100,
                "date": 1710000000,
                "message_thread_id": 0,
                "media_album_id": 0,
                "content": {
                    "@type": "messageDocument",
                    "caption": {"text": "invoice"},
                    "document": {
                        "file_name": "invoice.pdf",
                        "mime_type": "application/pdf",
                        "document": {
                            "id": 222,
                            "size": 4321,
                            "expected_size": 4321,
                            "local": {
                                "is_downloading_completed": False,
                                "is_downloading_active": False,
                                "downloaded_size": 0,
                                "path": "",
                            },
                            "remote": {
                                "id": "remote-222",
                                "unique_id": "doc-a",
                            },
                        },
                        "thumbnail": {
                            "width": 320,
                            "height": 240,
                            "format": {"@type": "thumbnailFormatJpeg"},
                            "file": {
                                "id": 333,
                                "size": 99,
                                "expected_size": 99,
                                "local": {
                                    "is_downloading_completed": False,
                                    "is_downloading_active": False,
                                    "downloaded_size": 0,
                                    "path": "",
                                },
                                "remote": {
                                    "id": "remote-333",
                                    "unique_id": "thumb-a",
                                },
                            },
                        },
                        "minithumbnail": {"data": "thumb"},
                    },
                },
            }
        if request_type == "downloadFile":
            self.assertEqual(int(payload.get("file_id") or 0), 333)
            return {
                "@type": "file",
                "id": 333,
                "size": 99,
                "expected_size": 99,
                "local": {
                    "is_downloading_completed": True,
                    "is_downloading_active": False,
                    "downloaded_size": 99,
                    "path": "D:/tdlib/thumb-a.jpg",
                },
                "remote": {
                    "id": "remote-333",
                    "unique_id": "thumb-a",
                },
            }
        raise AssertionError(f"Unexpected TDLib request: {payload}")

    def assertEqual(self, left, right) -> None:
        if left != right:
            raise AssertionError(f"{left!r} != {right!r}")


class MaintenanceTest(unittest.TestCase):
    def test_album_backfill_updates_album_id_and_caption_source(self) -> None:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        init_schema(conn)
        upsert_tdlib_file_record(
            conn,
            file_payload={
                "id": 111,
                "telegramId": 1,
                "uniqueId": "album-a",
                "messageId": 201,
                "chatId": 100,
                "mediaAlbumId": 0,
                "fileName": "a.jpg",
                "type": "photo",
                "mimeType": "image/jpeg",
                "size": 100,
                "downloadedSize": 0,
                "thumbnail": "",
                "downloadStatus": "idle",
                "date": 1710000000,
                "caption": "",
                "localPath": "",
                "hasSensitiveContent": False,
                "startDate": 0,
                "completionDate": 0,
                "transferStatus": "idle",
                "extra": {"width": 640, "height": 480, "type": "x"},
                "threadChatId": 0,
                "messageThreadId": 0,
                "reactionCount": 0,
            },
        )
        upsert_tdlib_file_record(
            conn,
            file_payload={
                "id": 112,
                "telegramId": 1,
                "uniqueId": "album-b",
                "messageId": 200,
                "chatId": 100,
                "mediaAlbumId": 900,
                "fileName": "b.jpg",
                "type": "photo",
                "mimeType": "image/jpeg",
                "size": 100,
                "downloadedSize": 0,
                "thumbnail": "",
                "downloadStatus": "idle",
                "date": 1710000000,
                "caption": "",
                "localPath": "",
                "hasSensitiveContent": False,
                "startDate": 0,
                "completionDate": 0,
                "transferStatus": "idle",
                "extra": {"width": 640, "height": 480, "type": "x"},
                "threadChatId": 0,
                "messageThreadId": 0,
                "reactionCount": 0,
            },
        )

        with patch("app.maintenance._load_tdlib_session", return_value=True):
            result = run_album_metadata_backfill(
                conn,
                _FakeAlbumMaintenanceManager(),
                telegram_id=1,
                root_path="D:/tdlib/account-1",
                limit=10,
            )

        self.assertEqual(result["scanned"], 1)
        self.assertEqual(result["updated"], 1)

        row_a = conn.execute(
            "SELECT media_album_id, caption FROM file_record WHERE unique_id = ?",
            ("album-a",),
        ).fetchone()
        row_b = conn.execute(
            "SELECT media_album_id, caption FROM file_record WHERE unique_id = ?",
            ("album-b",),
        ).fetchone()
        self.assertEqual(int(row_a["media_album_id"] or 0), 900)
        self.assertEqual(str(row_a["caption"] or ""), "#trip album")
        self.assertEqual(int(row_b["media_album_id"] or 0), 900)
        self.assertEqual(str(row_b["caption"] or ""), "")

    def test_thumbnail_backfill_links_thumbnail_record(self) -> None:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        init_schema(conn)
        upsert_tdlib_file_record(
            conn,
            file_payload={
                "id": 222,
                "telegramId": 1,
                "uniqueId": "doc-a",
                "messageId": 301,
                "chatId": 100,
                "mediaAlbumId": 0,
                "fileName": "invoice.pdf",
                "type": "file",
                "mimeType": "application/pdf",
                "size": 4321,
                "downloadedSize": 0,
                "thumbnail": "",
                "downloadStatus": "idle",
                "date": 1710000000,
                "caption": "invoice",
                "localPath": "",
                "hasSensitiveContent": False,
                "startDate": 0,
                "completionDate": 0,
                "transferStatus": "idle",
                "extra": None,
                "threadChatId": 0,
                "messageThreadId": 0,
                "reactionCount": 0,
            },
        )

        with patch("app.maintenance._load_tdlib_session", return_value=True):
            result = run_thumbnail_backfill(
                conn,
                _FakeThumbnailMaintenanceManager(),
                telegram_id=1,
                root_path="D:/tdlib/account-1",
                limit=10,
            )

        self.assertEqual(result["scanned"], 1)
        self.assertEqual(result["updated"], 1)

        parent = conn.execute(
            "SELECT thumbnail_unique_id FROM file_record WHERE unique_id = ?",
            ("doc-a",),
        ).fetchone()
        thumb = conn.execute(
            "SELECT type, local_path, download_status FROM file_record WHERE unique_id = ?",
            ("thumb-a",),
        ).fetchone()
        self.assertEqual(str(parent["thumbnail_unique_id"] or ""), "thumb-a")
        self.assertEqual(str(thumb["type"] or ""), "thumbnail")
        self.assertEqual(str(thumb["download_status"] or ""), "completed")
        self.assertEqual(str(thumb["local_path"] or ""), "D:/tdlib/thumb-a.jpg")


if __name__ == "__main__":
    unittest.main()
