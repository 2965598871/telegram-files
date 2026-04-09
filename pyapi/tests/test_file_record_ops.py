import sqlite3
import unittest

from app.db import init_schema
from app.file_record_ops import upsert_tdlib_file_record


class FileRecordOpsTest(unittest.TestCase):
    def test_upsert_keeps_distinct_file_ids_for_same_unique_id(self) -> None:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        init_schema(conn)

        base_payload = {
            "telegramId": 1,
            "uniqueId": "shared-unique",
            "chatId": 100,
            "mediaAlbumId": 0,
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
            "extra": {"width": 640, "height": 480, "type": "x"},
            "threadChatId": 0,
            "messageThreadId": 0,
            "reactionCount": 0,
        }

        upsert_tdlib_file_record(
            conn,
            file_payload={
                **base_payload,
                "id": 111,
                "messageId": 50,
                "fileName": "existing-a.jpg",
                "transferStatus": "completed",
            },
        )
        upsert_tdlib_file_record(
            conn,
            file_payload={
                **base_payload,
                "id": 222,
                "messageId": 60,
                "fileName": "existing-b.jpg",
                "transferStatus": "idle",
            },
        )

        rows = conn.execute(
            """
            SELECT id, transfer_status
            FROM file_record
            WHERE telegram_id = ? AND unique_id = ?
            ORDER BY id ASC
            """,
            (1, "shared-unique"),
        ).fetchall()

        self.assertEqual(
            [(row["id"], row["transfer_status"]) for row in rows],
            [(111, "completed"), (222, "idle")],
        )


if __name__ == "__main__":
    unittest.main()
