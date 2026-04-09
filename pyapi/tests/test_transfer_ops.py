import json
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app.db import init_schema
from app.file_record_ops import file_for_transfer, upsert_tdlib_file_record
from app.transfer_ops import execute_transfer


class _FakeResponse:
    def __init__(self, payload: dict):
        self._payload = json.dumps(payload).encode("utf-8")

    def read(self) -> bytes:
        return self._payload

    def __enter__(self):
        return self

    def __exit__(self, _exc_type, _exc, _tb):
        return False


class TransferOpsTest(unittest.TestCase):
    def _row_for_source(self, source_path: Path) -> sqlite3.Row:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute(
            """
            CREATE TABLE file_record (
                id INTEGER,
                unique_id TEXT,
                telegram_id INTEGER,
                chat_id INTEGER,
                type TEXT,
                file_name TEXT,
                local_path TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO file_record(id, unique_id, telegram_id, chat_id, type, file_name, local_path)
            VALUES(?, ?, ?, ?, ?, ?, ?)
            """,
            (1, "uniq-1", 456, 789, "file", source_path.name, str(source_path)),
        )
        row = conn.execute("SELECT * FROM file_record LIMIT 1").fetchone()
        self.assertIsNotNone(row)
        return row

    def test_group_by_ai_moves_file_into_classified_folder(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            source_path = temp_path / "source.txt"
            source_path.write_text("hello", encoding="utf-8")
            destination = temp_path / "dest"
            row = self._row_for_source(source_path)

            captured_request: dict[str, object] = {}

            def _fake_urlopen(req, timeout):
                captured_request["url"] = req.full_url
                captured_request["timeout"] = timeout
                captured_request["body"] = json.loads(req.data.decode("utf-8"))
                return _FakeResponse(
                    {
                        "choices": [
                            {
                                "message": {
                                    "content": json.dumps(
                                        {
                                            "path": "Classified/Docs",
                                            "reason": "document",
                                        }
                                    )
                                }
                            }
                        ]
                    }
                )

            rule = {
                "destination": str(destination),
                "transferPolicy": "GROUP_BY_AI",
                "duplicationPolicy": "OVERWRITE",
                "extra": {
                    "promptTemplate": "Classify {file_name} from {telegram_id}/{chat_id}",
                },
            }

            with (
                patch.dict(
                    os.environ,
                    {"OPENAI_API_KEY": "test-key", "OPENAI_MODEL": "gpt-4o-mini"},
                    clear=False,
                ),
                patch("app.transfer_ops.request.urlopen", side_effect=_fake_urlopen),
            ):
                status, resolved_path = execute_transfer(row, rule)

            expected_path = destination / "Classified" / "Docs" / "source.txt"
            self.assertEqual(status, "completed")
            self.assertEqual(Path(resolved_path), expected_path)
            self.assertTrue(expected_path.exists())
            self.assertFalse(source_path.exists())
            self.assertEqual(
                captured_request["url"], "https://api.openai.com/v1/chat/completions"
            )
            self.assertEqual(captured_request["timeout"], 30.0)
            self.assertIn(
                "source.txt", captured_request["body"]["messages"][1]["content"]
            )

    def test_group_by_ai_supports_exact_file_path_response(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            source_path = temp_path / "source.txt"
            source_path.write_text("hello", encoding="utf-8")
            destination = temp_path / "dest"
            row = self._row_for_source(source_path)

            rule = {
                "destination": str(destination),
                "transferPolicy": "GROUP_BY_AI",
                "duplicationPolicy": "OVERWRITE",
                "extra": {
                    "promptTemplate": "Return an exact file path for {file_name}"
                },
            }

            with (
                patch.dict(
                    os.environ,
                    {"OPENAI_API_KEY": "test-key"},
                    clear=False,
                ),
                patch(
                    "app.transfer_ops.request.urlopen",
                    return_value=_FakeResponse(
                        {
                            "choices": [
                                {
                                    "message": {
                                        "content": json.dumps(
                                            {
                                                "path": "Classified/renamed.txt",
                                                "reason": "renamed",
                                            }
                                        )
                                    }
                                }
                            ]
                        }
                    ),
                ),
            ):
                status, resolved_path = execute_transfer(row, rule)

            expected_path = destination / "Classified" / "renamed.txt"
            self.assertEqual(status, "completed")
            self.assertEqual(Path(resolved_path), expected_path)
            self.assertTrue(expected_path.exists())
            self.assertFalse(source_path.exists())

    def test_group_by_hashtag_uses_album_caption_for_later_items(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            source_path = temp_path / "second.jpg"
            source_path.write_text("hello", encoding="utf-8")
            destination = temp_path / "dest"

            conn = sqlite3.connect(":memory:")
            conn.row_factory = sqlite3.Row
            init_schema(conn)

            base_payload = {
                "telegramId": 1,
                "chatId": 100,
                "mediaAlbumId": 9001,
                "type": "photo",
                "mimeType": "image/jpeg",
                "size": 1234,
                "downloadedSize": 1234,
                "thumbnail": "",
                "downloadStatus": "completed",
                "date": 1710000000,
                "hasSensitiveContent": False,
                "startDate": 0,
                "completionDate": 1710000100000,
                "extra": {"width": 640, "height": 480, "type": "x"},
                "threadChatId": 0,
                "messageThreadId": 0,
                "reactionCount": 0,
                "transferStatus": "idle",
            }

            upsert_tdlib_file_record(
                conn,
                file_payload={
                    **base_payload,
                    "id": 111,
                    "uniqueId": "album-first",
                    "messageId": 50,
                    "fileName": "first.jpg",
                    "caption": "#trip album",
                    "localPath": str(temp_path / "first.jpg"),
                },
            )
            upsert_tdlib_file_record(
                conn,
                file_payload={
                    **base_payload,
                    "id": 222,
                    "uniqueId": "album-second",
                    "messageId": 51,
                    "fileName": "second.jpg",
                    "caption": "",
                    "localPath": str(source_path),
                },
            )

            row = file_for_transfer(
                conn,
                telegram_id=1,
                file_id=222,
                unique_id="album-second",
            )

            self.assertIsNotNone(row)
            self.assertEqual(str(row["caption"] or ""), "#trip album")

            rule = {
                "destination": str(destination),
                "transferPolicy": "GROUP_BY_HASHTAG",
                "duplicationPolicy": "OVERWRITE",
                "extra": {
                    "hashtagRules": [
                        {
                            "hashtag": "trip",
                            "folder": "Trips",
                            "matchType": "EXACT",
                        }
                    ]
                },
            }

            status, resolved_path = execute_transfer(row, rule)

            expected_path = destination / "Trips" / "second.jpg"
            self.assertEqual(status, "completed")
            self.assertEqual(Path(resolved_path), expected_path)
            self.assertTrue(expected_path.exists())
            self.assertFalse(source_path.exists())


if __name__ == "__main__":
    unittest.main()
