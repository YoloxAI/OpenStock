import os
import tempfile
import unittest
from pathlib import Path

from app.config import ConfigError, load_settings


class ConfigTests(unittest.TestCase):
    def test_missing_token_raises_error(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            env_path = Path(tmpdir) / ".env"
            env_path.write_text("DB_PATH=data/test.db\n", encoding="utf-8")

            with self.assertRaises(ConfigError):
                load_settings(str(env_path))

    def test_load_settings_builds_absolute_db_path(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            original_cwd = Path.cwd()
            os.chdir(tmpdir)
            try:
                env_path = Path(tmpdir) / ".env"
                env_path.write_text(
                    "TUSHARE_TOKEN=test-token\nDB_PATH=data/test.db\n",
                    encoding="utf-8",
                )
                settings = load_settings(str(env_path))
            finally:
                os.chdir(original_cwd)

            self.assertEqual(settings.tushare_token, "test-token")
            self.assertTrue(str(settings.db_path).endswith("data/test.db"))


if __name__ == "__main__":
    unittest.main()

