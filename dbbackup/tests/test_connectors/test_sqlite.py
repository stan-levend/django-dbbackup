from io import BytesIO

from django.db import connection
from django.test import TestCase
from mock import mock_open, patch

from dbbackup.db.sqlite import SqliteConnector, SqliteCPConnector
from dbbackup.tests.testapp.models import CharModel


class SqliteConnectorTest(TestCase):
    def test_write_dump_exclude(self):
        with connection.cursor() as c:
            c.execute("PRAGMA writable_schema = 1;")
            c.execute("CREATE TABLE sqlite_foo( name varchar(255), seq int );")
            c.execute("CREATE TABLE foo( name varchar(255), seq int );")

        dump_file = BytesIO()
        connector = SqliteConnector()
        connector.exclude = ["foo"]
        connector._write_dump(dump_file)
        dump_file.seek(0)
        for line in dump_file:
            self.assertTrue(line.strip().endswith(b";"))
            self.assertTrue(b"sqlite_foo" not in line)
            self.assertTrue(b"foo" not in line)

    def test_write_dump(self):
        dump_file = BytesIO()
        connector = SqliteConnector()
        connector._write_dump(dump_file)
        dump_file.seek(0)
        for line in dump_file:
            self.assertTrue(line.strip().endswith(b";"))

    def test_create_dump(self):
        connector = SqliteConnector()
        dump = connector.create_dump()
        self.assertTrue(dump.read())

    def test_create_dump_with_unicode(self):
        CharModel.objects.create(field="\xe9")
        connector = SqliteConnector()
        dump = connector.create_dump()
        self.assertTrue(dump.read())

    def test_restore_dump(self):
        connector = SqliteConnector()
        dump = connector.create_dump()
        connector.restore_dump(dump)

    def test_create_dump_with_virtual_tables(self):
        with connection.cursor() as c:
            c.execute("CREATE VIRTUAL TABLE lookup USING fts5(field)")

        connector = SqliteConnector()
        dump = connector.create_dump()
        self.assertTrue(dump.read())


@patch("dbbackup.db.sqlite.open", mock_open(read_data=b"foo"), create=True)
class SqliteCPConnectorTest(TestCase):
    def test_create_dump(self):
        connector = SqliteCPConnector()
        dump = connector.create_dump()
        dump_content = dump.read()
        self.assertTrue(dump_content)
        self.assertEqual(dump_content, b"foo")

    def test_restore_dump(self):
        connector = SqliteCPConnector()
        dump = connector.create_dump()
        connector.restore_dump(dump)
