from textual.widgets import (
    Label,
    Input,
    Button,
    Select,
    Header,
    Footer,
    RadioSet,
    RadioButton,
)
from textual.types import SelectType
from textual.containers import Center, Horizontal
from textual.screen import Screen
from textual.app import ComposeResult

from typing import Optional
import typing
import pymssql
import traceback

from ..LogRecord import LogRecord


class AuthScreen(Screen):
    CSS_PATH = "css/auth.tcss"
    CURSOR: Optional[pymssql.Cursor] = None

    def compose(self) -> ComposeResult:
        yield Header()
        yield Footer()

        with Center():
            yield Label("Server")
            yield Input(placeholder="localhost,1433", id="server-input")

            yield Label("Authentication")
            yield Select(
                options=[("Windows", "win"), ("SQL Login", "sql")],
                id="auth-select",
                allow_blank=False,
            )

            yield Label("Username")
            yield Input(placeholder="sa", id="username-input")

            yield Label("Password")
            yield Input(id="password-input", password=True)

            with RadioSet(id="auth-radio"):
                yield RadioButton(
                    "Online Transaction Log", value=True, name="online-log"
                )
                yield RadioButton("Backup File", name="backup-file", value=False)

            with Horizontal():
                yield Label("Desde")
                yield Input("2024-12-31", id="from-date")

                yield Label("Hasta", id="to-label")
                yield Input("2025-12-31", id="to-date")

            yield Button("Conectar", id="connect-button")

    def fetch_online_log(self):
        pass

    def fetch_table_schema(self) -> dict[str, list[tuple[str, str]]]:
        """Fetches the schema of the table. Sorts the column in how is it expected to be in the transaction log."""
        if not self.CURSOR:
            return {}

        self.CURSOR.execute(
            "SELECT DISTINCT TABLE_NAME FROM INFORMATION_SCHEMA.COLUMNS;"
        )
        tables: dict[str, list[tuple[str, str]]] = {}

        for row in self.CURSOR.fetchall():
            table_name: str = row[0]
            self.CURSOR.execute(
                f"""SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}';"""
            )

            if table_name not in tables:
                tables[table_name] = []

            # Types that are put to the end of the RowLog because of their variable size.
            final_types = []
            for column_row in self.CURSOR.fetchall():
                column_name, data_type = column_row

                if data_type in ["char", "varchar", "nvarchar", "nchar"]:
                    final_types.append((column_name, data_type))
                else:
                    tables[table_name].append((column_name, data_type))

            tables[table_name].extend(final_types)

        return tables

    def on_button_pressed(self, event: Button.Pressed) -> None:
        """Button pressed event handler."""

        server_data = self.query_one("#server-input", expect_type=Input).value
        auth = self.query_one("#auth-select", expect_type=Select).value
        username = self.query_one("#username-input", expect_type=Input).value
        password = self.query_one("#password-input", expect_type=Input).value

        # TODO: Implement Date logic and Windows Auth

        host = server_data.split(",")[0]
        port = server_data.split(",")[1]

        #! remove on prod
        host = "localhost"
        port = "1433"
        username = "sa"
        password = "freaky_gates123"

        # TODO: Database selector
        with pymssql.connect(
            server=host,
            port=port,
            user=username,
            password=password,
        ) as conn:
            self.notify("Connected!")
            self.CURSOR = conn.cursor()

            # TODO: Usar base de datos que quiera usar;
            self.CURSOR.execute("USE sachen;")

            self.CURSOR.execute(
                """SELECT 
    Operation,
    Context,
    [Transaction ID],
    AllocUnitName,
    [RowLog Contents 0],
    [RowLog Contents 1],
    [RowLog Contents 2],
    [RowLog Contents 3],
    *
FROM fn_dblog(NULL, NULL)
WHERE AllocUnitName IS NOT NULL 
  AND AllocUnitName NOT LIKE 'sys.%'
  AND AllocUnitName NOT LIKE 'Unknown Alloc%';
"""
            )

            logs: list[LogRecord] = []
            for row in self.CURSOR.fetchall():
                logs.append(
                    LogRecord(
                        operation=row[0],
                        context=row[1],
                        transaction_id=row[2],
                        alloc_unit=row[3],
                        raw_data=row[4],
                        raw_data2=row[5],
                    )
                )
            self.notify("Done Reading. Parsing...")
            self.parse_transaction_log(logs)

    def parse_transaction_log(self, log: list[LogRecord]) -> None:
        """Just parse it."""

        schema = self.fetch_table_schema()
        for record in log:
            # Parse the table name from alloc_unit
            # dbo.Something.[...]

            # Realistically, nobody is naming a table with a dot in the name.
            # If you are one of those, consider yourself an opp 🫵
            # - JH, 2024
            table_name = record.alloc_unit.split(".")[1]
            table_schema = schema.get(table_name, [])

            if not table_schema:
                raise TypeError(f"Table {table_name} not found in schema.")

            if record.operation == "LOP_INSERT_ROWS":
                # Skip first bytes
                useful_data = record.raw_data[4:]

                operation_data: dict[str, typing.Any] = {}
                for column_name, data_type in table_schema:
                    if data_type == "int":
                        if "id" in column_name.lower():
                            # Read an int (4 bytes)
                            print("Reading an ID")
                            read_int = int.from_bytes(useful_data[:4], "little")
                            operation_data[column_name] = read_int
                            useful_data = useful_data[2:]

                        else:
                            # Read an int (4 bytes)
                            read_int = int.from_bytes(useful_data[:4], "little")
                            operation_data[column_name] = read_int
                            useful_data = useful_data[4:]  # Skip the parsed data

                    elif data_type in ["char", "varchar", "nvarchar", "nchar"]:
                        # Read the string length (4 bytes, little-endian)
                        str_len = int.from_bytes(useful_data[:4], "little")
                        useful_data = useful_data[4:]  # Skip length bytes

                        print("String length:", str_len)

                        if data_type in ["nvarchar", "nchar"]:
                            # nvarchar uses UTF-16 encoding (2 bytes per character)
                            read_str = useful_data[: str_len * 2].decode("utf-16-le")
                            useful_data = useful_data[str_len * 2 :]
                        else:
                            # varchar uses UTF-8 or ASCII
                            read_str = useful_data[:str_len].decode("utf-8")
                            useful_data = useful_data[str_len:]

                        operation_data[column_name] = read_str

                    print(operation_data)

                break
