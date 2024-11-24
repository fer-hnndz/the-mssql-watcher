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
        parsed_transactions: dict[str, typing.Any] = {}
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

                if not parsed_transactions.get(record.operation):
                    parsed_transactions[record.operation] = {}
                    parsed_transactions[record.operation][table_name] = []

                elif not parsed_transactions[record.operation].get(table_name):
                    parsed_transactions[record.operation][table_name] = []

                # Skip first bytes
                useful_data = record.raw_data[4:]

                operation_data: dict[str, typing.Any] = {}
                for idx, (column_name, data_type) in enumerate(table_schema):
                    if data_type not in ["char", "varchar", "nvarchar", "nchar"]:

                        if data_type == "int":
                            read_int = int.from_bytes(useful_data[:4], "little")
                            operation_data[column_name] = read_int
                            useful_data = useful_data[4:]  # Skip parsed bytes

                # ================================================================================
                #                       Parse now variable length columns
                # ================================================================================

                # Filter only the variable length columns from the table schema
                variable_colummns = filter(
                    lambda col: col[1] in ["char", "varchar", "nvarchar", "nchar"],
                    table_schema,
                )

                # Declare here the dictionary to store the offsets so we can use the same offsets for the next column
                variable_offsets: dict[int, int] = {}

                for idx, (column_name, data_type) in enumerate(variable_colummns):

                    raw = record.raw_data  # Reset everything to the start of the data

                    # This offset represents the start position of the columns from the start of the data
                    column_offset = int.from_bytes(raw[2:4], "little")

                    if column_offset >= len(raw):
                        print(
                            "Error: Offset al número de columnas fuera del rango de datos."
                        )
                        return None

                    # The amount of columns is located 2 bytes after the column offset
                    total_columns = int.from_bytes(
                        raw[column_offset : column_offset + 2], "little"
                    )

                    print(f"Column count: {total_columns}")

                    # Calculate the amount of columns that have a variable size
                    null_bitmap_size = (total_columns + 7) // 8
                    variable_column_count_offset = column_offset + 2 + null_bitmap_size

                    # The amount of columns that have a variable size is located 2 bytes after the variable column count offset
                    variable_column_count = int.from_bytes(
                        raw[
                            variable_column_count_offset : variable_column_count_offset
                            + 2
                        ],
                        "little",
                    )

                    print(f"Variable column count: {variable_column_count}")

                    variable_data_start = (
                        variable_column_count_offset + 2 + variable_column_count * 2
                    )

                    offset_start = (
                        variable_column_count_offset + 2
                    )  # Here is where data starts

                    for i in range(variable_column_count):
                        start_pos = offset_start + (i * 2)
                        var_offset = int.from_bytes(
                            raw[start_pos : start_pos + 2], "little"
                        )

                        variable_offsets[i] = var_offset

                    print("Variable offsets:", variable_offsets)

                    last_offset = variable_offsets.get(idx - 1)
                    start = last_offset if last_offset else variable_data_start
                    end = (
                        variable_offsets[idx]
                        if idx < len(variable_offsets)
                        else len(raw)
                    )

                    print(f"Start: {start}, End: {end}")

                    data_chunk = raw[start:end]
                    decoded_value = self.try_decode(data_chunk)
                    operation_data[column_name] = decoded_value.strip()

                parsed_transactions[record.operation][table_name].append(operation_data)

        print("Parsed Transactions:", parsed_transactions)

    def try_decode(self, data):
        """
        Detecta automáticamente la codificación de los datos.
        Decodifica en UTF-8 primero, pero si encuentra un patrón típico de UTF-16, cambia a UTF-16.
        """

        if all(data[i] == 0 for i in range(1, len(data), 2)):
            try:
                return data.decode("utf-16", errors="strict")
            except UnicodeDecodeError:
                pass

        try:
            return data.decode("utf-8", errors="strict")
        except UnicodeDecodeError:
            return data.decode("utf-8", errors="replace")
