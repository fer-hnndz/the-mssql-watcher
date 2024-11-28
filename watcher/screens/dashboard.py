import typing

from textual.app import ComposeResult
from textual.containers import Center, Container, Vertical
from textual.screen import Screen
from textual.widgets import DataTable, Footer, Header, Label, Switch, Tabs, Tab, Log


class Dashboard(Screen):
    CSS_PATH = "css/dashboard.tcss"
    LAST_CELL_EVENT = None
    CURRENT_TAB = ""

    def __init__(
        self, parsed_data: dict[str, typing.Any], result_transactions: typing.Any = None
    ):
        super().__init__()
        self.app.sub_title = "Dashboard"
        self.parsed_data = parsed_data
        self.result_transactions = result_transactions

        print(self.parsed_data)

    def compose(self) -> ComposeResult:
        yield Header()
        yield Footer()

        with Container(id="switch-container"):
            with Vertical():
                yield Label("INSERT", id="insert-switch-label")
                yield Switch(value=True, id="insert-switch")

                yield Label("UPDATE", id="update-switch-label")
                yield Switch(value=True, id="update-switch")

                yield Label("DELETE", id="delete-switch-label")
                yield Switch(value=True, id="delete-switch")

        # Operation, Schema, Object, User, Begin Time, End Time, Transaction ID, LSN
        yield DataTable(id="transaction-table", zebra_stripes=True)

        yield Tabs(
            "Operation Details",
            "Row History",
            "Undo Script",
            "Redo Script",
            "Transaction Information",
        )
        with Container(id="operation-details-container"):
            yield Log(highlight=True, id="sql-log")

        # Tabs: Operation Details, Row History, Undo Script, Redo Script, Transaction Information

        # Operation Details: Field, Type, Old Value, New Value
        # Row History: Operation, Date, Username, LSN, [...Rows]
        # Undo Script: SQL Script
        # Redo Script: SQL Script

    # * on post_mount?
    def on_mount(self) -> None:
        log = self.query_one("#sql-log", expect_type=Log)

        table = self.query_one("#transaction-table", expect_type=DataTable)
        table.add_columns(
            "Operation",
            "Schema",
            "Object",
            "User",
            "Begin Time",
            "End Time",
            "Transaction ID",
            "LSN",
        )

        inserts_enabled = self.query_one("#insert-switch", expect_type=Switch).value
        updates_enabled = self.query_one("#update-switch", expect_type=Switch).value
        deletes_enabled = self.query_one("#delete-switch", expect_type=Switch).value

        log.write(
            self.gen_undo_sql(
                self.parsed_data["LOP_INSERT_ROWS"]["Usuarios"][0],
                "Usuarios",
                action="INSERT",
            )
        )

        # Format:
        # parsed_data -> Operation (CRUD) -> Table -> {schema, data (parsed data), transaction_id}
        if inserts_enabled:

            for table_name, operation_data in self.parsed_data[
                "LOP_INSERT_ROWS"
            ].items():

                idx = 0
                for row in operation_data:
                    print("idx", idx)
                    table.add_row(
                        "INSERT",
                        row["table"],
                        table_name,
                        row["username"],
                        row["begin_time"],
                        row["end_time"],
                        row["transaction_id"],
                        row["lsn"],
                        key=f"insert-{row['table']}-{idx}",
                    )
                    idx += 1

        if deletes_enabled:
            for table_name, operation_data in self.parsed_data[
                "LOP_DELETE_ROWS"
            ].items():

                idx = 0
                for row in operation_data:
                    table.add_row(
                        "DELETE",
                        row["table"],
                        table_name,
                        row["username"],
                        row["begin_time"],
                        row["end_time"],
                        row["transaction_id"],
                        row["lsn"],
                        key=f"delete-{row['table']}-{idx}",
                    )

                    idx += 1

    def on_tabs_tab_activated(self, event: Tabs.TabActivated) -> None:
        self.CURRENT_TAB = event.tab.id
        self.update_info()

    def update_info(self) -> None:
        log = self.query_one("#sql-log", expect_type=Log)

        if not self.LAST_CELL_EVENT or not self.CURRENT_TAB:
            return

        print("================ Tab")
        print(self.CURRENT_TAB)

        log.clear()
        if self.CURRENT_TAB == "tab-3":
            table = self.query_one("#transaction-table", expect_type=DataTable)
            row_key = self.LAST_CELL_EVENT.cell_key.row_key.value

            if not row_key:
                return

            operation = row_key.split("-")[0]
            idx = row_key.split("-")[2]

            table_name = table.get_row(row_key)[2]
            operation_dict = self.parsed_data["LOP_INSERT_ROWS"][table_name][int(idx)]

            log.write(
                self.gen_undo_sql(
                    operation_dict, operation_dict["table"], operation.upper()
                )
            )

        if self.CURRENT_TAB == "tab-4":

            table = self.query_one("#transaction-table", expect_type=DataTable)
            row_key = self.LAST_CELL_EVENT.cell_key.row_key.value

            if not row_key:
                return

            operation = row_key.split("-")[0]
            idx = row_key.split("-")[2]

            table_name = table.get_row(row_key)[2]
            operation_dict = self.parsed_data["LOP_INSERT_ROWS"][table_name][int(idx)]

            log.write(
                self.gen_redo_sql(
                    operation_dict, operation_dict["table"], operation.upper()
                )
            )

    # ===========================
    # app logic funcs
    # ===========================

    def gen_undo_sql(
        self,
        operation: dict[str, typing.Any],
        table_name: str,
        action: typing.Literal["INSERT", "UPDATE", "DELETE"],
    ) -> str:

        if action == "INSERT":

            where_clause = ""
            for column, value in operation["data"].items():
                # Determinar si el valor debe estar entre comillas simples
                if isinstance(value, str) or not isinstance(value, (int, float, bool)):
                    formatted_value = f"'{value}'"
                else:
                    formatted_value = value

                # Agregar condición al WHERE
                if where_clause:
                    where_clause += " AND "
                where_clause += f"{column} = {formatted_value}"

            return (
                f"DELETE FROM {operation['schema']}.{table_name} WHERE {where_clause};"
            )

        elif action == "DELETE":
            values_str = ", ".join(
                [f"{column} = {value}" for column, value in operation["data"].items()]
            )

            return (
                f"INSERT INTO {operation["schema"]}.{table_name} VALUES ({values_str});"
            )

        return ""

    def gen_redo_sql(
        self,
        operation: dict[str, typing.Any],
        table_name: str,
        action: typing.Literal["INSERT", "UPDATE", "DELETE"],
    ) -> str:
        if action == "INSERT":
            # Generar el SQL para volver a insertar
            columns = ", ".join(operation["data"].keys())
            values = ", ".join(
                [
                    (
                        f"'{value}'"
                        if isinstance(value, str)
                        or not isinstance(value, (int, float, bool))
                        else str(value)
                    )
                    for value in operation["data"].values()
                ]
            )
            return f"INSERT INTO {operation['schema']}.{table_name} ({columns}) VALUES ({values});"

        elif action == "DELETE":
            # Generar el SQL para volver a eliminar
            where_clause = ""
            for column, value in operation["data"].items():
                # Determinar si el valor debe estar entre comillas simples
                if isinstance(value, str) or not isinstance(value, (int, float, bool)):
                    formatted_value = f"'{value}'"
                else:
                    formatted_value = value

                # Agregar condición al WHERE
                if where_clause:
                    where_clause += " AND "
                where_clause += f"{column} = {formatted_value}"

            return (
                f"DELETE FROM {operation['schema']}.{table_name} WHERE {where_clause};"
            )

        elif action == "UPDATE":
            # Generar el SQL para rehacer un update
            set_clause = ", ".join(
                [
                    f"{column} = {'NULL' if value is None else (f"'{value}'" if isinstance(value, str) or not isinstance(value, (int, float, bool)) else value)}"
                    for column, value in operation["data"]["new"].items()
                ]
            )
            where_clause = " AND ".join(
                [
                    f"{column} = {'NULL' if value is None else (f"'{value}'" if isinstance(value, str) or not isinstance(value, (int, float, bool)) else value)}"
                    for column, value in operation["data"]["old"].items()
                ]
            )
            return f"UPDATE {operation['schema']}.{table_name} SET {set_clause} WHERE {where_clause};"

        return ""

    def on_data_table_cell_selected(self, event: DataTable.CellSelected):

        self.LAST_CELL_EVENT = event
        self.update_info()
