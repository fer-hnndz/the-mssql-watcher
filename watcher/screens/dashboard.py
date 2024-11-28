import typing

from textual.app import ComposeResult
from textual.containers import Center, Container, Vertical
from textual.screen import Screen
from textual.widgets import DataTable, Footer, Header, Label, Switch, Tabs, Tab, Log


class Dashboard(Screen):
    CSS_PATH = "css/dashboard.tcss"

    def __init__(self, parsed_data: dict[str, typing.Any]):
        super().__init__()
        self.app.sub_title = "Dashboard"
        self.parsed_data = parsed_data

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
        yield DataTable(id="transaction-table")

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
                        "-",
                        "-",
                        "-",
                        "-",
                        "/",
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
                        "-",
                        "-",
                        "-",
                        "-",
                        "/",
                        key=f"delete-{row['table']}-{idx}",
                    )

                    idx += 1

    def on_tabs_tabactivated(self, tabs: Tabs, tab: Tab) -> None:
        container = self.query_one(
            "#operation-details-container", expect_type=Container
        )
        container.remove_children("*")
        print(tab)

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

    def on_data_table_cell_selected(self, event: DataTable.CellSelected):

        # Find row key
        table = self.query_one("#transaction-table", expect_type=DataTable)
        row_key = event.cell_key.row_key.value

        if not row_key:
            return

        table_name = table.get_row(row_key)[2]

        log = self.query_one("#sql-log", expect_type=Log)
        operation = row_key.split("-")[0]
        idx = row_key.split("-")[2]

        if operation == "insert":
            log.clear()
            operation_dict = self.parsed_data["LOP_INSERT_ROWS"][table_name][int(idx)]
            log.write(
                self.gen_undo_sql(operation_dict, operation_dict["table"], "INSERT")
            )

        elif operation == "delete":
            log.clear()
            operation_dict = self.parsed_data["LOP_DELETE_ROWS"][table_name][int(idx)]
            log.write(
                self.gen_undo_sql(operation_dict, operation_dict["table"], "DELETE")
            )
