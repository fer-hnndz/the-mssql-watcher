import typing

from textual.app import ComposeResult
from textual.containers import Center, Container, Vertical
from textual.screen import Screen
from textual.widgets import DataTable, Footer, Header, Label, Switch, Tab


class Dashboard(Screen):
    CSS_PATH = "css/dashboard.tcss"

    def __init__(self, parsed_data: dict[str, typing.Any]):
        super().__init__()
        self.app.sub_title = "Dashboard"
        self.parsed_data = parsed_data

        print(self.parsed_data)

    def compose(self) -> ComposeResult:
        with Container():
            with Vertical():
                yield Label("INSERT", id="insert-switch-label")
                yield Switch(value=True, id="insert-switch")

                yield Label("UPDATE", id="update-switch-label")
                yield Switch(value=True, id="update-switch")

                yield Label("DELETE", id="delete-switch-label")
                yield Switch(value=True, id="delete-switch")

        # Operation, Schema, Object, User, Begin Time, End Time, Transaction ID, LSN
        yield DataTable(id="transaction-table")

        # Tabs: Operation Details, Row History, Undo Script, Redo Script, Transaction Information

        # Operation Details: Field, Type, Old Value, New Value
        # Row History: Operation, Date, Username, LSN, [...Rows]
        # Undo Script: SQL Script
        # Redo Script: SQL Script

    # * on post_mount?
    def on_mount(self) -> None:
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

        # Format:
        # parsed_data -> Operation (CRUD) -> Table -> {schema, data (parsed data), transaction_id}
        if inserts_enabled:
            for table_name, operation_data in self.parsed_data[
                "LOP_INSERT_ROWS"
            ].items():

                for row in operation_data:
                    table.add_row(
                        "INSERT", row["schema"], table_name, "-", "-", "-", "-", "/"
                    )
