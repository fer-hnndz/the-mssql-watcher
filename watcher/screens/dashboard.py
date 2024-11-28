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
                yield Switch(value=True)

                yield Label("UPDATE", id="update-switch-label")
                yield Switch(value=True)

                yield Label("DELETE", id="delete-switch-label")
                yield Switch(value=True)

        # Operation, Schema, Object, User, Begin Time, End Time, Transaction ID, LSN
        yield DataTable(id="transaction-table")

        # Tabs: Operation Details, Row History, Undo Script, Redo Script, Transaction Information

        # Operation Details: Field, Type, Old Value, New Value
        # Row History: Operation, Date, Username, LSN, [...Rows]
        # Undo Script: SQL Script
        # Redo Script: SQL Script
