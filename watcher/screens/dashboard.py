from textual.screen import Screen
from textual.app import ComposeResult

from textual.widgets import Label, Header, Footer, DataTable, Tab, Switch
from textual.containers import Container, Vertical, Center


class Dashboard(Screen):
    CSS_PATH = "css/dashboard.tcss"

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
