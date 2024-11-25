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
import pymssql
from ..parser import Parser

from .dashboard import Dashboard


class AuthScreen(Screen):
    CSS_PATH = "css/auth.tcss"
    CURSOR: Optional[pymssql.Cursor] = None

    def compose(self) -> ComposeResult:
        self.app.sub_title = "Auth Screen"
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

            yield Button("Conectar", id="connect-button", variant="primary")

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
        conn = pymssql.connect(
            server=host,
            port=port,
            user=username,
            password=password,
        )

        self.notify("Connected!")
        p = Parser(conn.cursor())

        p.parse_online_transaction_log()
        self.app.push_screen(Dashboard())
