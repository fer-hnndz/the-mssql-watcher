from typing import Optional

import pymssql
from textual.app import ComposeResult
from textual.containers import Center, Horizontal
from textual.screen import Screen
from textual.types import SelectType
from textual.widgets import (
    Button,
    Footer,
    Header,
    Input,
    Label,
    RadioButton,
    RadioSet,
    Select,
)
from textual.worker import Worker, WorkerState

from ..parser import Parser
from .dashboard import Dashboard


class AuthScreen(Screen):
    CSS_PATH = "css/auth.tcss"
    CURSOR: Optional[pymssql.Cursor] = None
    DATABASE: Optional[str] = None

    def compose(self) -> ComposeResult:
        self.app.sub_title = "Auth Screen"
        yield Header()
        yield Footer()

        with Center(id="main"):
            yield Label("Server", id="server-label")
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

            yield Label("Database", id="database-label")
            yield Input(id="database-input", placeholder="master")

            with Center():
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

        server_data_input = self.query_one("#server-input", expect_type=Input)
        auth_input = self.query_one("#auth-select", expect_type=Select)
        username_input = self.query_one("#username-input", expect_type=Input)
        password_input = self.query_one("#password-input", expect_type=Input)
        database_input = self.query_one("#database-input", expect_type=Input)

        server_data = server_data_input.value
        auth = auth_input.value
        username = username_input.value
        password = password_input.value
        database = database_input.value

        # TODO: Implement Date logic and Windows Auth

        host = server_data.split(",")[0]
        port = server_data.split(",")[1]

        main_container = self.query_one("#main", expect_type=Center)
        main_container.loading = True

        self.run_worker(
            self.try_connect(
                host, port, username, password, database, windows_auth=(auth == "win")
            ),
            exclusive=True,
        )

    async def try_connect(
        self,
        host: str,
        port: str,
        username: str,
        password: str,
        database: str,
        windows_auth: bool = True,
    ) -> None:
        try:
            self.DATABASE = database
            conn = None

            if windows_auth:
                conn = pymssql.connect(server=host, autocommit=True)
            else:

                conn = pymssql.connect(
                    server=host,
                    port=port,
                    user=username,
                    password=password,
                )

            self.CURSOR = conn.cursor()
        except Exception as e:
            self.notify(f"Error: {e}")

    def on_worker_state_changed(self, event: Worker.StateChanged) -> None:
        """Worker state changed event handler."""

        if event.state == WorkerState.SUCCESS:
            if not self.CURSOR or not self.DATABASE:
                return

            main_container = self.query_one("#main", expect_type=Center)
            main_container.loading = False

            self.notify("Connected!")

            p = Parser(self.CURSOR, database=self.DATABASE)
            self.app.push_screen(
                Dashboard(
                    parsed_data=p.parse_online_transaction_log(),
                )
            )
