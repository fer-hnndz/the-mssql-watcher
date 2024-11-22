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


class AuthScreen(Screen):
    CSS_PATH = "css/auth.tcss"

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
