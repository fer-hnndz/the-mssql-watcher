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

import pymssql
import traceback


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

    def on_button_pressed(self, event: Button.Pressed) -> None:
        """Button pressed event handler."""

        server_data = self.query_one("#server-input", expect_type=Input).value
        auth = self.query_one("#auth-select", expect_type=Select).value
        username = self.query_one("#username-input", expect_type=Input).value
        password = self.query_one("#password-input", expect_type=Input).value

        # TODO: Implement Date logic and Windows Auth

        host = server_data.split(",")[0]
        port = server_data.split(",")[1]

        # TODO: Database selector
        with pymssql.connect(
            server=host,
            port=port,
            user=username,
            password=password,
        ) as conn:
            self.notify("Connected!")
            cursor = conn.cursor()

            # TODO: Usar base de datos que quiera usar;
            cursor.execute("USE sachen;")
            cursor.execute(
                """SELECT 
    Operation,
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

            for row in cursor.fetchall():
                transaction_id = row[0]
                operation = row[1]
                alloc_unit = row[2]
                raw_data = row[3]

                if raw_data:  # Decodificar contenido hexadecimal si no es None
                    try:
                        # Asegúrate de que raw_data es un string hexadecimal
                        decoded = raw_data.decode(
                            "utf-8", errors="ignore"
                        )  # Decodifica a UTF-8

                        print(
                            f"Transacción: {transaction_id}, Operación: {operation}, Tabla: {alloc_unit}, Datos: {decoded}"
                        )

                    except Exception as e:
                        print(f"Error al decodificar: {e}")

            self.notify("Done")
