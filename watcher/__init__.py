from textual.app import App, ComposeResult
from textual.widgets import Label, Header, Footer


class WatcherApp(App):
    def compose(self) -> ComposeResult:
        yield Header()
        yield Footer()
        yield Label("Hello, World!")
