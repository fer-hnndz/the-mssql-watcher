from textual.app import App, ComposeResult
from textual.widgets import Footer, Header, Label, LoadingIndicator

from .screens.auth import AuthScreen


class WatcherApp(App):
    def on_mount(self) -> None:
        self.app.title = "The Microsoft SQL Server Watcher"
        self.push_screen(AuthScreen())
