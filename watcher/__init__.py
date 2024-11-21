from textual.app import App, ComposeResult
from textual.widgets import Label, Header, Footer, LoadingIndicator

from .screens.auth import AuthScreen


class WatcherApp(App):
    def on_mount(self) -> None:
        self.push_screen(AuthScreen())
