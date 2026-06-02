from waluigi.console.db.base import _set_engine
from waluigi.console.db.engine import create_console_engine


class ConsoleDB:
    """Registry: initialises one shared engine and exposes typed repositories."""

    def __init__(self, url: str):
        from waluigi.console.repositories.user_repo import UserRepository

        engine = create_console_engine(url)
        _set_engine(engine)

        self.users = UserRepository(engine)
