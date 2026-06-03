import getpass

from waluigi.cli.services.session import WaluigiSession


def login(session: WaluigiSession, username: str, password: str | None = None) -> None:
    if not password:
        password = getpass.getpass(f"Password for '{username}': ")
    try:
        r = session.http.post("/auth/login", json={"username": username, "password": password})
        if r.status_code == 200:
            data  = r.json()
            token = data.get("token")
            if token:
                session.save_token(token)
                ns     = data.get("namespaces", [])
                ns_str = "*" if ns == "*" else ", ".join(ns) if ns else "(none)"
                print(f"Login successful. Namespaces: {ns_str}")
            else:
                print("Error: no token received from server.")
        else:
            print(f"Unauthorized: {r.status_code}")
    except Exception as e:
        print(f"Error: {e}")


def logout(session: WaluigiSession) -> None:
    if session.delete_token():
        print("Logout successful. Token removed.")
    else:
        print("No active session found.")
