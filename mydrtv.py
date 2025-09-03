import uuid
import threading, queue
from dataclasses import dataclass, is_dataclass
from typing import List, Dict

# ========= Events =========
@dataclass(frozen=True)
class Event:
    name: str

@dataclass(frozen=True)
class UserRegistered(Event):
    user_id: str
    username: str
    email: str

    @staticmethod
    def create(user_id, username, email):
        return UserRegistered("user.registered", user_id, username, email)

# ========= EventBus =========
class EventBus:
    def __init__(self):
        self._handlers: Dict[str, List] = {}
        self._q: "queue.Queue[Event]" = queue.Queue()
        self._stop = threading.Event()
        self._worker = threading.Thread(target=self._run, daemon=True)
        self._worker.start()

    def subscribe(self, event_name: str, handler):
        self._handlers.setdefault(event_name, []).append(handler)

    def publish(self, event: Event):
        if not is_dataclass(event):
            raise TypeError("event must be a dataclass instance")
        self._q.put(event)

    def _run(self):
        while not self._stop.is_set():
            try:
                event = self._q.get(timeout=0.1)
            except queue.Empty:
                continue
            for handler in self._handlers.get(event.name, []):
                try:
                    handler(event)
                except Exception as ex:
                    print(f"[EventBus] Error in {event.name}: {ex}")
            self._q.task_done()

    def stop(self):
        self._stop.set()
        self._worker.join(timeout=1)

# ========= In-Memory Store =========
class InMemoryStore:
    def __init__(self):
        self.users: Dict[str, dict] = {}

# ========= Users Module =========
class UsersModule:
    def __init__(self, bus, store):
        self.bus, self.store = bus, store

    def register(self, username, email):
        uid = str(uuid.uuid4())
        self.store.users[uid] = {"user_id": uid, "username": username, "email": email}
        self.bus.publish(UserRegistered.create(uid, username, email))
        return uid

# ========= App Composition =========
class App:
    def __init__(self):
        self.bus = EventBus()
        self.store = InMemoryStore()
        self.users = UsersModule(self.bus, self.store)

    def shutdown(self):
        self.bus.stop()

# ========= Demo =========
if __name__ == "__main__":
    app = App()

    # Register users
    u1 = app.users.register("anna", "anna@example.com")
    u2 = app.users.register("bo", "bo@example.com")

    print("Registered users:")
    for uid, u in app.store.users.items():
        print(f"  {u['username']} ({u['email']}) -> {uid}")

    app.shutdown()
