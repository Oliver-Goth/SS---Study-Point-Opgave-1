import uuid
import threading, queue
from dataclasses import dataclass, is_dataclass
from typing import List, Dict
from collections import defaultdict

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

@dataclass(frozen=True)
class ProgramAdded(Event):
    program_id: str
    title: str
    tags: List[str]
    year: int
    genre: str

    @staticmethod
    def create(program_id, title, tags, year, genre):
        return ProgramAdded("program.added", program_id, title, tags, year, genre)

@dataclass(frozen=True)
class ProgramRated(Event):
    user_id: str
    program_id: str
    stars: int
    @staticmethod
    def create(user_id, program_id, stars):
        return ProgramRated("program.rated", user_id, program_id, stars)

@dataclass(frozen=True)
class ProgramSearched(Event):
    user_id: str
    query: str
    @staticmethod
    def create(user_id, query):
        return ProgramSearched("program.searched", user_id, query)

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
            raise TypeError("event must be dataclass")
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
        self.programs: Dict[str, dict] = {}
        self.ratings_by_program = defaultdict(list)
        self.search_log: List[dict] = []
        self.metrics = defaultdict(int)
        
# ========= Users Module =========
class UsersModule:
    def __init__(self, bus, store):
        self.bus, self.store = bus, store

    def register(self, username, email):
        uid = str(uuid.uuid4())
        self.store.users[uid] = {"user_id": uid, "username": username, "email": email}
        self.bus.publish(UserRegistered.create(uid, username, email))
        return uid
    
class CatalogModule:
    def __init__(self, bus, store):
        self.bus, self.store = bus, store

    def add_program(self, title, tags, year, genre):
        pid = str(uuid.uuid4())
        self.store.programs[pid] = {
            "program_id": pid,
            "title": title,
            "tags": [t.lower() for t in tags],
            "year": year,
            "genre": genre.lower()
        }
        self.bus.publish(ProgramAdded.create(pid, title, tags, year, genre))
        return pid

class RatingsModule:
    def __init__(self, bus, store):
        self.bus, self.store = bus, store
    def rate(self, user_id, program_id, stars):
        stars = max(1, min(5, int(stars)))
        self.store.ratings_by_program[program_id].append((user_id, stars))
        self.bus.publish(ProgramRated.create(user_id, program_id, stars))
    def get_program_average(self, program_id):
        ratings = self.store.ratings_by_program.get(program_id, [])
        return sum(s for _, s in ratings) / len(ratings) if ratings else 0.0
    
class SearchModule:
    def __init__(self, bus, store):
        self.bus, self.store = bus, store

    def search(self, user_id, query=None, year=None, genre=None):
        results = list(self.store.programs.values())

        if query:
            q = query.lower()
            results = [p for p in results if q in p["title"].lower() or any(q in t for t in p["tags"])]

        if year:
            results = [p for p in results if p.get("year") == year]

        if genre:
            results = [p for p in results if p.get("genre") == genre.lower()]

        self.bus.publish(ProgramSearched.create(user_id, query or ""))
        return results

# ========= App Composition =========
class App:
    def __init__(self):
        self.bus = EventBus()
        self.store = InMemoryStore()
        self.users = UsersModule(self.bus, self.store)
        self.catalog = CatalogModule(self.bus, self.store)
        self.ratings = RatingsModule(self.bus, self.store)
        self.search = SearchModule(self.bus, self.store)
    def shutdown(self):
        self.bus.stop()

# ========= Demo =========
if __name__ == "__main__":
    app = App()

    p1 = app.catalog.add_program("Matador", ["drama","classic","danish"], 1978, "historical")
    p2 = app.catalog.add_program("Borgen", ["politics","drama","danish"], 2010, "political drama")
    p3 = app.catalog.add_program("Forbrydelsen", ["crime","thriller","danish"], 2007, "scandinavian noir")
    p4 = app.catalog.add_program("Rejseholdet", ["crime","classic"], 2000, "drama")
    p5 = app.catalog.add_program("Arvingerne", ["drama"], 2014, "drama")

    u1 = app.users.register("anna", "anna@mail.com")
    u2 = app.users.register("bo", "bo@mail.com")

    app.ratings.rate(u1,p1,5); app.ratings.rate(u1,p2,4); app.ratings.rate(u1,p3,3); app.ratings.rate(u1,p4,4); app.ratings.rate(u1,p5,2)
    app.ratings.rate(u2,p1,5); app.ratings.rate(u2,p2,5); app.ratings.rate(u2,p3,4); app.ratings.rate(u2,p4,3); app.ratings.rate(u2,p5,3)

    results = app.search.search(u1,"drama")
    print("Search 'drama':",[r["title"] for r in results])

    print("Registered users:")
    for uid, u in app.store.users.items():
        print(f"  {u['username']} ({u['email']}) -> {uid}")

    print("\nAverage ratings:")
    for pid, p in app.store.programs.items():
        print(f"  {p['title']}: {app.ratings.get_program_average(pid):.2f}")

    app.shutdown()
