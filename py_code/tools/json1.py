import json
from pathlib import Path

movies = [{
    "id": 1,
    "title": "aclive",
    "year": 1999
}, {
    "id": 2,
    "title": "Mpnkey",
    "year": 1929
}]
data = json.dumps(movies)
Path("movies.json").write_text(data)

# reverse
data1 = Path("movies.json").read_text()
movies = json.loads(data)
