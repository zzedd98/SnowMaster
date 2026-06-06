import json
import time
import urllib.request

# --- Données en dur (format réaliste) ---
TITLE = "test"  # = serveur / masterTitle

# Chaque entrée : (id, alias/description, timestamp optionnel)
SUBCONTROLLERS = [
    ("TS1", "Comte Harebourg"),           # comme PL_Source.lua
    ("TS2", "Chasse OSAVORA"),              # alias d'un compte TS
]

# --- Construction du payload (identique au Lua) ---
now = int(time.time())
subcontrollers = {
    sub_id: {
        "alias": alias,
        "timestamp": now - (i * 1600),  # timestamps légèrement décalés, plus réaliste
    }
    for i, (sub_id, alias) in enumerate(SUBCONTROLLERS)
}

payload = {
    "title": TITLE,
    "subcontrollers": subcontrollers,
}

# --- Envoi ---
url = "http://127.0.0.1:8787/heartbeat"
data = json.dumps(payload, ensure_ascii=False).encode("utf-8")

req = urllib.request.Request(
    url,
    data=data,
    headers={"Content-Type": "application/json"},
    method="POST",
)

with urllib.request.urlopen(req) as resp:
    print(f"Status: {resp.status}")
    print(resp.read().decode("utf-8"))

print("\nPayload envoyé :")
print(json.dumps(payload, indent=2, ensure_ascii=False))