@app.on_event("startup")
async def startup_event():
    try:
        print("Initialisiere Confluence Smart-Money Engine v4.1...")
        # Starte Hintergrund-Task oder DB-Verbindung sicher mit Try-Catch
        # Hier den betroffenen Startup-Code einklinken
        print("Engine erfolgreich gestartet.")
    except Exception as e:
        print(f"Fehler im Startup-Event (wird abgefangen): {e}")
