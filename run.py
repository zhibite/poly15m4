import os

import uvicorn
from dotenv import load_dotenv

load_dotenv()

if __name__ == "__main__":
    # Secure default: only listen on loopback for local runs.
    # Containers must set HOST=0.0.0.0 (Dockerfile/docker-compose already do).
    host = os.getenv("HOST", "127.0.0.1")
    port = int(os.getenv("PORT", "8019"))
    uvicorn.run("app.main:app", host=host, port=port)
