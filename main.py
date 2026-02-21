from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from database import engine, SessionLocal
from models import Message
import json

app = FastAPI()

# Allow frontend connection
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Create DB tables
Message.metadata.create_all(bind=engine)

# Store active users
active_connections = {}


# ----------------------------
# Broadcast Online User List
# ----------------------------
async def broadcast_user_list():
    users = list(active_connections.keys())
    for connection in active_connections.values():
        await connection.send_json({
            "type": "user_list",
            "users": users
        })


# ----------------------------
# WebSocket Endpoint
# ----------------------------
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    username = None

    try:
        while True:
            data = await websocket.receive_json()

            # Register user
            if data["type"] == "register":
                username = data["username"]
                active_connections[username] = websocket
                await broadcast_user_list()

            # Private Message
            elif data["type"] == "private_message":
                db: Session = SessionLocal()

                # Save message to DB
                new_message = Message(
                    sender=data["sender"],
                    receiver=data["receiver"],
                    text=data["text"]
                )
                db.add(new_message)
                db.commit()
                db.close()

                # Send message to receiver if online
                receiver = data["receiver"]
                if receiver in active_connections:
                    await active_connections[receiver].send_json({
                        "type": "private_message",
                        "sender": data["sender"],
                        "text": data["text"]
                    })

    except WebSocketDisconnect:
        if username in active_connections:
            del active_connections[username]
            await broadcast_user_list()


# ----------------------------
# REST API - Load Chat History
# ----------------------------
@app.get("/messages/{user1}/{user2}")
def get_messages(user1: str, user2: str):
    db: Session = SessionLocal()

    messages = db.query(Message).filter(
        ((Message.sender == user1) & (Message.receiver == user2)) |
        ((Message.sender == user2) & (Message.receiver == user1))
    ).all()

    db.close()

    return messages