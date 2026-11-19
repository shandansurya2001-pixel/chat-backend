
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import or_, text
from datetime import datetime, timezone

from database import engine, SessionLocal
from models import Message, User

import os
import hmac
from fastapi import Header, HTTPException
from sqlalchemy import text

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Create tables without deleting existing messages
User.metadata.create_all(bind=engine)

# Add last_seen column to an existing users table

# Safely update existing database columns
with engine.begin() as connection:

    # Add last_seen to users table if missing
    user_columns = [
        row[1]
        for row in connection.exec_driver_sql(
            "PRAGMA table_info(users)"
        ).fetchall()
    ]

    if "last_seen" not in user_columns:
        connection.execute(
            text("ALTER TABLE users ADD COLUMN last_seen DATETIME")
        )

    # Add timestamp to messages table if missing
    message_columns = [
        row[1]
        for row in connection.exec_driver_sql(
            "PRAGMA table_info(messages)"
        ).fetchall()
    ]

    if "timestamp" not in message_columns:
        connection.execute(
            text("ALTER TABLE messages ADD COLUMN timestamp DATETIME")
        )



# Active users: username -> WebSocket
active_connections = {}


# Recover usernames from old messages
def recover_old_users():
    db = SessionLocal()

    try:
        messages = db.query(Message).all()

        names = set()

        for msg in messages:
            if msg.sender:
                names.add(msg.sender.strip())

            if msg.receiver:
                names.add(msg.receiver.strip())

        existing = {
            user.username
            for user in db.query(User).all()
        }

        for name in names:
            if name and name not in existing:
                db.add(User(username=name))

        db.commit()

    except Exception:
        db.rollback()
        raise

    finally:
        db.close()


recover_old_users()


# Get all registered users and their status
def get_user_status(username):
    db = SessionLocal()

    try:
        user = db.query(User).filter(
            User.username == username
        ).first()

        if not user:
            return None

        return {
            "username": user.username,
            "online": username in active_connections,
            "last_seen": (
                user.last_seen.isoformat() + "Z"
                if user.last_seen else None
            )
        }

    finally:
        db.close()


# Broadcast updated online/offline status
async def broadcast_user_list():
    for username, connection in list(active_connections.items()):
        try:
            db = SessionLocal()

            try:
                users = db.query(User).filter(
                    User.username != username
                ).all()

                user_list = [
                    {
                        "username": user.username,
                        "online": user.username in active_connections,
                        "last_seen": (
                            user.last_seen.isoformat() + "Z"
                            if user.last_seen else None
                        )
                    }
                    for user in users
                ]

            finally:
                db.close()

            await connection.send_json({
                "type": "user_list",
                "users": user_list
            })

        except Exception:
            pass


# WebSocket connection
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    username = None

    try:
        while True:
            data = await websocket.receive_json()

            # Register user
            if data.get("type") == "register":
                new_username = data.get("username", "").strip()

                if not new_username:
                    await websocket.send_json({
                        "type": "error",
                        "message": "Username cannot be empty"
                    })
                    continue

                username = new_username

                db = SessionLocal()

                try:
                    user = db.query(User).filter(
                        User.username == username
                    ).first()

                    if not user:
                        db.add(User(username=username))
                        db.commit()

                finally:
                    db.close()

                active_connections[username] = websocket

                await broadcast_user_list()

            # Send private message
            elif data.get("type") == "private_message":
                if not username:
                    continue

                # Use the registered username, not a client-supplied sender
                receiver = data.get("receiver", "").strip()
                message_text = data.get("text", "").strip()

                if not receiver or not message_text:
                    continue

                db = SessionLocal()

                try:
                    receiver_user = db.query(User).filter(
                        User.username == receiver
                    ).first()

                    if not receiver_user:
                        await websocket.send_json({
                            "type": "error",
                            "message": "User does not exist"
                        })
                        continue

                    new_message = Message(
                        sender=username,
                        receiver=receiver,
                        text=message_text
                    )

                    db.add(new_message)
                    db.commit()

                finally:
                    db.close()

                # Deliver to receiver if online
                if receiver in active_connections:
                    try:
                        await active_connections[receiver].send_json({
                            "type": "private_message",
                            "sender": username,
                            "receiver": receiver,
                            "text": message_text
                        })
                    except Exception:
                        pass

    except WebSocketDisconnect:
        pass

    finally:
        if username and active_connections.get(username) is websocket:
            del active_connections[username]

            db = SessionLocal()

            try:
                user = db.query(User).filter(
                    User.username == username
                ).first()

                if user:
                    user.last_seen = datetime.now(
                        timezone.utc
                    ).replace(tzinfo=None)

                    db.commit()

            finally:
                db.close()

            await broadcast_user_list()


# Get only people the current user has chatted with
@app.get("/contacts/{username}")
def get_contacts(username: str):
    db = SessionLocal()

    try:
        messages = db.query(Message).filter(
            or_(
                Message.sender == username,
                Message.receiver == username
            )
        ).all()

        contact_names = set()

        for msg in messages:
            if msg.sender != username:
                contact_names.add(msg.sender)

            if msg.receiver != username:
                contact_names.add(msg.receiver)

        users = db.query(User).filter(
            User.username.in_(contact_names)
        ).all() if contact_names else []

        return [
            {
                "username": user.username,
                "online": user.username in active_connections,
                "last_seen": (
                    user.last_seen.isoformat() + "Z"
                    if user.last_seen else None
                )
            }
            for user in users
        ]

    finally:
        db.close()


# Search registered users to start a new chat
@app.get("/users/search/{username}")
def search_users(username: str, q: str = ""):
    db = SessionLocal()

    try:
        query = db.query(User).filter(
            User.username != username
        )

        if q.strip():
            query = query.filter(
                User.username.ilike(f"%{q.strip()}%")
            )

        users = query.order_by(User.username).limit(30).all()

        return [
            {
                "username": user.username,
                "online": user.username in active_connections,
                "last_seen": (
                    user.last_seen.isoformat() + "Z"
                    if user.last_seen else None
                )
            }
            for user in users
        ]

    finally:
        db.close()


# Load previous conversation
@app.get("/messages/{user1}/{user2}")
def get_messages(user1: str, user2: str):
    db: Session = SessionLocal()

    try:
        messages = db.query(Message).filter(
            or_(
                (Message.sender == user1) &
                (Message.receiver == user2),

                (Message.sender == user2) &
                (Message.receiver == user1)
            )
        ).order_by(Message.id).all()

        return [
            {
                "id": msg.id,
                "sender": msg.sender,
                "receiver": msg.receiver,
                "text": msg.text
            }
            for msg in messages
        ]

    finally:
        db.close()

    
@app.post("/admin/reset-database")
def reset_database(x_reset_token: str = Header(default="")):
    expected_token = os.getenv("RESET_TOKEN")

    if not expected_token:
        raise HTTPException(
            status_code=503,
            detail="Reset token is not configured"
        )

    if not hmac.compare_digest(x_reset_token, expected_token):
        raise HTTPException(
            status_code=403,
            detail="Invalid reset token"
        )

    with engine.begin() as connection:
        connection.execute(text("DELETE FROM messages"))
        connection.execute(text("DELETE FROM users"))

    return {
        "message": "Live database cleared successfully!"
    }

