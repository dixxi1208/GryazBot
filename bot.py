# bot.py
import os
import re
import json
import math
import time
import sqlite3
import logging
from contextlib import closing
from typing import Optional

from telegram import (
    Update,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    ChatMemberUpdated,
    Chat,
    MessageEntity,
)
from telegram.constants import ChatType, ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ChatMemberHandler,
    CallbackQueryHandler,
    filters,
)

# ---------- Config / Logging ----------
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

DB_PATH = os.environ.get("DB_PATH", "gryaz.db")
TARGET_COOLDOWN = int(os.environ.get("TARGET_COOLDOWN", "300"))  # 5 min default
VOTE_TIMEOUT = int(os.environ.get("VOTE_TIMEOUT", "600"))        # 10 min default, 0 = disabled
INIT_SCORES_RAW = os.environ.get("INIT_SCORES", "").strip()

def parse_init_scores(raw: str):
    mapping = {}
    if not raw:
        return mapping
    # Try JSON first
    try:
        data = json.loads(raw)
        if isinstance(data, dict):
            for k, v in data.items():
                if isinstance(v, int):
                    mapping[str(k).lstrip("@").strip().lower()] = v
            return mapping
        if isinstance(data, list):
            for item in data:
                if not isinstance(item, dict):
                    continue
                pts = item.get("points")
                if isinstance(pts, int):
                    key = (item.get("username") or item.get("name") or "").lstrip("@").strip().lower()
                    if key:
                        mapping[key] = pts
            return mapping
    except Exception:
        pass
    # CSV fallback: "name:points, @user:points"
    for part in re.split(r"\s*,\s*", raw):
        if ":" not in part:
            continue
        k, v = part.split(":", 1)
        try:
            points = int(v.strip())
        except ValueError:
            continue
        mapping[k.lstrip("@").strip().lower()] = points
    return mapping

INIT_SCORES_MAP = parse_init_scores(INIT_SCORES_RAW)
if INIT_SCORES_MAP:
    log.info("INIT_SCORES loaded for %d keys", len(INIT_SCORES_MAP))

# ---------- DB helpers ----------
def db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with closing(db()) as conn, conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS members(
              chat_id    INTEGER NOT NULL,
              user_id    INTEGER NOT NULL,
              username   TEXT,
              first_name TEXT,
              is_bot     INTEGER NOT NULL DEFAULT 0,
              PRIMARY KEY(chat_id, user_id)
            );

            CREATE TABLE IF NOT EXISTS scores(
              chat_id INTEGER NOT NULL,
              user_id INTEGER NOT NULL,
              score   INTEGER NOT NULL DEFAULT 0,
              PRIMARY KEY(chat_id, user_id)
            );

            CREATE TABLE IF NOT EXISTS polls(
              id             INTEGER PRIMARY KEY AUTOINCREMENT,
              chat_id        INTEGER NOT NULL,
              message_id     INTEGER NOT NULL,
              target_user_id INTEGER NOT NULL,
              plus_count     INTEGER NOT NULL DEFAULT 0,
              minus_count    INTEGER NOT NULL DEFAULT 0,
              closed         INTEGER NOT NULL DEFAULT 0,
              expired        INTEGER NOT NULL DEFAULT 0,
              created_at     INTEGER NOT NULL DEFAULT (strftime('%s','now'))
            );

            CREATE TABLE IF NOT EXISTS poll_votes(
              poll_id       INTEGER NOT NULL,
              voter_user_id INTEGER NOT NULL,
              vote          TEXT NOT NULL CHECK(vote IN ('plus','minus')),
              PRIMARY KEY(poll_id, voter_user_id),
              FOREIGN KEY(poll_id) REFERENCES polls(id) ON DELETE CASCADE
            );
            """
        )

def upsert_member(chat: Chat, user) -> None:
    with closing(db()) as conn, conn:
        conn.execute(
            """
            INSERT INTO members(chat_id, user_id, username, first_name, is_bot)
            VALUES(?,?,?,?,?)
            ON CONFLICT(chat_id, user_id) DO UPDATE SET
              username   = excluded.username,
              first_name = excluded.first_name,
              is_bot     = excluded.is_bot
            """,
            (chat.id, user.id, user.username or "", user.first_name or "", 1 if user.is_bot else 0),
        )

def non_bot_member_count(chat_id: int) -> int:
    with closing(db()) as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS c FROM members WHERE chat_id=? AND is_bot=0",
            (chat_id,),
        ).fetchone()
        return int(row["c"] or 0)

def maybe_apply_init_score(chat_id: int, user) -> None:
    if not INIT_SCORES_MAP:
        return
    uname = (user.username or "").lstrip("@").strip().lower()
    fname = (user.first_name or "").strip().lower()
    for key in {uname, fname}:
        if key and key in INIT_SCORES_MAP:
            pts = INIT_SCORES_MAP[key]
            with closing(db()) as conn, conn:
                conn.execute(
                    """
                    INSERT INTO scores(chat_id, user_id, score)
                    VALUES(?,?,?)
                    ON CONFLICT(chat_id, user_id) DO UPDATE SET score=excluded.score
                    """,
                    (chat_id, user.id, pts),
                )
            log.info("Seeded score for user_id=%s in chat_id=%s to %s via key=%s",
                     user.id, chat_id, pts, key)
            break

# ---------- Utils ----------
async def ensure_group(update: Update) -> Optional[Chat]:
    if not update.effective_chat or update.effective_chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        if update.effective_message:
            await update.effective_message.reply_text("This command works only in group chats.")
        return None
    return update.effective_chat

def resolve_target_user(update: Update):
    msg = update.effective_message
    if msg and msg.reply_to_message and msg.reply_to_message.from_user:
        return msg.reply_to_message.from_user
    if msg and msg.entities:
        for ent in msg.entities:
            if ent.type == MessageEntity.MENTION:
                username = msg.text[ent.offset: ent.offset + ent.length].lstrip("@")
                with closing(db()) as conn:
                    row = conn.execute(
                        "SELECT user_id FROM members WHERE chat_id=? AND lower(username)=?",
                        (msg.chat_id, username.lower()),
                    ).fetchone()
                    if row:
                        return row["user_id"]
    return None

def format_time_left(seconds: int) -> str:
    m, s = divmod(seconds, 60)
    return f"{m}m {s}s" if m else f"{s}s"

# ---------- Core: start a vote ----------
async def start_vote_for_target(update: Update, context, chat: Chat, target_user):
    if update.effective_user:
        upsert_member(chat, update.effective_user)
        maybe_apply_init_score(chat.id, update.effective_user)
    upsert_member(chat, target_user)
    maybe_apply_init_score(chat.id, target_user)

    now = int(time.time())
    with closing(db()) as conn:
        # Block if there's an open poll already
        open_poll = conn.execute(
            "SELECT 1 FROM polls WHERE chat_id=? AND target_user_id=? AND closed=0 LIMIT 1",
            (chat.id, target_user.id),
        ).fetchone()
        if open_poll:
            await update.effective_message.reply_text(
                f"🗳️ A vote for {target_user.first_name} is already in progress."
            )
            return
        last = conn.execute(
            "SELECT created_at, expired FROM polls WHERE chat_id=? AND target_user_id=? ORDER BY created_at DESC LIMIT 1",
            (chat.id, target_user.id),
        ).fetchone()

    # Cooldown only if last poll was not expired
    if last and not last["expired"]:
        elapsed = now - int(last["created_at"])
        if elapsed < TARGET_COOLDOWN:
            await update.effective_message.reply_text(
                f"⏳ A vote for {target_user.first_name} happened recently. "
                f"Try again in {format_time_left(TARGET_COOLDOWN - elapsed)}."
            )
            return

    with closing(db()) as conn, conn:
        cur = conn.execute(
            "INSERT INTO polls(chat_id, message_id, target_user_id, plus_count, minus_count, closed, expired, created_at) "
            "VALUES(?,?,?,?,?,?,?,?)",
            (chat.id, 0, target_user.id, 0, 0, 0, 0, now),
        )
        poll_id = cur.lastrowid

    kb = InlineKeyboardMarkup(
        [[
            InlineKeyboardButton("👍 +", callback_data=f"vote:{poll_id}:plus"),
            InlineKeyboardButton("👎 –", callback_data=f"vote:{poll_id}:minus"),
        ]]
    )
    threshold = max(1, math.ceil(non_bot_member_count(chat.id) / 2))
    m = await update.effective_message.reply_text(
        f"*Gryaz vote started* for [{target_user.first_name}](tg://user?id={target_user.id}).\n"
        f"Need *{threshold}* 👍 or 👎 to decide.",
        reply_markup=kb,
        parse_mode=ParseMode.MARKDOWN,
    )
    with closing(db()) as conn, conn:
        conn.execute("UPDATE polls SET message_id=? WHERE id=?", (m.message_id, poll_id))

# ---------- Handlers ----------
async def cmd_start(update: Update, _):
    text = (
        "Hi! I’m a *gryaz* counter.\n\n"
        "• Reply with /gryaz or 🐗/💊 to start a vote\n"
        f"• Target cooldown: {TARGET_COOLDOWN//60}m\n"
        f"• Vote timeout: {'disabled' if VOTE_TIMEOUT<=0 else str(VOTE_TIMEOUT//60)+'m'}\n"
        "• One vote per person; half 👍 passes (+1), half 👎 cancels\n"
        "• /stats shows leaderboard (leaders 💊)"
    )
    await update.effective_message.reply_text(text, parse_mode=ParseMode.MARKDOWN)

async def cmd_stats(update: Update, _):
    chat = await ensure_group(update)
    if not chat:
        return
    with closing(db()) as conn:
        rows = conn.execute(
            """SELECT m.user_id, COALESCE(m.first_name,'') as first_name,
                      COALESCE(m.username,'') as username,
                      COALESCE(s.score,0) as score
               FROM members m
               LEFT JOIN scores s ON s.chat_id=m.chat_id AND s.user_id=m.user_id
              WHERE m.chat_id=? AND m.is_bot=0
              ORDER BY score DESC, first_name ASC""",
            (chat.id,),
        ).fetchall()
    if not rows:
        await update.effective_message.reply_text("No members tracked yet.")
        return
    top = rows[0]["score"]
    lines = ["*Gryaz stats:*"]
    for r in rows:
        name = f"[{r['first_name']}](tg://user?id={r['user_id']})"
        if r["username"]:
            name += f" (@{r['username']})"
        pill = " 💊" if r["score"] == top and top > 0 else ""
        lines.append(f"{name}: *{r['score']}*{pill}")
    await update.effective_message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN)

async def cmd_gryaz(update: Update, context):
    chat = await ensure_group(update)
    if not chat:
        return
    target = resolve_target_user(update)
    if isinstance(target, int):
        try:
            cm = await context.bot.get_chat_member(chat.id, target)
            target = cm.user
        except Exception:
            target = None
    if not target or target.is_bot:
        return
    await start_vote_for_target(update, context, chat, target)

async def on_emoji_trigger(update: Update, context):
    chat = await ensure_group(update)
    if not chat:
        return
    msg = update.effective_message
    if not (msg and msg.reply_to_message and msg.text):
        return
    target_user = msg.reply_to_message.from_user
    if not target_user or target_user.is_bot:
        return
    await start_vote_for_target(update, context, chat, target_user)

async def on_vote(update: Update, context):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    try:
        _, poll_id_str, vote = q.data.split(":")
        poll_id = int(poll_id_str)
        assert vote in ("plus", "minus")
    except Exception:
        return

    with closing(db()) as conn:
        poll = conn.execute("SELECT * FROM polls WHERE id=?", (poll_id,)).fetchone()
    if not poll or poll["closed"]:
        return

    now = int(time.time())
    # Expire if timed out
    if VOTE_TIMEOUT > 0 and now - int(poll["created_at"]) >= VOTE_TIMEOUT:
        with closing(db()) as conn, conn:
            conn.execute("UPDATE polls SET closed=1, expired=1 WHERE id=?", (poll_id,))
        try:
            await q.edit_message_text("⌛ *Vote expired*", parse_mode=ParseMode.MARKDOWN)
        except Exception:
            pass
        return

    # One vote only
    try:
        with closing(db()) as conn, conn:
            conn.execute("INSERT INTO poll_votes VALUES(?,?,?)", (poll_id, q.from_user.id, vote))
    except sqlite3.IntegrityError:
        await q.answer("You already voted!", show_alert=True)
        return

    with closing(db()) as conn:
        row = conn.execute(
            "SELECT SUM(vote='plus'), SUM(vote='minus') FROM poll_votes WHERE poll_id=?",
            (poll_id,),
        ).fetchone()
        plus, minus = int(row[0] or 0), int(row[1] or 0)
        conn.execute("UPDATE polls SET plus_count=?, minus_count=? WHERE id=?", (plus, minus, poll_id))

    threshold = max(1, math.ceil(non_bot_member_count(q.message.chat.id) / 2))
    if plus >= threshold:
        with closing(db()) as conn, conn:
            conn.execute("UPDATE polls SET closed=1 WHERE id=?", (poll_id,))
            conn.execute(
                "INSERT INTO scores VALUES(?,?,1) "
                "ON CONFLICT(chat_id,user_id) DO UPDATE SET score=score+1",
                (q.message.chat.id, poll["target_user_id"]),
            )
        await q.edit_message_text(
            f"✅ *Gryaz vote passed* (👍{plus} / 👎{minus}, need {threshold})\nScore updated: +1.",
            parse_mode=ParseMode.MARKDOWN,
        )
    elif minus >= threshold:
        with closing(db()) as conn, conn:
            conn.execute("UPDATE polls SET closed=1 WHERE id=?", (poll_id,))
        await q.edit_message_text(
            f"❌ *Gryaz vote cancelled* (👍{plus} / 👎{minus}, need {threshold})",
            parse_mode=ParseMode.MARKDOWN,
        )
    else:
        try:
            await q.edit_message_text(
                f"*Vote in progress...* 👍{plus}/👎{minus} (need {threshold})",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=q.message.reply_markup,
            )
        except Exception:
            pass

async def on_message(update: Update, _):
    if not update.effective_chat or update.effective_chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        return
    if update.effective_user:
        upsert_member(update.effective_chat, update.effective_user)
        maybe_apply_init_score(update.effective_chat.id, update.effective_user)
    if update.effective_message and update.effective_message.reply_to_message and update.effective_message.reply_to_message.from_user:
        upsert_member(update.effective_chat, update.effective_message.reply_to_message.from_user)
        maybe_apply_init_score(update.effective_chat.id, update.effective_message.reply_to_message.from_user)

async def on_chat_member(update: Update, _):
    cmu: ChatMemberUpdated = update.chat_member
    chat = cmu.chat
    user = cmu.new_chat_member.user
    upsert_member(chat, user)
    maybe_apply_init_score(chat.id, user)

# ---------- Background job: auto-expire polls ----------
async def expire_polls(context):
    """Periodically close and mark expired polls, and edit their messages."""
    if VOTE_TIMEOUT <= 0:
        return
    now = int(time.time())
    # Select polls that are open and past timeout
    with closing(db()) as conn:
        rows = conn.execute(
            "SELECT id, chat_id, message_id FROM polls WHERE closed=0 AND (? - created_at) >= ?",
            (now, VOTE_TIMEOUT),
        ).fetchall()
        if not rows:
            return
        conn.execute(
            "UPDATE polls SET closed=1, expired=1 WHERE closed=0 AND (? - created_at) >= ?",
            (now, VOTE_TIMEOUT),
        )
    # Try to edit each expired poll message
    for r in rows:
        try:
            await context.bot.edit_message_text(
                "⌛ *Vote expired*",
                chat_id=r["chat_id"],
                message_id=r["message_id"],
                parse_mode=ParseMode.MARKDOWN,
            )
        except Exception:
            # message may be deleted or too old to edit; ignore
            pass

# ---------- Main ----------
def main():
    init_db()
    token = os.environ.get("BOT_TOKEN")
    if not token:
        raise RuntimeError("BOT_TOKEN is not set")

    app = Application.builder().token(token).build()

    # Commands
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("gryaz", cmd_gryaz))
    app.add_handler(CommandHandler("stats", cmd_stats))

    # Votes
    app.add_handler(CallbackQueryHandler(on_vote, pattern=r"^vote:\d+:(plus|minus)$"))

    # Emoji triggers (reply with 🐗 or 💊)
    app.add_handler(MessageHandler(filters.REPLY & filters.TEXT & filters.Regex(r"(🐗|💊)"), on_emoji_trigger), group=-1)

    # Tracking
    app.add_handler(MessageHandler(filters.ALL & (~filters.StatusUpdate.ALL), on_message))
    app.add_handler(ChatMemberHandler(on_chat_member, ChatMemberHandler.CHAT_MEMBER))

    # Schedule background expiration job (runs even with no interactions)
    if VOTE_TIMEOUT > 0:
        # Run roughly every half-timeout, clamped between 15s and 300s
        interval = max(15, min(300, (VOTE_TIMEOUT // 2) or 60))
        app.job_queue.run_repeating(expire_polls, interval=interval, first=interval)
        log.info("Auto-expire enabled: timeout=%ss, check interval=%ss", VOTE_TIMEOUT, interval)
    else:
        log.info("Auto-expire disabled")

    log.info("Starting bot (DB_PATH=%s, TARGET_COOLDOWN=%s, VOTE_TIMEOUT=%s, INIT_SCORES=%s)",
             DB_PATH, TARGET_COOLDOWN, VOTE_TIMEOUT, "set" if INIT_SCORES_MAP else "unset")
    app.run_polling()

if __name__ == "__main__":
    main()
