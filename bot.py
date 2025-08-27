# bot.py
import os
import math
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

# ---------- Logging ----------
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

DB_PATH = os.environ.get("DB_PATH", "gryaz.db")

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
              chat_id INTEGER NOT NULL,
              user_id INTEGER NOT NULL,
              username TEXT,
              first_name TEXT,
              is_bot INTEGER NOT NULL DEFAULT 0,
              PRIMARY KEY(chat_id, user_id)
            );

            CREATE TABLE IF NOT EXISTS scores(
              chat_id INTEGER NOT NULL,
              user_id INTEGER NOT NULL,
              score   INTEGER NOT NULL DEFAULT 0,
              PRIMARY KEY(chat_id, user_id)
            );

            CREATE TABLE IF NOT EXISTS polls(
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              chat_id INTEGER NOT NULL,
              message_id INTEGER NOT NULL,
              target_user_id INTEGER NOT NULL,
              plus_count INTEGER NOT NULL DEFAULT 0,
              minus_count INTEGER NOT NULL DEFAULT 0,
              closed INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS poll_votes(
              poll_id INTEGER NOT NULL,
              voter_user_id INTEGER NOT NULL,
              vote TEXT NOT NULL CHECK(vote IN ('plus','minus')),
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
              username=excluded.username,
              first_name=excluded.first_name,
              is_bot=excluded.is_bot
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

# ---------- Handlers ----------
async def cmd_start(update: Update, _):
    text = (
        "Hi! I’m a *gryaz* counter.\n\n"
        "• Reply to someone with /gryaz to start a vote (+ / –)\n"
        "• When + votes reach half of non-bot members, vote closes and the target gets +1\n"
        "• If – votes reach half, the poll is cancelled\n"
        "• /stats — show scores\n\n"
        "_Tip: Disable privacy mode in BotFather and make me admin for best results._"
    )
    await update.effective_message.reply_text(text, parse_mode=ParseMode.MARKDOWN)

async def cmd_stats(update: Update, _):
    chat = await ensure_group(update)
    if not chat:
        return

    with closing(db()) as conn:
        rows = conn.execute(
            """
            SELECT m.user_id, COALESCE(m.first_name,'') AS first_name,
                   COALESCE(m.username,'') AS username,
                   COALESCE(s.score,0) AS score
            FROM members m
            LEFT JOIN scores s
              ON s.chat_id=m.chat_id AND s.user_id=m.user_id
            WHERE m.chat_id=?
              AND m.is_bot=0
            ORDER BY score DESC, first_name ASC
            """,
            (chat.id,),
        ).fetchall()

    if not rows:
        await update.effective_message.reply_text("No members tracked yet.")
        return

    lines = ["*Gryaz stats:*"]
    for r in rows:
        label = f"[{r['first_name']}](tg://user?id={r['user_id']})"
        if r["username"]:
            label += f" (@{r['username']})"
        lines.append(f"{label}: *{r['score']}*")

    await update.effective_message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN)

async def cmd_gryaz(update: Update, context):
    chat = await ensure_group(update)
    if not chat:
        return

    if update.effective_user:
        upsert_member(chat, update.effective_user)

    target = resolve_target_user(update)
    if isinstance(target, int):
        try:
            cm = await context.bot.get_chat_member(chat.id, target)
            target = cm.user
        except Exception:
            target = None

    if not target:
        msg_text = update.effective_message.text or ""
        if "@" in msg_text:
            await update.effective_message.reply_text(
                "I don’t know that @username yet — they need to send at least one message in this group first."
            )
        else:
            await update.effective_message.reply_text(
                "Select a target: reply to their message with /gryaz or use /gryaz @username (after they’ve spoken at least once)."
            )
        return

    if target.is_bot:
        await update.effective_message.reply_text("You can’t start a gryaz vote on a bot.")
        return

    upsert_member(chat, target)

    with closing(db()) as conn, conn:
        cur = conn.execute(
            "INSERT INTO polls(chat_id, message_id, target_user_id, plus_count, minus_count, closed) VALUES(?,?,?,?,?,?)",
            (chat.id, 0, target.id, 0, 0, 0),
        )
        poll_id = cur.lastrowid

    kb = InlineKeyboardMarkup(
        [[
            InlineKeyboardButton(text="👍 +", callback_data=f"vote:{poll_id}:plus"),
            InlineKeyboardButton(text="👎 –", callback_data=f"vote:{poll_id}:minus"),
        ]]
    )
    threshold = max(1, math.ceil(non_bot_member_count(chat.id) / 2))
    m = await update.effective_message.reply_text(
        f"*Gryaz vote started* for [{target.first_name}](tg://user?id={target.id}).\n"
        f"Need *{threshold}* 👍 or 👎 to decide.",
        reply_markup=kb,
        parse_mode=ParseMode.MARKDOWN,
    )
    with closing(db()) as conn, conn:
        conn.execute("UPDATE polls SET message_id=? WHERE id=?", (m.message_id, poll_id))

async def on_vote(update: Update, context):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    data = q.data or ""
    try:
        _, poll_id_str, vote = data.split(":")
        poll_id = int(poll_id_str)
        assert vote in ("plus", "minus")
    except Exception:
        return

    chat = q.message.chat
    voter = q.from_user
    upsert_member(chat, voter)

    with closing(db()) as conn:
        poll = conn.execute(
            "SELECT * FROM polls WHERE id=? AND chat_id=?", (poll_id, chat.id)
        ).fetchone()

    if not poll or poll["closed"]:
        await q.edit_message_reply_markup(reply_markup=None)
        return

    # Vote only once
    try:
        with closing(db()) as conn, conn:
            conn.execute(
                "INSERT INTO poll_votes(poll_id, voter_user_id, vote) VALUES(?,?,?)",
                (poll_id, voter.id, vote),
            )
    except sqlite3.IntegrityError:
        await q.answer("You already voted!", show_alert=True)
        return

    # Recount
    with closing(db()) as conn:
        row = conn.execute(
            "SELECT SUM(vote='plus') AS plus_c, SUM(vote='minus') AS minus_c FROM poll_votes WHERE poll_id=?",
            (poll_id,),
        ).fetchone()
        plus_c = int(row["plus_c"] or 0)
        minus_c = int(row["minus_c"] or 0)

        conn.execute(
            "UPDATE polls SET plus_count=?, minus_count=? WHERE id=?",
            (plus_c, minus_c, poll_id),
        )

    threshold = max(1, math.ceil(non_bot_member_count(chat.id) / 2))

    # YES wins
    if plus_c >= threshold:
        with closing(db()) as conn, conn:
            conn.execute("UPDATE polls SET closed=1 WHERE id=?", (poll_id,))
            target_user_id = poll["target_user_id"]
            conn.execute(
                """
                INSERT INTO scores(chat_id, user_id, score)
                VALUES(?,?,1)
                ON CONFLICT(chat_id, user_id) DO UPDATE SET score = score + 1
                """,
                (chat.id, target_user_id),
            )

        try:
            cm = await context.bot.get_chat_member(chat.id, poll["target_user_id"])
            target_name = cm.user.first_name
            target_id = cm.user.id
        except Exception:
            target_name = "user"
            target_id = poll["target_user_id"]

        await q.edit_message_text(
            f"✅ *Gryaz vote passed* for [{target_name}](tg://user?id={target_id}). "
            f"(👍 {plus_c} / 👎 {minus_c}, needed {threshold})\n"
            f"Score updated: +1.",
            parse_mode=ParseMode.MARKDOWN,
        )

    # NO wins
    elif minus_c >= threshold:
        with closing(db()) as conn, conn:
            conn.execute("UPDATE polls SET closed=1 WHERE id=?", (poll_id,))

        await q.edit_message_text(
            f"❌ *Gryaz vote cancelled*.\n"
            f"(👍 {plus_c} / 👎 {minus_c}, needed {threshold})",
            parse_mode=ParseMode.MARKDOWN,
        )

    # Still in progress
    else:
        await q.edit_message_text(
            f"*Vote in progress...*\n👍 {plus_c} / 👎 {minus_c} (need {threshold})",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(
                [[
                    InlineKeyboardButton(text="👍 +", callback_data=f"vote:{poll_id}:plus"),
                    InlineKeyboardButton(text="👎 –", callback_data=f"vote:{poll_id}:minus"),
                ]]
            ),
        )

async def on_message(update: Update, _):
    if not update.effective_chat or update.effective_chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        return
    if update.effective_user:
        upsert_member(update.effective_chat, update.effective_user)
    if update.effective_message and update.effective_message.reply_to_message and update.effective_message.reply_to_message.from_user:
        upsert_member(update.effective_chat, update.effective_message.reply_to_message.from_user)

async def on_chat_member(update: Update, _):
    cmu: ChatMemberUpdated = update.chat_member
    chat = cmu.chat
    user = cmu.new_chat_member.user
    upsert_member(chat, user)

# ---------- Main ----------
def main():
    init_db()
    token = os.environ.get("BOT_TOKEN")
    if not token:
        raise RuntimeError("BOT_TOKEN is not set")

    app = Application.builder().token(token).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("gryaz", cmd_gryaz))
    app.add_handler(CommandHandler("stats", cmd_stats))

    app.add_handler(CallbackQueryHandler(on_vote, pattern=r"^vote:\d+:(plus|minus)$"))

    app.add_handler(MessageHandler(filters.ALL & (~filters.StatusUpdate.ALL), on_message))
    app.add_handler(ChatMemberHandler(on_chat_member, ChatMemberHandler.CHAT_MEMBER))

    log.info("Starting bot polling…")
    app.run_polling()

if __name__ == "__main__":
    main()
