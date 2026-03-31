# -*- coding: utf-8 -*-

import discord
from discord.ext import commands
from discord import app_commands
import sqlite3
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()
TOKEN = os.getenv("DISCORD_TOKEN")

intents = discord.Intents.default()
intents.members = True
intents.message_content = True

bot = commands.Bot(command_prefix="!", intents=intents)

conn = sqlite3.connect("dartliga.db")
c = conn.cursor()

# =============================
# DATABASE
# =============================

c.execute("CREATE TABLE IF NOT EXISTS players (user_id INTEGER PRIMARY KEY, rating INTEGER)")

c.execute("""
CREATE TABLE IF NOT EXISTS matches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    player1_id INTEGER,
    player2_id INTEGER,
    winner_id INTEGER,
    loser_id INTEGER,
    platform TEXT,
    status TEXT DEFAULT 'pending',
    score TEXT,
    winner_avg REAL,
    loser_avg REAL,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
)
""")

c.execute("""
CREATE TABLE IF NOT EXISTS monthly_points (
    user_id INTEGER,
    month TEXT,
    points INTEGER,
    PRIMARY KEY (user_id, month)
)
""")

conn.commit()

# =============================
# RATING
# =============================

K_FACTOR = 32

def get_rating(user_id):
    c.execute("SELECT rating FROM players WHERE user_id=?", (user_id,))
    r = c.fetchone()
    if r:
        return r[0]
    c.execute("INSERT INTO players VALUES (?, 1000)", (user_id,))
    conn.commit()
    return 1000

def update_rating(user_id, rating):
    c.execute("UPDATE players SET rating=? WHERE user_id=?", (rating, user_id))
    conn.commit()

def calculate_elo(r1, r2, score):
    expected = 1 / (1 + 10 ** ((r2 - r1) / 400))
    return round(r1 + K_FACTOR * (score - expected))

# =============================
# WEBSITE GENERATION
# =============================

def upload():
    os.system("git add .")
    os.system('git commit -m "update leaderboard"')
    os.system("git push")

def generate_html():
    guild = bot.guilds[0] if bot.guilds else None

    # WORLD
    c.execute("SELECT user_id, rating FROM players ORDER BY rating DESC LIMIT 10")
    world = c.fetchall()

    # MONTHLY
    month = datetime.now().strftime("%Y-%m")
    c.execute("""
        SELECT user_id, points FROM monthly_points
        WHERE month=?
        ORDER BY points DESC LIMIT 10
    """, (month,))
    monthly = c.fetchall()

    html = """
    <html>
    <body style='background:#0d1117;color:white;font-family:sans-serif;text-align:center'>
    <h1>🏆 Dart Ranking</h1>
    <div style="display:flex;justify-content:center;gap:50px;">
    """

    # WORLD TABLE
    html += "<div><h2>🌍 World Ranking</h2><table>"
    for i, (uid, rating) in enumerate(world, 1):
        name = f"User {uid}"
        if guild:
            m = guild.get_member(uid)
            if m:
                name = m.display_name
        html += f"<tr><td>{i}</td><td>{name}</td><td>{rating}</td></tr>"
    html += "</table></div>"

    # MONTHLY TABLE
    html += "<div><h2>🗓️ Monthly Ranking</h2><table>"
    for i, (uid, points) in enumerate(monthly, 1):
        name = f"User {uid}"
        if guild:
            m = guild.get_member(uid)
            if m:
                name = m.display_name
        html += f"<tr><td>{i}</td><td>{name}</td><td>{points}</td></tr>"
    html += "</table></div>"

    html += "</div></body></html>"

    with open("leaderboard.html", "w", encoding="utf-8") as f:
        f.write(html)

# =============================
# TOP 10 COMMAND
# =============================

@bot.tree.command(name="top10")
async def top10(interaction: discord.Interaction):

    c.execute("SELECT user_id, rating FROM players ORDER BY rating DESC LIMIT 10")
    data = c.fetchall()

    text = "🏆 Top 10:\n\n"

    for i, (uid, rating) in enumerate(data, 1):
        user = await bot.fetch_user(uid)
        text += f"{i}. {user.name} - {rating}\n"

    await interaction.response.send_message(text)

# =============================
# READY
# =============================

@bot.event
async def on_ready():
    await bot.tree.sync()
    generate_html()
    upload()
    print("Bot online")

bot.run(TOKEN)