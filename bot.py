# -*- coding: utf-8 -*-

import discord
from discord import app_commands
from discord.ext import commands
import sqlite3
import os
from dotenv import load_dotenv
from datetime import datetime

# =============================
# ENV + BOT SETUP
# =============================

load_dotenv()
TOKEN = os.getenv("DISCORD_TOKEN")

intents = discord.Intents.default()
intents.members = True
intents.message_content = True

bot = commands.Bot(command_prefix="!", intents=intents)

# =============================
# DATABASE
# =============================

conn = sqlite3.connect("dartliga.db")
c = conn.cursor()

c.execute("""
CREATE TABLE IF NOT EXISTS players (
    user_id INTEGER PRIMARY KEY,
    rating INTEGER
)
""")

c.execute("""
CREATE TABLE IF NOT EXISTS matches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    player1_id INTEGER,
    player2_id INTEGER,
    winner_id INTEGER,
    loser_id INTEGER,
    winner_old_rating INTEGER,
    winner_new_rating INTEGER,
    loser_old_rating INTEGER,
    loser_new_rating INTEGER,
    status TEXT DEFAULT 'pending',
    platform TEXT,
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
# GLOBALS
# =============================

queue_dartcounter = []
queue_scolia = []

QUEUE_MESSAGE_ID = None
QUEUE_CHANNEL_ID = None

K_FACTOR = 32

# =============================
# GITHUB UPLOAD (FIXED)
# =============================

def upload_to_github():
    print("🔄 Starte Git Upload...")

    os.system("git add .")
    result_commit = os.system('git commit -m "auto update leaderboard"')

    if result_commit != 0:
        print("⚠️ Nichts zu committen")

    result_push = os.system("git push")

    if result_push == 0:
        print("✅ GitHub Upload erfolgreich")
    else:
        print("❌ Git Push Fehler")

# =============================
# HTML GENERATION (UTF-8 FIX)
# =============================

def generate_leaderboard_html():
    c.execute("SELECT user_id, rating FROM players ORDER BY rating DESC")
    players = c.fetchall()

    guild = bot.guilds[0] if bot.guilds else None

    html = "<html><body style='background:#0d0d0d;color:white;text-align:center;'>"
    html += "<h1>🏆 World Ranking</h1><table style='margin:auto;'>"

    for i, (user_id, rating) in enumerate(players, start=1):
        name = f"User {user_id}"
        if guild:
            member = guild.get_member(user_id)
            if member:
                name = member.display_name

        html += f"<tr><td>{i}</td><td>{name}</td><td>{rating}</td></tr>"

    html += "</table></body></html>"

    with open("leaderboard.html", "w", encoding="utf-8") as f:
        f.write(html)


def generate_monthly_leaderboard_html():
    current_month = datetime.now().strftime("%Y-%m")

    c.execute("""
        SELECT user_id, points
        FROM monthly_points
        WHERE month=?
        ORDER BY points DESC
    """, (current_month,))

    players = c.fetchall()
    guild = bot.guilds[0] if bot.guilds else None

    html = "<html><body style='background:#0d0d0d;color:white;text-align:center;'>"
    html += f"<h1>🏆 Monatsranking ({current_month})</h1><table style='margin:auto;'>"

    for i, (user_id, points) in enumerate(players, start=1):
        name = f"User {user_id}"
        if guild:
            member = guild.get_member(user_id)
            if member:
                name = member.display_name

        html += f"<tr><td>{i}</td><td>{name}</td><td>{points}</td></tr>"

    html += "</table></body></html>"

    with open("monthly.html", "w", encoding="utf-8") as f:
        f.write(html)

# =============================
# RATING SYSTEM
# =============================

def get_rating(user_id):
    c.execute("SELECT rating FROM players WHERE user_id=?", (user_id,))
    r = c.fetchone()
    if r:
        return r[0]
    c.execute("INSERT INTO players VALUES (?, 1000)", (user_id,))
    conn.commit()
    return 1000

def update_rating(user_id, new_rating):
    c.execute("UPDATE players SET rating=? WHERE user_id=?", (new_rating, user_id))
    conn.commit()

def calculate_elo(r1, r2, score1):
    expected = 1 / (1 + 10 ** ((r2 - r1) / 400))
    return round(r1 + K_FACTOR * (score1 - expected))

# =============================
# QUEUE SYSTEM (FIXED)
# =============================

class QueueView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="🎯 DartCounter", style=discord.ButtonStyle.green, custom_id="dart_btn")
    async def dart(self, interaction: discord.Interaction, button: discord.ui.Button):
        await handle_queue(interaction, "dartcounter")

    @discord.ui.button(label="🔵 Scolia", style=discord.ButtonStyle.blurple, custom_id="scolia_btn")
    async def scolia(self, interaction: discord.Interaction, button: discord.ui.Button):
        await handle_queue(interaction, "scolia")

    @discord.ui.button(label="❌ Verlassen", style=discord.ButtonStyle.red, custom_id="leave_btn")
    async def leave(self, interaction: discord.Interaction, button: discord.ui.Button):

        if interaction.user in queue_dartcounter:
            queue_dartcounter.remove(interaction.user)

        if interaction.user in queue_scolia:
            queue_scolia.remove(interaction.user)

        await interaction.response.send_message("Queue aktualisiert", ephemeral=True)
        await update_queue_panel(interaction.guild)


async def handle_queue(interaction, platform):

    queue = queue_dartcounter if platform == "dartcounter" else queue_scolia

    if interaction.user in queue:
        await interaction.response.send_message("Schon in Queue", ephemeral=True)
        return

    queue.append(interaction.user)

    if len(queue) >= 2:
        p1 = queue.pop(0)
        p2 = queue.pop(0)

        get_rating(p1.id)
        get_rating(p2.id)

        c.execute("INSERT INTO matches (player1_id, player2_id, platform) VALUES (?, ?, ?)",
                  (p1.id, p2.id, platform))
        conn.commit()

        match_id = c.lastrowid

        await interaction.response.send_message(
            f"🎯 MATCH #{match_id}\n{p1.mention} vs {p2.mention}"
        )
    else:
        await interaction.response.send_message("Beigetreten", ephemeral=True)

    await update_queue_panel(interaction.guild)

# =============================
# LIVE PANEL
# =============================

async def update_queue_panel(guild):
    if not QUEUE_MESSAGE_ID:
        return

    channel = guild.get_channel(QUEUE_CHANNEL_ID)
    msg = await channel.fetch_message(QUEUE_MESSAGE_ID)

    embed = discord.Embed(title="🎯 Queue")

    embed.add_field(name="DartCounter", value=len(queue_dartcounter))
    embed.add_field(name="Scolia", value=len(queue_scolia))

    await msg.edit(embed=embed, view=QueueView())

# =============================
# COMMANDS
# =============================

@bot.tree.command(name="queue_panel")
async def queue_panel(interaction: discord.Interaction):

    global QUEUE_MESSAGE_ID, QUEUE_CHANNEL_ID

    embed = discord.Embed(title="🎯 Queue")

    await interaction.response.send_message(embed=embed, view=QueueView())

    msg = await interaction.original_response()
    QUEUE_MESSAGE_ID = msg.id
    QUEUE_CHANNEL_ID = interaction.channel.id


@bot.tree.command(name="result")
@app_commands.describe(
    match_id="Match ID",
    winner="Gewinner",
    score="Ergebnis",
    winner_avg="Average Gewinner",
    loser_avg="Average Verlierer"
)
async def result(interaction: discord.Interaction, match_id: int, winner: discord.Member, score: str, winner_avg: float, loser_avg: float):

    c.execute("SELECT player1_id, player2_id, status FROM matches WHERE id=?", (match_id,))
    match = c.fetchone()

    if not match:
        await interaction.response.send_message("Match nicht gefunden", ephemeral=True)
        return

    p1, p2, status = match

    if status != "pending":
        await interaction.response.send_message("Schon abgeschlossen", ephemeral=True)
        return

    loser_id = p1 if winner.id == p2 else p2

    r1 = get_rating(winner.id)
    r2 = get_rating(loser_id)

    new_r1 = calculate_elo(r1, r2, 1)
    new_r2 = calculate_elo(r2, r1, 0)

    update_rating(winner.id, new_r1)
    update_rating(loser_id, new_r2)

    current_month = datetime.now().strftime("%Y-%m")
    elo_gain = max(0, new_r1 - r1)

    c.execute("""
        INSERT INTO monthly_points (user_id, month, points)
        VALUES (?, ?, ?)
        ON CONFLICT(user_id, month)
        DO UPDATE SET points = points + ?
    """, (winner.id, current_month, elo_gain, elo_gain))

    c.execute("""
        UPDATE matches SET
            winner_id=?, loser_id=?,
            winner_old_rating=?, winner_new_rating=?,
            loser_old_rating=?, loser_new_rating=?,
            score=?, winner_avg=?, loser_avg=?,
            status='confirmed'
        WHERE id=?
    """, (winner.id, loser_id, r1, new_r1, r2, new_r2, score, winner_avg, loser_avg, match_id))

    conn.commit()

    print("🔥 Generiere HTML...")
    generate_leaderboard_html()
    generate_monthly_leaderboard_html()

    print("🔥 Upload...")
    upload_to_github()

    await interaction.response.send_message("Match gespeichert!")

# =============================
# READY
# =============================

@bot.event
async def on_ready():
    print("🔥 Generiere HTML...")
    generate_leaderboard_html()
    generate_monthly_leaderboard_html()

    print("🔥 Upload...")
    upload_to_github()

    await bot.tree.sync()

    print(f"{bot.user} ist online!")

bot.run(TOKEN)