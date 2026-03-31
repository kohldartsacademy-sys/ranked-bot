# -*- coding: utf-8 -*-

import discord
from discord.ext import commands
from discord import app_commands
import sqlite3
import os
from dotenv import load_dotenv
from datetime import datetime

# =============================
# SETUP
# =============================

load_dotenv()
TOKEN = os.getenv("DISCORD_TOKEN")

intents = discord.Intents.default()
intents.members = True

bot = commands.Bot(command_prefix="!", intents=intents)

# =============================
# DATABASE
# =============================

conn = sqlite3.connect("dartliga.db")
c = conn.cursor()

c.execute("CREATE TABLE IF NOT EXISTS players (user_id INTEGER PRIMARY KEY, rating INTEGER)")
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
# RATING SYSTEM
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
# GITHUB UPLOAD
# =============================

def upload():
    os.system("git add .")
    os.system('git commit -m "update"')
    os.system("git push")

# =============================
# HTML
# =============================

def generate_html():
    c.execute("SELECT user_id, rating FROM players ORDER BY rating DESC")
    data = c.fetchall()

    guild = bot.guilds[0] if bot.guilds else None

    html = "<html><body style='background:black;color:white;text-align:center;'>"
    html += "<h1>🏆 Ranking</h1><table style='margin:auto;'>"

    for i, (uid, rating) in enumerate(data, 1):
        name = f"User {uid}"
        if guild:
            m = guild.get_member(uid)
            if m:
                name = m.display_name

        html += f"<tr><td>{i}</td><td>{name}</td><td>{rating}</td></tr>"

    html += "</table></body></html>"

    with open("leaderboard.html", "w", encoding="utf-8") as f:
        f.write(html)

# =============================
# QUEUE SYSTEM
# =============================

queue = []

class QueueView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Join Queue", style=discord.ButtonStyle.green, custom_id="join")
    async def join(self, interaction: discord.Interaction, button: discord.ui.Button):

        if interaction.user in queue:
            await interaction.response.send_message("Schon in Queue", ephemeral=True)
            return

        queue.append(interaction.user)

        if len(queue) >= 2:
            p1 = queue.pop(0)
            p2 = queue.pop(0)

            get_rating(p1.id)
            get_rating(p2.id)

            c.execute("INSERT INTO matches (player1_id, player2_id) VALUES (?,?)", (p1.id, p2.id))
            conn.commit()

            match_id = c.lastrowid

            await interaction.response.send_message(f"Match #{match_id}: {p1.mention} vs {p2.mention}")
        else:
            await interaction.response.send_message("Beigetreten", ephemeral=True)

    @discord.ui.button(label="Leave Queue", style=discord.ButtonStyle.red, custom_id="leave")
    async def leave(self, interaction: discord.Interaction, button: discord.ui.Button):

        if interaction.user in queue:
            queue.remove(interaction.user)
            await interaction.response.send_message("Verlassen", ephemeral=True)
        else:
            await interaction.response.send_message("Nicht in Queue", ephemeral=True)

# =============================
# COMMANDS
# =============================

@bot.tree.command(name="queue_panel")
async def queue_panel(interaction: discord.Interaction):
    await interaction.response.send_message("Queue:", view=QueueView())

@bot.tree.command(name="result")
@app_commands.describe(match_id="Match ID", winner="Gewinner", score="Score", winner_avg="Winner Avg", loser_avg="Loser Avg")
async def result(interaction: discord.Interaction, match_id: int, winner: discord.Member, score: str, winner_avg: float, loser_avg: float):

    c.execute("SELECT player1_id, player2_id FROM matches WHERE id=?", (match_id,))
    match = c.fetchone()

    if not match:
        await interaction.response.send_message("Match nicht gefunden")
        return

    p1, p2 = match
    loser_id = p1 if winner.id == p2 else p2

    r1 = get_rating(winner.id)
    r2 = get_rating(loser_id)

    new_r1 = calculate_elo(r1, r2, 1)
    new_r2 = calculate_elo(r2, r1, 0)

    update_rating(winner.id, new_r1)
    update_rating(loser_id, new_r2)

    c.execute("""
    UPDATE matches SET
    winner_id=?, loser_id=?, score=?, winner_avg=?, loser_avg=?, status='confirmed'
    WHERE id=?
    """, (winner.id, loser_id, score, winner_avg, loser_avg, match_id))

    conn.commit()

    generate_html()
    upload()

    await interaction.response.send_message("Match gespeichert")

@bot.tree.command(name="stats")
async def stats(interaction: discord.Interaction, player: discord.Member):
    rating = get_rating(player.id)
    await interaction.response.send_message(f"{player.display_name}: {rating} ELO")

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