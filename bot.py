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
# HTML + GITHUB
# =============================

def upload():
    os.system("git add .")
    os.system('git commit -m "update leaderboard"')
    os.system("git push")

def generate_html():
    c.execute("SELECT user_id, rating FROM players ORDER BY rating DESC")
    data = c.fetchall()

    guild = bot.guilds[0] if bot.guilds else None

    html = "<html><body style='background:black;color:white;text-align:center;'>"
    html += "<h1>🏆 World Ranking</h1><table style='margin:auto;'>"

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
# QUEUE
# =============================

queue_dart = []
queue_scolia = []

QUEUE_MESSAGE_ID = None
QUEUE_CHANNEL_ID = None

async def update_queue(guild):
    if not QUEUE_MESSAGE_ID or not QUEUE_CHANNEL_ID:
        return

    channel = guild.get_channel(QUEUE_CHANNEL_ID)
    if not channel:
        return

    try:
        msg = await channel.fetch_message(QUEUE_MESSAGE_ID)

        embed = discord.Embed(title="🎯 Dart Matchmaking")

        dart = "\n".join([u.display_name for u in queue_dart]) or "Keine Spieler"
        scolia = "\n".join([u.display_name for u in queue_scolia]) or "Keine Spieler"

        embed.add_field(name=f"🎯 DartCounter ({len(queue_dart)})", value=dart, inline=False)
        embed.add_field(name=f"🔵 Scolia ({len(queue_scolia)})", value=scolia, inline=False)

        await msg.edit(embed=embed, view=QueueView())
    except:
        pass

class QueueView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="🎯 DartCounter", style=discord.ButtonStyle.green)
    async def dart(self, interaction: discord.Interaction, button: discord.ui.Button):
        await handle_queue(interaction, "dart")

    @discord.ui.button(label="🔵 Scolia", style=discord.ButtonStyle.blurple)
    async def scolia(self, interaction: discord.Interaction, button: discord.ui.Button):
        await handle_queue(interaction, "scolia")

    @discord.ui.button(label="❌ Leave", style=discord.ButtonStyle.red)
    async def leave(self, interaction: discord.Interaction, button: discord.ui.Button):

        if interaction.user in queue_dart:
            queue_dart.remove(interaction.user)

        if interaction.user in queue_scolia:
            queue_scolia.remove(interaction.user)

        await interaction.response.send_message("Queue verlassen", ephemeral=True)
        await update_queue(interaction.guild)

async def handle_queue(interaction, mode):

    queue = queue_dart if mode == "dart" else queue_scolia

    if interaction.user in queue:
        await interaction.response.send_message("Schon in Queue", ephemeral=True)
        return

    queue.append(interaction.user)

    if len(queue) >= 2:
        p1 = queue.pop(0)
        p2 = queue.pop(0)

        get_rating(p1.id)
        get_rating(p2.id)

        c.execute("INSERT INTO matches (player1_id, player2_id, platform) VALUES (?,?,?)",
                  (p1.id, p2.id, mode))
        conn.commit()

        match_id = c.lastrowid

        await interaction.response.send_message(
            f"🎯 Match #{match_id} ({mode})\n{p1.mention} vs {p2.mention}"
        )
    else:
        await interaction.response.send_message("Beigetreten", ephemeral=True)

    await update_queue(interaction.guild)

# =============================
# COMMANDS
# =============================

@bot.tree.command(name="queue_panel")
async def queue_panel(interaction: discord.Interaction):

    global QUEUE_MESSAGE_ID, QUEUE_CHANNEL_ID

    embed = discord.Embed(title="🎯 Dart Matchmaking")

    await interaction.response.send_message(embed=embed, view=QueueView())

    msg = await interaction.original_response()
    QUEUE_MESSAGE_ID = msg.id
    QUEUE_CHANNEL_ID = interaction.channel.id

    await update_queue(interaction.guild)

# 🔥 FULL STATS
@bot.tree.command(name="stats")
async def stats(interaction: discord.Interaction, player: discord.Member):

    rating = get_rating(player.id)

    c.execute("SELECT user_id FROM players ORDER BY rating DESC")
    ranking = [r[0] for r in c.fetchall()]
    world_rank = ranking.index(player.id) + 1 if player.id in ranking else "N/A"

    month = datetime.now().strftime("%Y-%m")
    c.execute("SELECT user_id FROM monthly_points WHERE month=? ORDER BY points DESC", (month,))
    monthly = [r[0] for r in c.fetchall()]
    monthly_rank = monthly.index(player.id) + 1 if player.id in monthly else "N/A"

    c.execute("SELECT COUNT(*) FROM matches WHERE winner_id=? AND status='confirmed'", (player.id,))
    wins = c.fetchone()[0]

    c.execute("SELECT COUNT(*) FROM matches WHERE loser_id=? AND status='confirmed'", (player.id,))
    losses = c.fetchone()[0]

    total = wins + losses
    winrate = round((wins / total) * 100, 1) if total > 0 else 0

    # averages
    c.execute("""
        SELECT winner_id, winner_avg, loser_id, loser_avg
        FROM matches
        WHERE status='confirmed'
        AND (winner_id=? OR loser_id=?)
        ORDER BY id DESC
    """, (player.id, player.id))

    data = c.fetchall()
    avgs = []

    for w, wa, l, la in data:
        if w == player.id and wa:
            avgs.append(wa)
        elif l == player.id and la:
            avgs.append(la)

    if avgs:
        avg = round(sum(avgs) / len(avgs), 2)
        best = max(avgs)
        worst = min(avgs)
        last5 = "\n".join([f"{i+1}. {v}" for i, v in enumerate(avgs[:5])])
    else:
        avg = best = worst = 0
        last5 = "Keine Daten"

    embed = discord.Embed(title=f"📊 Stats von {player.display_name}")

    embed.add_field(name="🌍 Global Rank", value=world_rank)
    embed.add_field(name="🗓️ Monthly Rank", value=monthly_rank)
    embed.add_field(name="🏆 Rating", value=rating)
    embed.add_field(name="🎯 Spiele", value=total)
    embed.add_field(name="✅ Siege", value=wins)
    embed.add_field(name="❌ Niederlagen", value=losses)
    embed.add_field(name="📈 Winrate", value=f"{winrate}%")
    embed.add_field(name="🎯 Ø Average", value=avg)
    embed.add_field(name="🔥 Letzte 5", value=last5)
    embed.add_field(name="💎 Best", value=best)
    embed.add_field(name="📉 Worst", value=worst)

    await interaction.response.send_message(embed=embed)

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