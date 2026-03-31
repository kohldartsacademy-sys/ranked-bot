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
# WEBSITE
# =============================

def upload():
    os.system("git add .")
    os.system('git commit -m "update leaderboard"')
    os.system("git push")

def generate_html():
    guild = bot.guilds[0] if bot.guilds else None

    # =============================
    # DATA
    # =============================

    c.execute("SELECT user_id, rating FROM players ORDER BY rating DESC")
    players = c.fetchall()

    month = datetime.now().strftime("%Y-%m")

    c.execute("""
        SELECT user_id, points FROM monthly_points
        WHERE month=?
        ORDER BY points DESC
    """, (month,))
    monthly_data = c.fetchall()

    top3 = players[:3]
    rest = players[3:]

    # =============================
    # HTML START
    # =============================

    html = """
    <html>
    <head>
    <style>
    body {background:#0b0f14;color:white;font-family:Segoe UI;text-align:center;}

    .container {display:flex;justify-content:center;gap:50px;margin-top:40px;}

    .podium {display:flex;justify-content:center;gap:30px;margin-top:30px;}

    .card {background:#161b22;padding:20px;border-radius:20px;width:180px;transition:0.3s;}
    .card:hover {transform:scale(1.05);}

    .gold {border:2px solid gold;}
    .silver {border:2px solid silver;}
    .bronze {border:2px solid #cd7f32;}

    .avatar {width:70px;height:70px;border-radius:50%;}

    table {width:100%;border-collapse:collapse;margin-top:20px;}
    td {padding:10px;border-bottom:1px solid #222;}

    tr:hover {background:#161b22;}

    a {color:#58a6ff;text-decoration:none;font-weight:bold;}
    </style>
    </head>

    <body>

    <h1>🏆 Dart Ranking Dashboard</h1>
    """

    # =============================
    # PODIUM
    # =============================

    html += "<div class='podium'>"
    classes = ["gold", "silver", "bronze"]

    for i, (uid, rating) in enumerate(top3):

        name = f"User {uid}"
        avatar = "https://cdn.discordapp.com/embed/avatars/0.png"

        if guild:
            m = guild.get_member(uid)
            if m:
                name = m.display_name
                avatar = m.display_avatar.url

        html += f"""
        <div class='card {classes[i]}'>
        <img src="{avatar}" class="avatar"><br>
        <h2>#{i+1}</h2>
        <a href='player_{uid}.html'>{name}</a>
        <p>{rating} ELO</p>
        </div>
        """

    html += "</div>"

    # =============================
    # TABLES
    # =============================

    html += "<div class='container'>"

    # 🌍 WORLD
    html += "<div style='width:40%'><h2>🌍 World Ranking</h2><table>"

    for i, (uid, rating) in enumerate(players, 1):

        name = f"User {uid}"
        avatar = "https://cdn.discordapp.com/embed/avatars/0.png"

        if guild:
            m = guild.get_member(uid)
            if m:
                name = m.display_name
                avatar = m.display_avatar.url

        html += f"""
        <tr>
        <td>{i}</td>
        <td><img src="{avatar}" class="avatar"></td>
        <td><a href='player_{uid}.html'>{name}</a></td>
        <td>{rating}</td>
        </tr>
        """

    html += "</table></div>"

    # 🗓️ MONTHLY
    html += "<div style='width:40%'><h2>🗓️ Monatsranking</h2><table>"

    for i, (uid, points) in enumerate(monthly_data, 1):

        name = f"User {uid}"
        avatar = "https://cdn.discordapp.com/embed/avatars/0.png"

        if guild:
            m = guild.get_member(uid)
            if m:
                name = m.display_name
                avatar = m.display_avatar.url

        html += f"""
        <tr>
        <td>{i}</td>
        <td><img src="{avatar}" class="avatar"></td>
        <td><a href='player_{uid}.html'>{name}</a></td>
        <td>{points}</td>
        </tr>
        """

    html += "</table></div>"

    html += "</div>"

    # =============================
    # PLAYER PROFILES
    # =============================

    for i, (uid, rating) in enumerate(players, 1):

        name = f"User {uid}"
        avatar = "https://cdn.discordapp.com/embed/avatars/0.png"

        if guild:
            m = guild.get_member(uid)
            if m:
                name = m.display_name
                avatar = m.display_avatar.url

        # Stats
        c.execute("SELECT COUNT(*) FROM matches WHERE winner_id=? AND status='confirmed'", (uid,))
        wins = c.fetchone()[0]

        c.execute("SELECT COUNT(*) FROM matches WHERE loser_id=? AND status='confirmed'", (uid,))
        losses = c.fetchone()[0]

        total = wins + losses
        winrate = round((wins / total) * 100, 1) if total > 0 else 0

        # Monthly rank
        c.execute("SELECT user_id FROM monthly_points WHERE month=? ORDER BY points DESC", (month,))
        monthly = [r[0] for r in c.fetchall()]
        monthly_rank = monthly.index(uid) + 1 if uid in monthly else "N/A"

        # Average
        c.execute("""
            SELECT winner_id, winner_avg, loser_id, loser_avg
            FROM matches
            WHERE status='confirmed'
            AND (winner_id=? OR loser_id=?)
        """, (uid, uid))

        data = c.fetchall()
        avgs = []

        for w, wa, l, la in data:
            if w == uid and wa:
                avgs.append(wa)
            elif l == uid and la:
                avgs.append(la)

        avg = round(sum(avgs) / len(avgs), 2) if avgs else 0

        history = ""
        for match in data[:5]:
            history += f"<li>{match[1] or match[3]}</li>"

        profile_html = f"""
        <html>
        <body style='background:#0b0f14;color:white;font-family:Segoe UI;text-align:center'>

        <div style="background:#161b22;padding:30px;margin:auto;margin-top:50px;width:400px;border-radius:20px;">

        <img src="{avatar}" style="width:120px;height:120px;border-radius:50%;">

        <h1>{name}</h1>

        <p>🏆 Rating: {rating}</p>
        <p>🌍 Rank: {i}</p>
        <p>🗓️ Monatsrang: {monthly_rank}</p>

        <p>🎯 Spiele: {total}</p>
        <p>📈 Winrate: {winrate}%</p>

        <p>🎯 Ø Average: {avg}</p>

        <h3>🔥 Letzte Matches</h3>
        <ul>{history}</ul>

        <br><a href="leaderboard.html">⬅ Zurück</a>

        </div>
        </body>
        </html>
        """

        with open(f"player_{uid}.html", "w", encoding="utf-8") as f:
            f.write(profile_html)

    # =============================
    # SAVE
    # =============================

    html += "</body></html>"

    with open("leaderboard.html", "w", encoding="utf-8") as f:
        f.write(html)

# =============================
# QUEUE
# =============================

queue_dart=[]
queue_scolia=[]
QUEUE_MESSAGE_ID=None
QUEUE_CHANNEL_ID=None

async def update_queue(guild):
    if not QUEUE_MESSAGE_ID or not QUEUE_CHANNEL_ID:
        return

    channel = guild.get_channel(QUEUE_CHANNEL_ID)
    if not channel:
        return

    try:
        msg = await channel.fetch_message(QUEUE_MESSAGE_ID)

        embed = discord.Embed(
            title="🎯 Dart Matchmaking",
            description="Live Queue Status",
            color=discord.Color.blue()
        )

        # Dart Queue
        dart_list = "\n".join([u.display_name for u in queue_dart]) if queue_dart else "Keine Spieler"

        embed.add_field(
            name=f"🎯 DartCounter ({len(queue_dart)})",
            value=dart_list,
            inline=False
        )

        # Scolia Queue
        scolia_list = "\n".join([u.display_name for u in queue_scolia]) if queue_scolia else "Keine Spieler"

        embed.add_field(
            name=f"🔵 Scolia ({len(queue_scolia)})",
            value=scolia_list,
            inline=False
        )

        await msg.edit(embed=embed, view=QueueView())

    except Exception as e:
        print("Queue Update Fehler:", e)

class QueueView(discord.ui.View):
    def __init__(self):super().__init__(timeout=None)

    @discord.ui.button(label="🎯 DartCounter",style=discord.ButtonStyle.green)
    async def dart(self,i,b):await handle_queue(i,"dart")

    @discord.ui.button(label="🔵 Scolia",style=discord.ButtonStyle.blurple)
    async def scolia(self,i,b):await handle_queue(i,"scolia")

    @discord.ui.button(label="❌ Leave",style=discord.ButtonStyle.red)
    async def leave(self,i,b):
        if i.user in queue_dart:queue_dart.remove(i.user)
        if i.user in queue_scolia:queue_scolia.remove(i.user)
        await i.response.send_message("Verlassen",ephemeral=True)
        await update_queue(i.guild)

async def handle_queue(i,mode):
    q=queue_dart if mode=="dart" else queue_scolia
    if i.user in q:
        await i.response.send_message("Schon drin",ephemeral=True);return
    q.append(i.user)

    if len(q)>=2:
        p1,p2=q.pop(0),q.pop(0)
        get_rating(p1.id);get_rating(p2.id)
        c.execute("INSERT INTO matches (player1_id,player2_id,platform) VALUES (?,?,?)",(p1.id,p2.id,mode))
        conn.commit()
        await i.response.send_message(f"🎯 Match {p1.mention} vs {p2.mention}")
    else:
        await i.response.send_message("Beigetreten",ephemeral=True)

    await update_queue(i.guild)

# =============================
# COMMANDS
# =============================

@bot.tree.command(name="queue_panel")
async def queue_panel(interaction: discord.Interaction):

    global QUEUE_MESSAGE_ID, QUEUE_CHANNEL_ID

    embed = discord.Embed(
        title="🎯 Dart Matchmaking",
        description="Live Queue wird geladen..."
    )

    await interaction.response.send_message(embed=embed, view=QueueView())

    msg = await interaction.original_response()

    QUEUE_MESSAGE_ID = msg.id
    QUEUE_CHANNEL_ID = interaction.channel.id

    await update_queue(i.guild)

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

    embed = discord.Embed(title=f"📊 Stats von {player.display_name}")

    embed.add_field(name="🌍 Global Rank", value=world_rank)
    embed.add_field(name="🗓️ Monthly Rank", value=monthly_rank)
    embed.add_field(name="🏆 Rating", value=rating)
    embed.add_field(name="🎯 Spiele", value=total)
    embed.add_field(name="📈 Winrate", value=f"{winrate}%")

    await interaction.response.send_message(embed=embed)

@bot.tree.command(name="result")
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

    month = datetime.now().strftime("%Y-%m")
    gain = max(0, new_r1 - r1)

    c.execute("""
        INSERT INTO monthly_points (user_id, month, points)
        VALUES (?, ?, ?)
        ON CONFLICT(user_id, month)
        DO UPDATE SET points = points + ?
    """, (winner.id, month, gain, gain))

    c.execute("""
        UPDATE matches SET
        winner_id=?, loser_id=?, score=?, winner_avg=?, loser_avg=?, status='confirmed'
        WHERE id=?
    """, (winner.id, loser_id, score, winner_avg, loser_avg, match_id))

    conn.commit()

    generate_html()
    upload()

    await interaction.response.send_message("Match gespeichert & Website aktualisiert")

@bot.tree.command(name="history")
async def history(interaction: discord.Interaction, player: discord.Member):

    c.execute("""
        SELECT player1_id, player2_id, winner_id, score, platform
        FROM matches
        WHERE status='confirmed'
        AND (player1_id=? OR player2_id=?)
        ORDER BY id DESC
        LIMIT 10
    """, (player.id, player.id))

    matches = c.fetchall()

    if not matches:
        await interaction.response.send_message("Keine Matches gefunden.")
        return

    text = f"📜 Match History von {player.display_name}:\n\n"

    for p1, p2, winner, score, platform in matches:

        opponent_id = p2 if player.id == p1 else p1
        opponent = await bot.fetch_user(opponent_id)

        result = "🏆 Win" if winner == player.id else "❌ Loss"

        text += f"{result} vs {opponent.name} ({platform})\n"
        text += f"Score: {score}\n\n"

    await interaction.response.send_message(text)

@bot.tree.command(name="top10")
async def top10(i):
    c.execute("SELECT user_id,rating FROM players ORDER BY rating DESC LIMIT 10")
    data=c.fetchall()
    text="🏆 Top 10\n\n"
    for n,(uid,r) in enumerate(data,1):
        u=await bot.fetch_user(uid)
        text+=f"{n}. {u.name} - {r}\n"
    await i.response.send_message(text)

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