# -*- coding: utf-8 -*-

import json
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

try:
    c.execute("ALTER TABLE matches ADD COLUMN elo_change INTEGER")
    conn.commit()
except:
    pass

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

    <h1>🏆 RANKED DARTS Dashboard</h1>
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

        rows = c.fetchall()

        avgs = []

        for winner_id, winner_avg, loser_id, loser_avg in rows:

            # Spieler ist Gewinner
            if winner_id == uid and winner_avg is not None:
                avgs.append(float(winner_avg))

            # Spieler ist Verlierer
            if loser_id == uid and loser_avg is not None:
                avgs.append(float(loser_avg))

        # FINALER AVERAGE
        overall_avg = round(sum(avgs) / len(avgs), 2) if avgs else 0

        history = ""

        for p1, p2, winner, score, platform, wa, la, elo_gain, mid in c.execute("""
            SELECT player1_id, player2_id, winner_id, score, platform, winner_avg, loser_avg, elo_change, id
            FROM matches
            WHERE status='confirmed'
            AND (player1_id=? OR player2_id=?)
            ORDER BY id DESC
            LIMIT 5
        """, (uid, uid)):

            opponent_id = p2 if uid == p1 else p1

            name_opponent = f"User {opponent_id}"
            if guild:
                m2 = guild.get_member(opponent_id)
                if m2:
                    name_opponent = m2.display_name

            elo_gain = elo_gain if elo_gain else 0

            if winner == uid:
                result = "🟢 Win"
                match_avg = wa
                elo_text = f"+{elo_gain}"
            else:
                result = "🔴 Loss"
                match_avg = la
                elo_text = f"-{elo_gain}"

            history += f"<li>{result} vs {name_opponent} ({score}) → {match_avg} ({elo_text} ELO)</li>"

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

        <p>🎯 Ø Average: {overall_avg}</p>

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

queue_dart = []
queue_scolia = []
QUEUE_MESSAGE_ID = None
QUEUE_CHANNEL_ID = None

# 🔥 AKTUELLES MATCH
CURRENT_MATCH = None
MATCH_CONFIRMATIONS = set()

# 🔥 MATCH MESSAGE SPEICHERN
MATCH_MESSAGE_ID = None
MATCH_CHANNEL_ID = None


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

        # 🔥 MATCH ANZEIGE
        global CURRENT_MATCH
        if CURRENT_MATCH:
            embed.add_field(
                name="🔥 Aktuelles Match",
                value=f"{CURRENT_MATCH['p1'].display_name} vs {CURRENT_MATCH['p2'].display_name}\n({CURRENT_MATCH['mode']})",
                inline=False
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
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="🎯 DartCounter", style=discord.ButtonStyle.green, custom_id="queue_dart")
    async def dart(self, interaction: discord.Interaction, button: discord.ui.Button):
        await handle_queue(interaction, "dart")

    @discord.ui.button(label="🔵 Scolia", style=discord.ButtonStyle.blurple, custom_id="queue_scolia")
    async def scolia(self, interaction: discord.Interaction, button: discord.ui.Button):
        await handle_queue(interaction, "scolia")

    @discord.ui.button(label="❌ Leave", style=discord.ButtonStyle.red, custom_id="queue_leave")
    async def leave(self, interaction: discord.Interaction, button: discord.ui.Button):

        if interaction.user in queue_dart:
            queue_dart.remove(interaction.user)

        if interaction.user in queue_scolia:
            queue_scolia.remove(interaction.user)

        await interaction.response.send_message("Queue verlassen", ephemeral=True)
        await update_queue(interaction.guild)

    # 🔥 NEU: MATCH CONFIRM BUTTON
    @discord.ui.button(label="✅ Match bestätigen", style=discord.ButtonStyle.success, custom_id="match_confirm")
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):

        global CURRENT_MATCH, MATCH_CONFIRMATIONS

        if not CURRENT_MATCH:
            await interaction.response.send_message("Kein aktives Match", ephemeral=True)
            return

        if interaction.user.id not in [CURRENT_MATCH["p1"].id, CURRENT_MATCH["p2"].id]:
            await interaction.response.send_message("Du bist nicht Teil dieses Matches", ephemeral=True)
            return

        MATCH_CONFIRMATIONS.add(interaction.user.id)

        # Beide bestätigt?
        if len(MATCH_CONFIRMATIONS) >= 2:
            await interaction.response.send_message(
                "🔥 Match bestätigt! Der Gewinner trägt das Ergebnis nach dem Spiel mit /result ein."
            )

        else:
            await interaction.response.send_message("✅ Bestätigung gespeichert, warte auf Gegner...")

async def handle_queue(interaction, mode):
    global CURRENT_MATCH

    q = queue_dart if mode == "dart" else queue_scolia

    if interaction.user in q:
        await interaction.response.send_message("Schon in Queue", ephemeral=True)
        return

    q.append(interaction.user)

    if len(q) >= 2:
        p1, p2 = q.pop(0), q.pop(0)

        get_rating(p1.id)
        get_rating(p2.id)

        c.execute(
            "INSERT INTO matches (player1_id, player2_id, platform) VALUES (?,?,?)",
            (p1.id, p2.id, mode)
        )
        conn.commit()

        match_id = c.lastrowid

        # 🔥 MATCH SPEICHERN
        CURRENT_MATCH = {
            "p1": p1,
            "p2": p2,
            "mode": mode,
            "id": match_id
        }
        
        msg = await interaction.channel.send(
            f"🎯 Match #{match_id}\n{p1.mention} vs {p2.mention}"
        )

        # 🔥 speichern für späteres Löschen
        global MATCH_MESSAGE_ID, MATCH_CHANNEL_ID
        MATCH_MESSAGE_ID = msg.id
        MATCH_CHANNEL_ID = interaction.channel.id
        

    else:
        await interaction.response.send_message("Beigetreten", ephemeral=True)

    await update_queue(interaction.guild)

# =============================
# COMMANDS
# =============================

@bot.tree.command(name="rebuild_ratings")
@app_commands.checks.has_permissions(administrator=True)
async def rebuild_ratings(interaction: discord.Interaction):

    # =============================
    # RESET
    # =============================

    c.execute("UPDATE players SET rating = 1000")
    c.execute("DELETE FROM monthly_points")

    conn.commit()

    # =============================
    # MATCHES LADEN
    # =============================

    c.execute("""
        SELECT player1_id, player2_id, winner_id, loser_id, timestamp
        FROM matches
        WHERE status='confirmed'
        ORDER BY timestamp ASC
    """)

    matches = c.fetchall()

    # =============================
    # REBUILD
    # =============================

    for p1, p2, winner, loser, timestamp in matches:

        r1 = get_rating(winner)
        r2 = get_rating(loser)

        new_r1 = calculate_elo(r1, r2, 1)
        new_r2 = calculate_elo(r2, r1, 0)

        update_rating(winner, new_r1)
        update_rating(loser, new_r2)

        # 🔥 MONTHLY BERECHNEN
        gain = max(0, new_r1 - r1)

        month = timestamp[:7]  # YYYY-MM

        c.execute("""
            INSERT INTO monthly_points (user_id, month, points)
            VALUES (?, ?, ?)
            ON CONFLICT(user_id, month)
            DO UPDATE SET points = points + ?
        """, (winner, month, gain, gain))

    conn.commit()

    generate_html()
    upload()

    await interaction.response.send_message("🔄 Ratings + Monatsranking komplett neu berechnet")

@bot.tree.command(name="matches")
async def matches(interaction: discord.Interaction):

    c.execute("""
        SELECT id, player1_id, player2_id, status, platform
        FROM matches
        ORDER BY id DESC
        LIMIT 20
    """)

    data = c.fetchall()

    if not data:
        await interaction.response.send_message("Keine Matches vorhanden")
        return

    text = "📋 Letzte Matches:\n\n"

    for mid, p1, p2, status, platform in data:

        user1 = interaction.guild.get_member(p1)
        user2 = interaction.guild.get_member(p2)

        name1 = user1.display_name if user1 else f"User {p1}"
        name2 = user2.display_name if user2 else f"User {p2}"

        status_icon = "✅" if status == "confirmed" else "⏳"

        text += f"{status_icon} Match #{mid}\n"
        text += f"{name1} vs {name2} ({platform})\n\n"

    await interaction.response.send_message(text)

@bot.tree.command(name="edit_result")
@app_commands.checks.has_permissions(administrator=True)
async def edit_result(
    interaction: discord.Interaction,
    match_id: int,
    new_winner: discord.Member,
    new_score: str,
    new_winner_avg: float,
    new_loser_avg: float
):

    # Match holen
    c.execute("""
        SELECT player1_id, player2_id, winner_id, loser_id
        FROM matches
        WHERE id=?
    """, (match_id,))
    match = c.fetchone()

    if not match:
        await interaction.response.send_message("❌ Match nicht gefunden")
        return

    p1, p2, old_winner, old_loser = match

    month = datetime.now().strftime("%Y-%m")

    # =============================
    # 🔥 1. ALTES MATCH RÜCKGÄNGIG
    # =============================

    r_win = get_rating(old_winner)
    r_los = get_rating(old_loser)

    # Rückrechnung
    prev_win = calculate_elo(r_win, r_los, 0)
    prev_los = calculate_elo(r_los, r_win, 1)

    update_rating(old_winner, prev_win)
    update_rating(old_loser, prev_los)

    # Monthly zurückziehen
    old_gain = max(0, r_win - prev_win)

    c.execute("""
        UPDATE monthly_points
        SET points = points - ?
        WHERE user_id=? AND month=?
    """, (old_gain, old_winner, month))

    # =============================
    # 🔥 2. NEUES MATCH BERECHNEN
    # =============================

    new_loser = p1 if new_winner.id == p2 else p2

    r_win = get_rating(new_winner.id)
    r_los = get_rating(new_loser)

    new_win = calculate_elo(r_win, r_los, 1)
    new_los = calculate_elo(r_los, r_win, 0)

    update_rating(new_winner.id, new_win)
    update_rating(new_loser, new_los)

    # Monthly neu hinzufügen
    new_gain = max(0, new_win - r_win)

    c.execute("""
        INSERT INTO monthly_points (user_id, month, points)
        VALUES (?, ?, ?)
        ON CONFLICT(user_id, month)
        DO UPDATE SET points = points + ?
    """, (new_winner.id, month, new_gain, new_gain))

    # =============================
    # 🔥 MATCH UPDATE
    # =============================

    c.execute("""
        UPDATE matches SET
        winner_id=?,
        loser_id=?,
        score=?,
        winner_avg=?,
        loser_avg=?
        WHERE id=?
    """, (
        new_winner.id,
        new_loser,
        new_score,
        new_winner_avg,
        new_loser_avg,
        match_id
    ))

    conn.commit()

    generate_html()
    upload()

    await interaction.response.send_message("✅ Match komplett korrekt neu berechnet")

@bot.tree.command(name="queue_panel")
async def queue_panel(interaction: discord.Interaction):

    global QUEUE_MESSAGE_ID, QUEUE_CHANNEL_ID

    embed = discord.Embed(
        title="🎯 RANKED DARTS",
        description="Live Queue wird geladen..."
    )

    await interaction.response.send_message(embed=embed, view=QueueView())

    msg = await interaction.original_response()

    QUEUE_MESSAGE_ID = msg.id
    QUEUE_CHANNEL_ID = interaction.channel.id

    await update_queue(interaction.guild)

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
async def result(
    interaction: discord.Interaction,
    match_id: int,
    winner: discord.Member,
    score: str,
    winner_avg: float,
    loser_avg: float,
    screenshot: discord.Attachment
):

    # ❌ Screenshot prüfen
    if not screenshot:
        await interaction.response.send_message("❌ Du musst einen Screenshot hochladen!")
        return

    # Optional: prüfen ob Bild
    if not screenshot.content_type.startswith("image"):
        await interaction.response.send_message("❌ Datei muss ein Bild sein!")
        return

    # Match holen
    c.execute("SELECT player1_id, player2_id FROM matches WHERE id=?", (match_id,))
    match = c.fetchone()

    if not match:
        await interaction.response.send_message("Match nicht gefunden")
        return

    p1, p2 = match

    # 🔥 CHECK: Nur Spieler dürfen Ergebnis eintragen
    if interaction.user.id not in [p1, p2]:
        await interaction.response.send_message(
            "❌ Du bist nicht Teil dieses Matches!",
            ephemeral=True
        )
        return

    loser_id = p1 if winner.id == p2 else p2

    # ELO
    r1 = get_rating(winner.id)
    r2 = get_rating(loser_id)

    new_r1 = calculate_elo(r1, r2, 1)
    new_r2 = calculate_elo(r2, r1, 0)

    elo_gain = new_r1 - r1

    update_rating(winner.id, new_r1)
    update_rating(loser_id, new_r2)

    # Monthly
    month = datetime.now().strftime("%Y-%m")
    gain = max(0, new_r1 - r1)

    c.execute("""
        INSERT INTO monthly_points (user_id, month, points)
        VALUES (?, ?, ?)
        ON CONFLICT(user_id, month)
        DO UPDATE SET points = points + ?
    """, (winner.id, month, gain, gain))

    # Match speichern
    c.execute("""
        UPDATE matches SET
        winner_id=?, loser_id=?, score=?, winner_avg=?, loser_avg=?, elo_change=?, status='confirmed'
        WHERE id=?
    """, (
        winner.id,
        loser_id,
        score,
        winner_avg,
        loser_avg,
        elo_gain,
        match_id
    ))

    conn.commit()

    # 🔥 Screenshot anzeigen
    embed = discord.Embed(title="📊 Match Ergebnis")
    embed.add_field(name="Match ID", value=match_id)
    embed.add_field(name="Gewinner", value=winner.display_name)
    embed.add_field(name="Score", value=score)
    embed.set_image(url=screenshot.url)

    generate_html()
    upload()

    # =============================
    # MATCH MESSAGE LÖSCHEN
    # =============================

    global MATCH_MESSAGE_ID, MATCH_CHANNEL_ID, CURRENT_MATCH, MATCH_CONFIRMATIONS

    try:
        if MATCH_MESSAGE_ID and MATCH_CHANNEL_ID:
            channel = interaction.guild.get_channel(MATCH_CHANNEL_ID)
            msg = await channel.fetch_message(MATCH_MESSAGE_ID)
            await msg.delete()
    except:
        pass

    # 🔄 Reset
    CURRENT_MATCH = None
    MATCH_CONFIRMATIONS.clear()
    MATCH_MESSAGE_ID = None
    MATCH_CHANNEL_ID = None

    # Panel aktualisieren
    await update_queue(interaction.guild)

    await interaction.response.send_message(embed=embed)

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

    for p1, p2, winner, score, platform, elo_gain in c.execute("""
        SELECT player1_id, player2_id, winner_id, score, platform, elo_change
        FROM matches
        WHERE status='confirmed'
        AND (player1_id=? OR player2_id=?)
        ORDER BY id DESC
        LIMIT 10
    """, (player.id, player.id)):

        opponent_id = p2 if player.id == p1 else p1
        opponent = interaction.guild.get_member(opponent_id)

        name = opponent.display_name if opponent else f"User {opponent_id}"

        elo_gain = elo_gain if elo_gain else 0

        if winner == player.id:
            result = "🏆 Win"
            elo_text = f"+{elo_gain}"
        else:
            result = "❌ Loss"
            elo_text = f"-{elo_gain}"

        text += f"{result} vs {name} ({platform}) ({elo_text} ELO)\n"
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

@bot.tree.command(name="export_matches")
@app_commands.checks.has_permissions(administrator=True)
async def export_matches(interaction: discord.Interaction):

    import csv

    c.execute("""
        SELECT id, player1_id, player2_id, winner_id, loser_id,
               score, winner_avg, loser_avg, platform, status, timestamp
        FROM matches
        ORDER BY id DESC
    """)

    data = c.fetchall()

    if not data:
        await interaction.response.send_message("Keine Matches vorhanden")
        return

    filename = "matches_export.csv"

    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)

        # Header
        writer.writerow([
            "Match ID",
            "Spieler 1",
            "Spieler 2",
            "Gewinner",
            "Verlierer",
            "Score",
            "Winner Avg",
            "Loser Avg",
            "Plattform",
            "Status",
            "Datum"
        ])

        for mid, p1, p2, winner, loser, score, wa, la, platform, status, timestamp in data:

            user1 = interaction.guild.get_member(p1)
            user2 = interaction.guild.get_member(p2)
            win_user = interaction.guild.get_member(winner)
            los_user = interaction.guild.get_member(loser)

            name1 = user1.display_name if user1 else f"User {p1}"
            name2 = user2.display_name if user2 else f"User {p2}"
            name_win = win_user.display_name if win_user else f"User {winner}"
            name_los = los_user.display_name if los_user else f"User {loser}"

            writer.writerow([
                mid,
                name1,
                name2,
                name_win,
                name_los,
                score,
                wa,
                la,
                platform,
                status,
                timestamp
            ])

    await interaction.response.send_message(
        content="📄 Export fertig:",
        file=discord.File(filename)
    )

@bot.tree.command(name="export_bad_matches")
@app_commands.checks.has_permissions(administrator=True)
async def export_bad_matches(interaction: discord.Interaction):

    import csv

    c.execute("""
        SELECT id, player1_id, player2_id, winner_id, loser_id,
               score, winner_avg, loser_avg, platform, status, timestamp
        FROM matches
        WHERE status='confirmed'
        AND (
            winner_avg IS NULL OR
            loser_avg IS NULL OR
            winner_avg < 10 OR
            loser_avg < 10 OR
            winner_avg > 150 OR
            loser_avg > 150 OR
            score IS NULL OR
            score = ''
        )
        ORDER BY id DESC
    """)

    data = c.fetchall()

    if not data:
        await interaction.response.send_message("✅ Keine fehlerhaften Matches gefunden!")
        return

    filename = "bad_matches.csv"

    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)

        writer.writerow([
            "Match ID",
            "Spieler 1",
            "Spieler 2",
            "Gewinner",
            "Verlierer",
            "Score",
            "Winner Avg",
            "Loser Avg",
            "Plattform",
            "Status",
            "Datum",
            "Fehler"
        ])

        for mid, p1, p2, winner, loser, score, wa, la, platform, status, timestamp in data:

            user1 = interaction.guild.get_member(p1)
            user2 = interaction.guild.get_member(p2)
            win_user = interaction.guild.get_member(winner)
            los_user = interaction.guild.get_member(loser)

            name1 = user1.display_name if user1 else f"User {p1}"
            name2 = user2.display_name if user2 else f"User {p2}"
            name_win = win_user.display_name if win_user else f"User {winner}"
            name_los = los_user.display_name if los_user else f"User {loser}"

            # 🔥 Fehlerbeschreibung
            errors = []

            if wa is None or la is None:
                errors.append("Missing Average")

            if wa and wa < 10:
                errors.append("Winner Avg zu niedrig")

            if la and la < 10:
                errors.append("Loser Avg zu niedrig")

            if wa and wa > 150:
                errors.append("Winner Avg zu hoch")

            if la and la > 150:
                errors.append("Loser Avg zu hoch")

            if not score:
                errors.append("Score fehlt")

            writer.writerow([
                mid,
                name1,
                name2,
                name_win,
                name_los,
                score,
                wa,
                la,
                platform,
                status,
                timestamp,
                ", ".join(errors)
            ])

    await interaction.response.send_message(
        content=f"⚠️ {len(data)} fehlerhafte Matches gefunden:",
        file=discord.File(filename)
    )

@bot.tree.command(name="quick_edit")
@app_commands.checks.has_permissions(administrator=True)
async def quick_edit(
    interaction: discord.Interaction,
    match_id: int,
    score: str,
    winner_avg: float,
    loser_avg: float
):

    # Match holen
    c.execute("""
        SELECT id FROM matches WHERE id=?
    """, (match_id,))
    match = c.fetchone()

    if not match:
        await interaction.response.send_message("❌ Match nicht gefunden")
        return

    # 🔥 NUR DATEN ÄNDERN (KEIN ELO!)
    c.execute("""
        UPDATE matches SET
        score=?,
        winner_avg=?,
        loser_avg=?
        WHERE id=?
    """, (
        score,
        winner_avg,
        loser_avg,
        match_id
    ))

    conn.commit()

    generate_html()
    upload()

    await interaction.response.send_message(
        f"⚡ Match #{match_id} schnell bearbeitet (ohne ELO Änderung)"
    )

# =============================
# READY
# =============================

@bot.event
async def on_ready():
    await bot.tree.sync()

    bot.add_view(QueueView())

    generate_html()
    upload()

    print("Bot online (Persistent Queue aktiv)")

bot.run(TOKEN)