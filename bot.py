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
# GITHUB AUTO UPLOAD
# =============================

def upload_to_github():
    try:
        os.system("git add .")
        os.system('git commit -m "auto update leaderboard"')
        os.system("git push")
        print("✅ GitHub Upload")
    except:
        pass

# =============================
# HTML GENERATION
# =============================

def generate_leaderboard_html():
    c.execute("SELECT user_id, rating FROM players ORDER BY rating DESC")
    players = c.fetchall()

    guild = bot.guilds[0] if bot.guilds else None

    html = """
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<meta http-equiv="refresh" content="30">
<style>
body { background:#0d0d0d; color:white; font-family:Arial; text-align:center; }
h1 { color:#00ffe1; }
table { margin:auto; width:80%; border-collapse:collapse; }
th, td { padding:10px; border-bottom:1px solid #333; }
th { background:#00ffe1; color:black; }
</style>
</head>
<body>
<h1>🏆 World Ranking</h1>
<table>
<tr><th>#</th><th>Spieler</th><th>ELO</th></tr>
"""

    for i, (user_id, rating) in enumerate(players, start=1):

        name = f"User {user_id}"

        if guild:
            member = guild.get_member(user_id)
            if member:
                name = member.display_name
            else:
                user = bot.get_user(user_id)
                if user:
                    name = user.name

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

    html = f"""
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<meta http-equiv="refresh" content="30">
<style>
body {{ background:#0d0d0d; color:white; font-family:Arial; text-align:center; }}
h1 {{ color:#ff00ff; }}
table {{ margin:auto; width:80%; border-collapse:collapse; }}
th, td {{ padding:10px; border-bottom:1px solid #333; }}
th {{ background:#ff00ff; color:black; }}
</style>
</head>
<body>
<h1>🏆 Monatsranking ({current_month})</h1>
<table>
<tr><th>#</th><th>Spieler</th><th>Punkte</th></tr>
"""

    for i, (user_id, points) in enumerate(players, start=1):

        name = f"User {user_id}"

        if guild:
            member = guild.get_member(user_id)
            if member:
                name = member.display_name
            else:
                user = bot.get_user(user_id)
                if user:
                    name = user.name

        html += f"<tr><td>{i}</td><td>{name}</td><td>{points}</td></tr>"

    html += "</table></body></html>"

    with open("monthly.html", "w", encoding="utf-8") as f:
        f.write(html)

# =============================
# RATING SYSTEM
# =============================

K_FACTOR = 32

def get_rating(user_id):
    c.execute("SELECT rating FROM players WHERE user_id=?", (user_id,))
    result = c.fetchone()
    if result:
        return result[0]
    else:
        c.execute("INSERT INTO players (user_id, rating) VALUES (?, ?)", (user_id, 1000))
        conn.commit()
        return 1000

def update_rating(user_id, new_rating):
    c.execute("UPDATE players SET rating=? WHERE user_id=?", (new_rating, user_id))
    conn.commit()

def calculate_elo(r1, r2, score1):
    expected = 1 / (1 + 10 ** ((r2 - r1) / 400))
    return round(r1 + K_FACTOR * (score1 - expected))

# =============================
# QUEUE SYSTEM
# =============================

queue_dartcounter = []
queue_scolia = []

class QueueView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="🎯 DartCounter", style=discord.ButtonStyle.green)
    async def dartcounter(self, interaction: discord.Interaction, button: discord.ui.Button):
        await handle_queue(interaction, "dartcounter")

    @discord.ui.button(label="🔵 Scolia", style=discord.ButtonStyle.blurple)
    async def scolia(self, interaction: discord.Interaction, button: discord.ui.Button):
        await handle_queue(interaction, "scolia")

async def handle_queue(interaction, platform):
    user = interaction.user
    queue = queue_dartcounter if platform == "dartcounter" else queue_scolia

    if user in queue:
        await interaction.response.send_message("Du bist bereits in der Queue.", ephemeral=True)
        return

    queue.append(user)

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
            f"🎯 MATCH #{match_id}\n{p1.mention} vs {p2.mention}\n\n/result match_id:{match_id}"
        )
    else:
        await interaction.response.send_message("Du bist der Queue beigetreten.", ephemeral=True)

# =============================
# CONFIRM VIEW
# =============================

class ConfirmView(discord.ui.View):
    def __init__(self, winner, loser, r1, r2, match_id, score):
        super().__init__(timeout=600)
        self.winner = winner
        self.loser = loser
        self.r1 = r1
        self.r2 = r2
        self.match_id = match_id
        self.score = score

    @discord.ui.button(label="✅ Bestätigen", style=discord.ButtonStyle.green)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):

        new_r1 = calculate_elo(self.r1, self.r2, 1)
        new_r2 = calculate_elo(self.r2, self.r1, 0)

        update_rating(self.winner.id, new_r1)
        update_rating(self.loser.id, new_r2)

        current_month = datetime.now().strftime("%Y-%m")
        elo_gain = max(0, new_r1 - self.r1)

        c.execute("""
            INSERT INTO monthly_points (user_id, month, points)
            VALUES (?, ?, ?)
            ON CONFLICT(user_id, month)
            DO UPDATE SET points = points + ?
        """, (self.winner.id, current_month, elo_gain, elo_gain))

        c.execute("""
            UPDATE matches SET
                winner_id=?, loser_id=?,
                winner_old_rating=?, winner_new_rating=?,
                loser_old_rating=?, loser_new_rating=?,
                score=?, status='confirmed'
            WHERE id=?
        """, (self.winner.id, self.loser.id,
              self.r1, new_r1,
              self.r2, new_r2,
              self.score,
              self.match_id))

        conn.commit()

        generate_leaderboard_html()
        generate_monthly_leaderboard_html()
        upload_to_github()

        await interaction.response.edit_message(content="🏆 Match bestätigt!", view=None)

# =============================
# COMMANDS
# =============================

@bot.tree.command(name="queue_panel")
async def queue_panel(interaction: discord.Interaction):
    embed = discord.Embed(title="🎯 Dart Liga Matchmaking")
    await interaction.response.send_message(embed=embed, view=QueueView())

@bot.tree.command(name="stats")
@app_commands.describe(player="Spieler auswählen")
async def stats(interaction: discord.Interaction, player: discord.Member):

    rating = get_rating(player.id)

    c.execute("SELECT COUNT(*) FROM matches WHERE winner_id=? AND status='confirmed'", (player.id,))
    wins = c.fetchone()[0]

    c.execute("SELECT COUNT(*) FROM matches WHERE loser_id=? AND status='confirmed'", (player.id,))
    losses = c.fetchone()[0]

    total = wins + losses
    winrate = round((wins / total) * 100, 1) if total > 0 else 0

    embed = discord.Embed(title=f"📊 Stats von {player.display_name}", color=discord.Color.green())

    embed.add_field(name="🏆 Rating", value=rating, inline=False)
    embed.add_field(name="🎯 Spiele", value=total, inline=True)
    embed.add_field(name="✅ Siege", value=wins, inline=True)
    embed.add_field(name="❌ Niederlagen", value=losses, inline=True)
    embed.add_field(name="📈 Winrate", value=f"{winrate}%", inline=False)

    await interaction.response.send_message(embed=embed)

@bot.tree.command(name="result")
async def result(interaction: discord.Interaction, match_id: int, winner: discord.Member, score: str):

    c.execute("SELECT player1_id, player2_id FROM matches WHERE id=?", (match_id,))
    match = c.fetchone()

    if not match:
        await interaction.response.send_message("Match nicht gefunden.")
        return

    p1, p2 = match
    loser_id = p1 if winner.id == p2 else p2
    loser = await bot.fetch_user(loser_id)

    r1 = get_rating(winner.id)
    r2 = get_rating(loser_id)

    view = ConfirmView(winner, loser, r1, r2, match_id, score)

    await interaction.response.send_message("Bestätigen:", view=view)

# =============================
# READY
# =============================

@bot.event
async def on_ready():
    await bot.tree.sync()

    c.execute("SELECT player1_id, player2_id FROM matches")
    for p1, p2 in c.fetchall():
        get_rating(p1)
        get_rating(p2)

    conn.commit()

    generate_leaderboard_html()
    generate_monthly_leaderboard_html()
    upload_to_github()

    print(f"{bot.user} ist online!")

bot.run(TOKEN)