import discord
from discord import app_commands
from discord.ext import commands
import sqlite3
import math

import os
TOKEN = os.getenv("DISCORD_TOKEN")

intents = discord.Intents.default()
intents.members = True

bot = commands.Bot(command_prefix="!", intents=intents)

# Datenbank
conn = sqlite3.connect("dartliga.db")
c = conn.cursor()

c.execute("""CREATE TABLE IF NOT EXISTS players (
    user_id INTEGER PRIMARY KEY,
    rating INTEGER
)""")

conn.commit()

c.execute("""CREATE TABLE IF NOT EXISTS matches (
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
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
)""")

conn.commit()

queue = []

K_FACTOR = 32

def get_rating(user_id):
    c.execute("SELECT rating FROM players WHERE user_id = ?", (user_id,))
    result = c.fetchone()
    if result:
        return result[0]
    else:
        c.execute("INSERT INTO players (user_id, rating) VALUES (?, ?)", (user_id, 1000))
        conn.commit()
        return 1000

def update_rating(user_id, new_rating):
    c.execute("UPDATE players SET rating = ? WHERE user_id = ?", (new_rating, user_id))
    conn.commit()

def calculate_elo(r1, r2, score1):
    expected1 = 1 / (1 + 10 ** ((r2 - r1) / 400))
    return round(r1 + K_FACTOR * (score1 - expected1))

@bot.event
async def on_ready():
    await bot.tree.sync()
    print(f"{bot.user} ist online!")

@bot.tree.command(name="queue", description="Tritt der Matchmaking-Warteschlange bei")
async def join_queue(interaction: discord.Interaction):
    user = interaction.user

    if user in queue:
        await interaction.response.send_message("Du bist bereits in der Warteschlange.", ephemeral=True)
        return

    queue.append(user)

    if len(queue) >= 2:
        player1 = queue.pop(0)
        player2 = queue.pop(0)

        # Match in Datenbank erstellen
        c.execute("""INSERT INTO matches (player1_id, player2_id)
                     VALUES (?, ?)""", (player1.id, player2.id))
        conn.commit()

        match_id = c.lastrowid

        await interaction.response.send_message(
            f"🎯 **MATCH #{match_id} GEFUNDEN!**\n"
            f"{player1.mention} vs {player2.mention}\n\n"
            f"Nach dem Spiel:\n"
            f"`/result match_id:{match_id}` verwenden!"
        )
    else:
        await interaction.response.send_message("Du bist der Warteschlange beigetreten. Warte auf einen Gegner...")

pending_results = {}

class ConfirmView(discord.ui.View):
    def __init__(self, winner, loser, r1, r2, match_id):
        super().__init__(timeout=60)
        self.winner = winner
        self.loser = loser
        self.r1 = r1
        self.r2 = r2
        self.match_id = match_id

    @discord.ui.button(label="✅ Bestätigen", style=discord.ButtonStyle.green)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.loser.id:
            await interaction.response.send_message("Nur der Gegner kann bestätigen!", ephemeral=True)
            return

        new_r1 = calculate_elo(self.r1, self.r2, 1)
        new_r2 = calculate_elo(self.r2, self.r1, 0)

        update_rating(self.winner.id, new_r1)
        update_rating(self.loser.id, new_r2)

        c.execute("""UPDATE matches SET
            winner_id = ?, loser_id = ?,
            winner_old_rating = ?, winner_new_rating = ?,
            loser_old_rating = ?, loser_new_rating = ?,
            status = 'confirmed'
            WHERE id = ?""",
            (self.winner.id, self.loser.id,
             self.r1, new_r1,
             self.r2, new_r2,
             self.match_id))
        conn.commit()

        await interaction.response.edit_message(
            content=f"🏆 Match #{self.match_id} bestätigt!\n"
                    f"📈 {self.winner.display_name}: {self.r1} → {new_r1}\n"
                    f"📉 {self.loser.display_name}: {self.r2} → {new_r2}",
            view=None
        )

    @discord.ui.button(label="❌ Ablehnen", style=discord.ButtonStyle.red)
    async def reject(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user != self.loser:
            await interaction.response.send_message("Nur der Gegner kann ablehnen!", ephemeral=True)
            return

        await interaction.response.edit_message(
            content="❌ Ergebnis wurde abgelehnt. Bitte klärt das Match manuell.",
            view=None
        )
@bot.tree.command(name="result", description="Melde das Ergebnis eines Matches")
@app_commands.describe(
    match_id="Die Match-ID",
    winner="Gewinner des Spiels",
    screenshot="Screenshot vom Ergebnis (Pflicht)"
)
async def result(interaction: discord.Interaction, match_id: int, winner: discord.Member, screenshot: discord.Attachment):

    c.execute("SELECT player1_id, player2_id, status FROM matches WHERE id = ?", (match_id,))
    match = c.fetchone()

    if not match:
        await interaction.response.send_message("❌ Match-ID nicht gefunden.", ephemeral=True)
        return

    player1_id, player2_id, status = match

    if status != "pending":
        await interaction.response.send_message("❌ Dieses Match wurde bereits abgeschlossen.", ephemeral=True)
        return

    if winner.id not in [player1_id, player2_id]:
        await interaction.response.send_message("❌ Dieser Spieler gehört nicht zu diesem Match.", ephemeral=True)
        return

    if not screenshot.content_type.startswith("image"):
        await interaction.response.send_message("❌ Bitte lade ein gültiges Bild hoch!", ephemeral=True)
        return

    loser_id = player1_id if winner.id == player2_id else player2_id
    loser = await bot.fetch_user(loser_id)

    r1 = get_rating(winner.id)
    r2 = get_rating(loser_id)

    view = ConfirmView(winner, loser, r1, r2, match_id)

    embed = discord.Embed(
        title=f"🎯 Ergebnis für Match #{match_id}",
        description=f"{winner.mention} meldet einen Sieg gegen {loser.mention}\n\n"
                    f"{loser.mention}, bitte bestätige das Ergebnis.",
        color=discord.Color.blue()
    )

    embed.set_image(url=screenshot.url)

    await interaction.response.send_message(embed=embed, view=view)


@bot.tree.command(name="leaderboard", description="Zeigt die Rangliste")
async def leaderboard(interaction: discord.Interaction):
    c.execute("SELECT user_id, rating FROM players ORDER BY rating DESC LIMIT 10")
    top = c.fetchall()

    text = "🏆 **Dart Liga Ranking**\n\n"
    for i, (user_id, rating) in enumerate(top, start=1):
        user = await bot.fetch_user(user_id)
        text += f"{i}. {user.name} – {rating}\n"

    await interaction.response.send_message(text)

@bot.tree.command(name="history", description="Zeigt die letzten 5 Matches")
async def history(interaction: discord.Interaction):

    c.execute("""SELECT winner_id, loser_id, winner_new_rating, loser_new_rating, timestamp
                 FROM matches
                 ORDER BY id DESC
                 LIMIT 5""")

    matches = c.fetchall()

    if not matches:
        await interaction.response.send_message("Noch keine Matches gespielt.")
        return

    text = "📊 **Letzte 5 Matches:**\n\n"

    for winner_id, loser_id, w_rating, l_rating, timestamp in matches:
        winner = await bot.fetch_user(winner_id)
        loser = await bot.fetch_user(loser_id)

        text += f"🏆 {winner.name} vs {loser.name}\n"
        text += f"   ➜ {w_rating} - {l_rating} ({timestamp})\n\n"

    await interaction.response.send_message(text)
@bot.tree.command(name="stats", description="Zeigt die Statistiken eines Spielers")
@app_commands.describe(player="Der Spieler, dessen Stats angezeigt werden sollen")
async def stats(interaction: discord.Interaction, player: discord.Member):

    rating = get_rating(player.id)

    # Siege zählen
    c.execute("SELECT COUNT(*) FROM matches WHERE winner_id = ?", (player.id,))
    wins = c.fetchone()[0]

    # Niederlagen zählen
    c.execute("SELECT COUNT(*) FROM matches WHERE loser_id = ?", (player.id,))
    losses = c.fetchone()[0]

    total = wins + losses

    if total > 0:
        winrate = round((wins / total) * 100, 1)
    else:
        winrate = 0

    embed = discord.Embed(
        title=f"📊 Stats von {player.display_name}",
        color=discord.Color.green()
    )

    embed.add_field(name="🏆 Rating", value=rating, inline=False)
    embed.add_field(name="🎯 Spiele", value=total, inline=True)
    embed.add_field(name="✅ Siege", value=wins, inline=True)
    embed.add_field(name="❌ Niederlagen", value=losses, inline=True)
    embed.add_field(name="📈 Winrate", value=f"{winrate}%", inline=False)

    await interaction.response.send_message(embed=embed)
bot.run(TOKEN)