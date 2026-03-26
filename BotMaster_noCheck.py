# SnowMaster (fusion GUI + runner) - version avec Flask pour heartbeats
import sys
import os


# ==================== REDIRECTION STDOUT/STDERR POUR .EXE ====================
# Quand packagé en .exe sans console (mode windowed), sys.stdout/stderr sont None.
# Les appels à print() sur None peuvent causer des crashs après quelques minutes.
# On redirige vers os.devnull pour éviter ce problème.
class _NullWriter:
    """Faux stream en écriture pour éviter tout crash si devnull indisponible."""

    def write(self, s): ...
    def flush(self): ...


if getattr(sys, "frozen", False):
    # Mode .exe packagé (PyInstaller)
    try:
        if sys.stdout is None or not hasattr(sys.stdout, "write"):
            try:
                sys.stdout = open(os.devnull, "w", encoding="utf-8")
            except Exception:
                sys.stdout = _NullWriter()
        if sys.stderr is None or not hasattr(sys.stderr, "write"):
            try:
                sys.stderr = open(os.devnull, "w", encoding="utf-8")
            except Exception:
                sys.stderr = _NullWriter()
    except Exception:
        if sys.stdout is None:
            sys.stdout = _NullWriter()
        if sys.stderr is None:
            sys.stderr = _NullWriter()

import threading
import time
import json
import tempfile
import subprocess
import ctypes
from collections import deque
from datetime import datetime
from typing import Dict, Deque, Optional, List, Tuple
import shlex
import logging
from concurrent.futures import ThreadPoolExecutor
import traceback


# ==================== CRASH HANDLER GLOBAL ====================
# Écrit les exceptions non capturées dans un fichier crash.log
# pour faciliter le debug dans un .exe
def _get_crash_log_path():
    if getattr(sys, "frozen", False):
        # En .exe : écrire dans ANKADIR (même dossier que logs) pour cohérence
        try:
            return os.path.join(ANKADIR, "crash.log")
        except Exception:
            base = os.path.dirname(sys.executable)
            return os.path.join(base, "crash.log")
    base = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(base, "crash.log")


def _crash_handler(exc_type, exc_value, exc_tb):
    """Handler global pour les exceptions non capturées."""
    crash_log = _get_crash_log_path()
    try:
        with open(crash_log, "a", encoding="utf-8") as f:
            f.write(f"\n{'='*60}\n")
            f.write(f"CRASH @ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"{'='*60}\n")
            f.write("".join(traceback.format_exception(exc_type, exc_value, exc_tb)))
            f.write("\n")
    except Exception:
        pass
    # Appeler le handler par défaut
    sys.__excepthook__(exc_type, exc_value, exc_tb)


sys.excepthook = _crash_handler

# Handler pour les threads (les exceptions dans les threads ne passent pas par sys.excepthook)
_original_threading_excepthook = threading.excepthook


def _thread_crash_handler(args):
    """Handler pour les exceptions non capturées dans les threads."""
    crash_log = _get_crash_log_path()
    try:
        with open(crash_log, "a", encoding="utf-8") as f:
            f.write(f"\n{'='*60}\n")
            f.write(f"THREAD CRASH @ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Thread: {args.thread.name if args.thread else 'Unknown'}\n")
            f.write(f"{'='*60}\n")
            if args.exc_type and args.exc_value and args.exc_traceback:
                f.write(
                    "".join(
                        traceback.format_exception(
                            args.exc_type, args.exc_value, args.exc_traceback
                        )
                    )
                )
            f.write("\n")
    except Exception:
        pass
    # Appeler le handler par défaut
    _original_threading_excepthook(args)


threading.excepthook = _thread_crash_handler

from ctypes import wintypes

import struct, time
import win32gui, win32api, win32con

# Flask pour recevoir heartbeats / locks
from flask import Flask, request, jsonify

# GUI libs
from PySide6.QtCore import (
    Qt,
    QTimer,
    Signal,
    QObject,
    QSize,
    QEvent,
    QItemSelectionModel,
    QCoreApplication,
    QMetaObject,
)
from PySide6.QtWidgets import (
    QApplication,
    QWidget,
    QHBoxLayout,
    QVBoxLayout,
    QListWidget,
    QListWidgetItem,
    QLabel,
    QPushButton,
    QFileDialog,
    QSplitter,
    QGroupBox,
    QFormLayout,
    QInputDialog,
    QAbstractItemView,
    QListView,
    QMessageBox,
    QSizePolicy,
    QGridLayout,
    QCheckBox,
    QSpinBox,
    QDialog,
    QGraphicsOpacityEffect,
    QGraphicsDropShadowEffect,
    QSystemTrayIcon,
    QTextEdit,
    QLineEdit,
    QProgressBar,
    QFrame,
)
from PySide6.QtGui import QIcon, QColor
from PySide6.QtWidgets import QStyledItemDelegate
from PySide6.QtGui import QPainter
from PySide6.QtWidgets import QStyle
from PySide6.QtGui import QIcon, QMouseEvent


from PySide6.QtWidgets import QLineEdit
from PySide6.QtGui import QIntValidator

from PySide6.QtCore import QPropertyAnimation, QEasingCurve
from PySide6.QtWidgets import QGraphicsOpacityEffect


# system libs used by runner
import psutil
import win32gui, win32process, win32con, win32api, win32security
import socket

# runner automation libs
import pyautogui
import pyperclip

# networking helper
import urllib.request

import re

# add at top of file:
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

# Bot Discord pour effacer les messages
# import discord
# from discord.ext import commands
import asyncio

# ========================= CONFIG =========================
if getattr(sys, "frozen", False):
    # quand PyInstaller packagé : le répertoire utile est celui de l'exécutable
    _BOT_ROOT_DEFAULT = os.path.dirname(sys.executable)
else:
    # en dev : le répertoire du script (dossier SnowMaster)
    _BOT_ROOT_DEFAULT = os.path.dirname(os.path.abspath(__file__))

# Dossier de l'application (configs, images, autopilot) = dossier courant du script / exécutable
ANKADIR = _BOT_ROOT_DEFAULT
# Fichier de préférences : Settings.json (créé s'il n'existe pas au premier chargement)
SETTINGS_BASENAME = "settings.json"
IMAGES_DIR = os.path.join(ANKADIR, "images")
CONFIGS_DIR = os.path.join(ANKADIR, "configs")
AUTOPILOT_DIR = os.path.join(ANKADIR, "autopilot")
PREFS_FILE = os.path.join(CONFIGS_DIR, SETTINGS_BASENAME)
INSTANCES_FILE = os.path.join(CONFIGS_DIR, "instances.json")
HOLDINGS_STATE_PATH = os.path.join(CONFIGS_DIR, "holdings.json")

# -------------------- Logging fichier --------------------
# S'assurer que le dossier existe AVANT de créer les handlers
try:
    os.makedirs(ANKADIR, exist_ok=True)
    os.makedirs(IMAGES_DIR, exist_ok=True)
    os.makedirs(CONFIGS_DIR, exist_ok=True)
except Exception as e:
    # Log dans crash.log si le dossier ne peut pas être créé
    try:
        with open(_get_crash_log_path(), "a", encoding="utf-8") as f:
            f.write(f"[WARN] Impossible de créer ANKADIR: {ANKADIR} - {e}\n")
    except Exception:
        pass

logger = logging.getLogger("SnowMaster")
logger.setLevel(logging.DEBUG)

# Plus d'écriture dans un fichier logs.txt : on attache uniquement un NullHandler
if not logger.handlers:
    logger.addHandler(logging.NullHandler())

scan_pids_logger = logging.getLogger("ScanPIDs")
scan_pids_logger.setLevel(logging.DEBUG)
# Plus de fichier scan_pids_debug.log : uniquement NullHandler (et print console dans scan_log)
if not scan_pids_logger.handlers:
    scan_pids_logger.addHandler(logging.NullHandler())


def scan_log(msg):
    """Log du scan des PIDs (console uniquement, plus de fichier dédié)."""
    try:
        scan_pids_logger.info(msg)
        print(msg)
    except Exception:
        pass


def app_log_debug(msg, *args, **kwargs):
    try:
        logger.debug(msg, *args, **kwargs)
    except Exception:
        pass


def app_log_info(msg, *args, **kwargs):
    try:
        logger.info(msg, *args, **kwargs)
    except Exception:
        pass


def app_log_warn(msg, *args, **kwargs):
    try:
        logger.warning(msg, *args, **kwargs)
    except Exception:
        pass


def app_log_error(msg, *args, **kwargs):
    try:
        logger.error(msg, *args, **kwargs)
    except Exception:
        pass


# Valeurs par défaut (utilisées seulement si absentes du JSON)
# Dossier d'images par défaut : sous-répertoire "images" du dossier courant (SnowMaster).
RESOURCES = IMAGES_DIR
# Exécutable client par défaut (en pratique surchargé par prefs['exe'])
EXE = "Nvidia.exe"

API_HOST = "127.0.0.1"
API_PORT = 8787
HEARTBEAT_RED_S = 480  # Délai par défaut avant qu'un voyant passe au rouge (8 minutes)

# Contrôleur utilisé par le bouton PANIC (chemin vers le script contrôleur global)
PANIC_CONTROLLER_PATH = ""
MAX_LOGS_PER_INSTANCE = 1000
CARD_HEIGHT = 62  # compact
CARD_WIDTH = 320

# ===== Persistence des holdings (kamas) =====


# Couleurs voyants
CLR_GREEN = "#10b981"
CLR_YELLOW = "#f59e0b"
CLR_RED = "#ef4444"
CLR_GREY = "#6b7280"
CLR_PURPLE = "#8b5cf6"  # violet pour instances restaurées récemment

CLR_BLUE = "#3b82f6"  #  bleu pour les instances "vides" lancées manuellement ---

# ===== PRICES SCRAPING SCOPE =====
ALLOWED_SERVERS_DISPLAY = [
    "Mikhal",
    "Dakal",
    "Kourial",
    "Rafal",
    "Salar",
    "Brial",
    "Imagiro",
    "Orukam",
    "Draconiros",
    "Hell Mina",
    "Tylezia",
    "Ombre",
    "Tal Kasha",
]

# Mapping “nom à afficher” -> “nom tel qu’il apparaît sur le site”
SERVER_SCRAPE_NAME = {
    "Hell Mina": "HellMina",
    "Ombre": "Ombre(Shadow)",
    "Tal Kasha": "TalKasha",
}

# ======== AUTO RELAUNCH CONFIG ========
AUTO_RELAUNCH_DEFAULT = True
AUTO_RELAUNCH_COOLDOWN_S = 10  # anti-spam par instance

# ======== SERVER DISPLAY ORDER ========
SERVER_KAMAS_DISPLAY_ORDER = [
    "Mikhal",
    "Kourial",
    "Dakal",
    "Draconiros",
    "Rafal",
    "Salar",
    "Brial",
    "Hell Mina",
    "Orukam",
    "Imagiro",
    "Tylezia",
    "Ombre",
]


def iter_servers_in_display_order(keys_iterable):
    keys = list(keys_iterable) if keys_iterable else []
    seen = set()
    for name in SERVER_KAMAS_DISPLAY_ORDER:
        if name in keys and name not in seen:
            seen.add(name)
            yield name
    for name in keys:
        if name not in seen:
            seen.add(name)
            yield name


# ======== PREFS / AUTOPILOT / DISCORD ========
DEFAULT_PREFS = {
    # Chemin du répertoire de l'instance (ex. .../SnowbotInstances/1/). Vide = déduit du script/exe.
    "bot_root": "",
    # Client orchestré : "snowbot" (app = SnowMaster) ou "ankabot" (app = AnkaMaster)
    # Valeurs supportées : "snowbot" | "ankabot"
    "appVariant": "snowbot",
    "autoRelaunch": True,
    "autopilot": {"enabled": False, "mode": "load_only", "schedules": []},
    "instances": {
        "launch_delay": 1,  # délai par défaut en secondes entre les relances
        "mode": "load_and_launch",  # comportement par défaut lors du chargement d'une config
        "overwrite_on_load": True,  # si True -> écrase les instances existantes lors du chargement d'une config
    },
    # Configuration pour Discord : envoi de hook quand le voyant global passe au rouge
    "discord": {
        "enabled": False,  # si True : envoi des hooks Discord activé
        "webhook": "",  # URL du webhook Discord à renseigner dans Settings.json
        "silent_notifications": False,  # si True : les messages Discord n'envoient PAS de notifications (pas de petite bulle blanche)
    },
    # Configuration pour le bot Discord qui efface les messages au passage au vert
    "discord_bot": {
        "enabled": False,  # si True : bot Discord activé
        "token": "",  # Token du bot Discord (à renseigner dans Settings.json)
        "channel_id": 0,  # ID du channel Discord où effacer les messages
        "clear_limit": 100,  # Nombre maximum de messages à effacer/rechercher (par défaut 100)
        "recovery_mode": "edit_and_notify",  # Mode : "edit" (éditer sans notif) | "edit_and_notify" (éditer + notif résolution) | "delete" (supprimer)
    },
    # Délai avant qu'un voyant passe au rouge (en secondes)
    "reddot": 480,  # Heartbeat timeout par défaut : 8 minutes
    # Contrôleur global utilisé par le bouton PANIC (chemin complet du script)
    "panic_controller": "",
    # Icônes UI : noms de fichiers sous SnowMaster/images/ (ex: "play.png") ou chemin absolu
    "icons": {
        "play": "play.png",
        "focus": "show.png",
        "stop": "stop.png",
        "trash": "poubelle.png",
    },
}


##################################### BOT DISCORD POUR EFFACEMENT DES MESSAGES
##################################### BOT DISCORD POUR EFFACEMENT DES MESSAGES


class DiscordClearBot:
    """
    Bot Discord simple qui efface les messages dans un channel spécifique
    quand le voyant global de l'application passe au vert.
    Avec reconnexion automatique en cas de déconnexion.
    """

    def __init__(self):
        self.bot = None
        self.loop = None
        self.thread = None
        self.is_running = False
        self.should_run = False  # Flag pour savoir si le bot doit tourner
        self.token = None
        self.channel_id = None
        self.last_heartbeat = 0  # Timestamp du dernier signe de vie

    def start(self, token: str, channel_id: int):
        """Démarre le bot dans un thread séparé avec reconnexion automatique."""
        if self.should_run:
            app_log_warn("Bot Discord déjà démarré (ou en cours de reconnexion)")
            return

        self.token = token
        self.channel_id = channel_id
        self.should_run = True

        # Créer un nouveau thread pour le bot Discord
        self.thread = threading.Thread(target=self._run_bot_with_reconnect, daemon=True)
        self.thread.start()
        app_log_info(f"Bot Discord démarré pour le channel {channel_id}")

    def _run_bot_with_reconnect(self):
        """Boucle de reconnexion automatique."""
        while self.should_run:
            try:
                app_log_info("Bot Discord : tentative de connexion...")
                self._run_bot()
            except Exception as e:
                app_log_error(f"Bot Discord déconnecté: {e}")
                self.is_running = False

            # Si should_run est toujours True, on attend 10 secondes et on reconnecte
            if self.should_run:
                app_log_info("Bot Discord : reconnexion dans 10 secondes...")
                time.sleep(10)

    def _run_bot(self):
        """Exécute le bot dans son propre event loop."""
        # Créer un nouveau event loop pour ce thread
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

        # Créer le bot avec les intents nécessaires
        intents = discord.Intents.default()
        intents.message_content = True
        intents.messages = True
        intents.guilds = True  # Nécessaire pour lire l'historique des messages

        self.bot = discord.Client(intents=intents)

        @self.bot.event
        async def on_ready():
            app_log_info(f"Bot Discord connecté : {self.bot.user}")
            self.is_running = True
            self.last_heartbeat = time.time()

        @self.bot.event
        async def on_disconnect():
            app_log_warn("Bot Discord déconnecté de Discord")
            self.is_running = False

        # Démarrer le bot (bloquant jusqu'à déconnexion)
        self.loop.run_until_complete(self.bot.start(self.token))

    def clear_messages(self, limit: int = 100):
        """
        Efface les messages dans le channel configuré.
        Utilise asyncio pour exécuter la coroutine dans le loop du bot.
        """
        if not self.is_running or not self.bot or not self.loop:
            app_log_warn("Bot Discord non disponible pour effacer les messages")
            return

        # Exécuter l'effacement dans le loop du bot
        asyncio.run_coroutine_threadsafe(self._clear_messages_async(limit), self.loop)

    async def _clear_messages_async(self, limit: int):
        """Coroutine qui efface les messages dans le channel."""
        try:
            channel = self.bot.get_channel(self.channel_id)
            if not channel:
                app_log_error(f"Channel {self.channel_id} non trouvé")
                return

            # Effacer les messages
            deleted = 0
            async for message in channel.history(limit=limit):
                try:
                    await message.delete()
                    deleted += 1
                    await asyncio.sleep(0.5)  # Éviter le rate limit
                except discord.errors.Forbidden:
                    app_log_error("Permission insuffisante pour effacer les messages")
                    break
                except Exception as e:
                    app_log_warn(f"Erreur lors de l'effacement d'un message: {e}")

            app_log_info(f"✅ {deleted} message(s) effacé(s) dans le channel Discord")

        except Exception as e:
            app_log_error(f"Erreur lors de l'effacement des messages: {e}")

    def edit_last_message(self, limit: int = 50):
        """
        Édite le dernier message d'alerte pour indiquer qu'il est résolu.
        Utilise asyncio pour exécuter la coroutine dans le loop du bot.
        """
        if not self.is_running or not self.bot or not self.loop:
            app_log_warn("Bot Discord non disponible pour éditer les messages")
            return

        # Exécuter l'édition dans le loop du bot
        asyncio.run_coroutine_threadsafe(
            self._edit_last_message_async(limit), self.loop
        )

    async def _edit_last_message_async(self, limit: int):
        """Coroutine qui édite le dernier message d'alerte dans le channel."""
        try:
            channel = self.bot.get_channel(self.channel_id)
            if not channel:
                app_log_error(f"Channel {self.channel_id} non trouvé")
                return

            # Chercher le dernier message contenant le token d'alerte (selon appVariant)
            last_alert_message = None
            async for message in channel.history(limit=limit):
                # Vérifier si c'est un message d'alerte (avec embed)
                if message.embeds:
                    for embed in message.embeds:
                        if embed.title and DISCORD_ALERT_SEARCH_TOKEN in embed.title:
                            last_alert_message = message
                            break
                    if last_alert_message:
                        break

            if not last_alert_message:
                app_log_info("Aucun message d'alerte à éditer trouvé")
                return

            # Créer le nouvel embed "RÉSOLU"
            from datetime import datetime

            now_ts = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

            resolved_embed = discord.Embed(
                title="✅ PROBLÈME RÉSOLU",
                description="Le voyant global est revenu au **VERT** ! Toutes les instances fonctionnent normalement.",
                color=0x10B981,  # Vert
            )
            resolved_embed.add_field(name="📅 Résolu le", value=now_ts, inline=True)
            resolved_embed.set_thumbnail(
                url="https://cdn-icons-png.flaticon.com/512/5610/5610944.png"
            )
            resolved_embed.set_footer(text=DISCORD_ALERT_SYSTEM_FOOTER)

            # Éditer le message
            try:
                await last_alert_message.edit(embed=resolved_embed)
                app_log_info(
                    f"✅ Message d'alerte édité en 'RÉSOLU' (ID: {last_alert_message.id})"
                )
            except discord.errors.Forbidden:
                app_log_error("Permission insuffisante pour éditer les messages")
            except Exception as e:
                app_log_error(f"Erreur lors de l'édition du message: {e}")

        except Exception as e:
            app_log_error(f"Erreur lors de la recherche/édition du message: {e}")

    def edit_and_notify(self, limit: int = 50):
        """
        Édite le dernier message d'alerte ET envoie un nouveau message de notification.
        Mode hybride : garde l'historique propre + vous notifie de la résolution.
        """
        if not self.is_running or not self.bot or not self.loop:
            app_log_warn("Bot Discord non disponible pour éditer/notifier")
            return

        # Exécuter dans le loop du bot
        asyncio.run_coroutine_threadsafe(self._edit_and_notify_async(limit), self.loop)

    async def _edit_and_notify_async(self, limit: int):
        """Coroutine qui édite le message d'alerte ET envoie une notification de résolution."""
        try:
            channel = self.bot.get_channel(self.channel_id)
            if not channel:
                app_log_error(f"Channel {self.channel_id} non trouvé")
                return

            # 1) Chercher et éditer le dernier message d'alerte
            last_alert_message = None
            async for message in channel.history(limit=limit):
                if message.embeds:
                    for embed in message.embeds:
                        if embed.title and DISCORD_ALERT_SEARCH_TOKEN in embed.title:
                            last_alert_message = message
                            break
                    if last_alert_message:
                        break

            from datetime import datetime

            now_ts = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

            # Éditer le message d'alerte si trouvé
            if last_alert_message:
                resolved_embed = discord.Embed(
                    title="✅ PROBLÈME RÉSOLU",
                    description="Le voyant global est revenu au **VERT** ! Toutes les instances fonctionnent normalement.",
                    color=0x10B981,  # Vert
                )
                resolved_embed.add_field(name="📅 Résolu le", value=now_ts, inline=True)
                resolved_embed.set_thumbnail(
                    url="https://cdn-icons-png.flaticon.com/512/5610/5610944.png"
                )
                resolved_embed.set_footer(text=DISCORD_ALERT_SYSTEM_FOOTER)

                try:
                    await last_alert_message.edit(embed=resolved_embed)
                    app_log_info(
                        f"✅ Message d'alerte édité en 'RÉSOLU' (ID: {last_alert_message.id})"
                    )
                except Exception as e:
                    app_log_error(f"Erreur lors de l'édition du message: {e}")

                # 2a) Envoyer une notification courte (sera supprimée après 30s)
                try:
                    notification_message = await channel.send(
                        "🟢 **Tout est revenu au VERT !** Le problème a été résolu."
                    )
                    app_log_info(
                        f"🔔 Message de notification envoyé (ID: {notification_message.id})"
                    )

                    # Supprimer le message de notification après 30 secondes
                    await asyncio.sleep(30)
                    try:
                        await notification_message.delete()
                        app_log_info(
                            f"🗑️ Message de notification auto-supprimé après 30s"
                        )
                    except Exception:
                        pass

                except discord.errors.Forbidden:
                    app_log_error("Permission insuffisante pour envoyer des messages")
                except Exception as e:
                    app_log_error(f"Erreur lors de l'envoi de la notification: {e}")

            else:
                # 2b) Aucun message d'alerte trouvé → Créer un nouveau message complet
                app_log_info(
                    "⚠️ Aucun message d'alerte à éditer trouvé (peut-être supprimé manuellement)"
                )
                app_log_info("📤 Création d'un nouveau message de résolution complet")

                try:
                    # Créer un embed complet de résolution
                    recovery_embed = discord.Embed(
                        title="✅ TOUT EST AU VERT",
                        description=f"Le voyant global {APP_DISPLAY_NAME} est revenu au **VERT** ! Toutes les instances fonctionnent normalement.",
                        color=0x10B981,  # Vert
                    )
                    recovery_embed.add_field(
                        name="📅 Statut mis à jour le", value=now_ts, inline=True
                    )
                    recovery_embed.add_field(
                        name="ℹ️ Info",
                        value="Le message d'alerte précédent n'a pas été trouvé.",
                        inline=False,
                    )
                    recovery_embed.set_thumbnail(
                        url="https://cdn-icons-png.flaticon.com/512/5610/5610944.png"
                    )
                    recovery_embed.set_footer(text=DISCORD_ALERT_SYSTEM_FOOTER)

                    recovery_message = await channel.send(embed=recovery_embed)
                    app_log_info(
                        f"✅ Nouveau message de résolution créé (ID: {recovery_message.id})"
                    )

                except discord.errors.Forbidden:
                    app_log_error("Permission insuffisante pour envoyer des messages")
                except Exception as e:
                    app_log_error(
                        f"Erreur lors de la création du message de résolution: {e}"
                    )

        except Exception as e:
            app_log_error(f"Erreur lors de l'édition et notification: {e}")

    def check_and_resolve_alert(self, recovery_mode: str, limit: int):
        """
        Vérifie d'abord si le dernier message Discord est une alerte rouge.
        N'agit QUE si c'est le cas (évite les notifications inutiles).
        VERSION SIMPLIFIÉE : envoie juste un nouveau message (pas d'édition).
        """
        if not self.is_running or not self.bot or not self.loop:
            app_log_warn("Bot Discord non disponible pour vérifier les alertes")
            return

        # Exécuter la vérification dans le loop du bot
        asyncio.run_coroutine_threadsafe(
            self._check_and_resolve_alert_async(), self.loop
        )

    async def _check_and_resolve_alert_async(self):
        """Coroutine qui vérifie le dernier message et envoie un message de résolution si nécessaire."""
        try:
            app_log_info("🔍 Vérification du dernier message Discord...")

            channel = self.bot.get_channel(self.channel_id)
            if not channel:
                app_log_error(f"❌ Channel {self.channel_id} non trouvé")
                return

            app_log_info(f"✅ Channel trouvé : {channel.name}")

            # 1) Vérifier le dernier message du channel
            last_message = None
            try:
                async for message in channel.history(limit=1):
                    last_message = message
                    break
            except discord.errors.Forbidden:
                app_log_error(
                    "❌ Permission insuffisante pour lire l'historique des messages"
                )
                app_log_error(
                    "   Vérifiez que le bot a les permissions 'Read Message History' sur le serveur Discord"
                )
                return
            except Exception as e:
                app_log_error(f"❌ Erreur lors de la lecture de l'historique : {e}")
                return

            if not last_message:
                app_log_info("ℹ️ Aucun message dans le channel Discord")
                return

            app_log_info(
                f"📨 Dernier message trouvé (ID: {last_message.id}, Auteur: {last_message.author.name})"
            )

            # 2) Vérifier si c'est une alerte rouge
            is_red_alert = False
            if last_message.embeds:
                app_log_info(
                    f"   Le message contient {len(last_message.embeds)} embed(s)"
                )
                for embed in last_message.embeds:
                    app_log_info(f"   Embed title: {embed.title}")
                    if embed.title and DISCORD_ALERT_SEARCH_TOKEN in embed.title:
                        is_red_alert = True
                        app_log_info(
                            f"✅ Dernier message est une alerte rouge → Envoi du message de résolution"
                        )
                        break
            else:
                app_log_info("   Le message ne contient pas d'embed")

            if not is_red_alert:
                app_log_info(
                    f"ℹ️ Le dernier message n'est PAS une alerte rouge → Aucune action"
                )
                return

            # 3) Le dernier message est bien une alerte rouge → Envoyer un message de résolution
            try:
                # Embed simple et stylé
                resolution_embed = discord.Embed(
                    description="🟢 **Tout est revenu au VERT !** Le problème a été résolu.",
                    color=0x10B981,  # Vert
                )
                resolution_message = await channel.send(embed=resolution_embed)
                app_log_info(
                    f"✅ Message de résolution envoyé (ID: {resolution_message.id})"
                )
            except discord.errors.Forbidden:
                app_log_error("❌ Permission insuffisante pour envoyer des messages")
            except Exception as e:
                app_log_error(
                    f"❌ Erreur lors de l'envoi du message de résolution: {e}"
                )

        except Exception as e:
            app_log_error(f"❌ Erreur lors de la vérification: {e}")
            import traceback

            app_log_error(f"   Traceback: {traceback.format_exc()}")

    async def _edit_alert_message(self, alert_message, timestamp: str):
        """Édite un message d'alerte pour marquer 'RÉSOLU'."""
        try:
            resolved_embed = discord.Embed(
                title="✅ PROBLÈME RÉSOLU",
                description="Le voyant global est revenu au **VERT** ! Toutes les instances fonctionnent normalement.",
                color=0x10B981,  # Vert
            )
            resolved_embed.add_field(name="📅 Résolu le", value=timestamp, inline=True)
            resolved_embed.set_thumbnail(
                url="https://cdn-icons-png.flaticon.com/512/5610/5610944.png"
            )
            resolved_embed.set_footer(text=DISCORD_ALERT_SYSTEM_FOOTER)

            await alert_message.edit(embed=resolved_embed)
            app_log_info(
                f"✅ Message d'alerte édité en 'RÉSOLU' (ID: {alert_message.id})"
            )
        except discord.errors.Forbidden:
            app_log_error(
                "Permission insuffisante pour éditer les messages (le message n'a pas été envoyé par le bot)"
            )
        except Exception as e:
            app_log_error(f"Erreur lors de l'édition du message d'alerte: {e}")

    def send_red_alert(self, red_count: int, instances_list: str, timestamp: str):
        """
        Envoie une alerte rouge via le bot Discord (au lieu du webhook).
        Comme ça, le bot pourra éditer ce message plus tard.
        """
        if not self.is_running or not self.bot or not self.loop:
            app_log_warn("Bot Discord non disponible pour envoyer l'alerte")
            return

        # Exécuter l'envoi dans le loop du bot
        asyncio.run_coroutine_threadsafe(
            self._send_red_alert_async(red_count, instances_list, timestamp), self.loop
        )

    async def _send_red_alert_async(
        self, red_count: int, instances_list: str, timestamp: str
    ):
        """Coroutine qui envoie l'alerte rouge."""
        try:
            channel = self.bot.get_channel(self.channel_id)
            if not channel:
                app_log_error(f"Channel {self.channel_id} non trouvé")
                return

            # Créer l'embed d'alerte (même format que le webhook)
            alert_embed = discord.Embed(
                title=f"🔴 {DISCORD_ALERT_SEARCH_TOKEN}",
                description="Le voyant global est passé au **ROUGE** !",
                color=0xFF0000,  # Rouge
            )
            alert_embed.add_field(name="📅 Heure", value=timestamp, inline=True)
            alert_embed.add_field(
                name="📊 Instances en rouge", value=str(red_count), inline=True
            )
            alert_embed.add_field(
                name="📋 Liste des instances", value=instances_list, inline=False
            )
            alert_embed.set_thumbnail(
                url="https://cdn-icons-png.flaticon.com/512/8359/8359080.png"
            )
            alert_embed.set_footer(text=DISCORD_ALERT_SYSTEM_FOOTER)

            # Envoyer le message
            alert_message = await channel.send(embed=alert_embed)
            app_log_info(f"🔴 Alerte rouge envoyée via le bot (ID: {alert_message.id})")

        except discord.errors.Forbidden:
            app_log_error("Permission insuffisante pour envoyer des messages")
        except Exception as e:
            app_log_error(f"Erreur lors de l'envoi de l'alerte rouge: {e}")

    def stop(self):
        """Arrête le bot Discord (fermeture rapide sans attendre)."""
        if not self.should_run:
            return

        try:
            self.should_run = False  # Arrêter la boucle de reconnexion
            self.is_running = False
            if self.bot and self.loop:
                # Lancer la fermeture sans attendre (le thread daemon se terminera tout seul)
                asyncio.run_coroutine_threadsafe(self.bot.close(), self.loop)
            app_log_info("Bot Discord arrêté (fermeture rapide)")
        except Exception:
            pass  # On ignore les erreurs pour ne pas ralentir la fermeture


# Instance globale du bot Discord
_discord_bot = DiscordClearBot()

##################################### BAILS UTILES
##################################### BAILS UTILES
##################################### BAILS UTILES
##################################### BAILS UTILES
##################################### BAILS UTILES


_user32 = ctypes.windll.user32
_user32 = ctypes.windll.user32
SendMessageW = _user32.SendMessageW
PostMessageW = _user32.PostMessageW


class RECT(ctypes.Structure):
    _fields_ = [
        ("left", ctypes.c_long),
        ("top", ctypes.c_long),
        ("right", ctypes.c_long),
        ("bottom", ctypes.c_long),
    ]


try:
    UINT_PTR = wintypes.UINT_PTR
except AttributeError:
    UINT_PTR = ctypes.c_size_t


class NMHDR(ctypes.Structure):
    _fields_ = [
        ("hwndFrom", wintypes.HWND),
        ("idFrom", UINT_PTR),
        ("code", wintypes.UINT),
    ]


def click_connexion_button(
    hwnd, image_path=None, confidence=None, timeout=0.0, background: bool = False
):
    """
    Click 'Connexion' arrière-plan, sans encapsulation.
    - background=True : ne JAMAIS activer/focus avant le clic.
    Ordre :
      1) bouton WinForms 'Connexion' -> BM_CLICK
      2) sinon WM_COMMAND(BN_CLICKED) au parent
      3) fallback : clic client (WM_MOUSE*) sans activer
      4) ultime : VK_RETURN (si default), seulement si background=False
    """
    # 1) Bouton WinForms exact 'Connexion' => BM_CLICK (ne nécessite pas l'activation)
    try:
        btn = _find_winforms_button(
            hwnd, captions=("Connexion", "Connect", "OK", "Se connecter")
        )
        if btn:
            if _bm_click(btn):
                print(f"[EARLY CLICK] BM_CLICK sur hwnd bouton={btn}")
                return True
            else:
                print("[WARN] BM_CLICK sans effet, tentative WM_COMMAND(BN_CLICKED)")
                try:
                    parent = win32gui.GetParent(btn) or hwnd
                except Exception:
                    parent = hwnd
                if _send_bn_clicked_to_parent(parent, btn):
                    print(
                        f"[EARLY CLICK] WM_COMMAND(BN_CLICKED) parent={parent} child={btn}"
                    )
                    return True
    except Exception as e:
        print(f"[WARN] path bouton failed: {e}")

    return False


def _find_winforms_button(
    hwnd_parent: int, captions=("Connexion", "Connect", "OK", "Se connecter")
) -> int | None:
    """
    Retourne le HWND du bouton WinForms dont la classe commence par 'WindowsForms10.BUTTON'
    et dont le texte (caption) correspond exactement à l’une des captions.
    Recherche en profondeur (descendants).
    """
    caps = [c.strip().lower() for c in captions]
    for h in _iter_children_recursive(hwnd_parent):
        cls = _safe_get_class(h)
        if not cls.startswith("WindowsForms10.BUTTON"):
            continue
        txt = _safe_get_text(h).strip().lower()
        if txt in caps:
            return h
    return None


def _safe_get_class(hwnd: int) -> str:
    try:
        return win32gui.GetClassName(hwnd) or ""
    except Exception:
        return ""


def _safe_get_text(hwnd: int) -> str:
    try:
        return win32gui.GetWindowText(hwnd) or ""
    except Exception:
        return ""


def _iter_children_recursive(parent_hwnd: int):
    res = []

    def enum_cb(h, _):
        res.append(h)
        # Descend récursivement
        win32gui.EnumChildWindows(h, enum_cb, None)

    try:
        win32gui.EnumChildWindows(parent_hwnd, enum_cb, None)
    except Exception:
        pass
    return res


def _bm_click(hwnd_button: int) -> bool:
    """
    Envoie un BM_CLICK sur un bouton (win32con.BM_CLICK).
    Si l’UI du client est standard (Win32/WinForms/WPF host de 'Button'), ça déclenche le click handler.
    """
    try:
        if not hwnd_button or not win32gui.IsWindow(hwnd_button):
            return False
        # Optionnel: s’assurer qu’il est activé/visible
        if not win32gui.IsWindowVisible(hwnd_button):
            # On peut tenter quand même ; certaines toolkits routent quand même.
            pass
        win32gui.SendMessage(hwnd_button, win32con.BM_CLICK, 0, 0)
        return True
    except Exception as e:
        print(f"[WARN] _bm_click failed: {e}")
        return False


# def _send_bn_clicked_to_parent(parent_hwnd: int, child_hwnd: int) -> bool:
#     """
#     Simule le 'click' d'un bouton via le message parent WM_COMMAND/BN_CLICKED.
#     Utile si SendMessage(BM_CLICK) sur le bouton n'a aucun effet.
#     """
#     try:
#         try:
#             ctrl_id = win32gui.GetDlgCtrlID(child_hwnd)
#         except Exception:
#             ctrl_id = 0

#         BN_CLICKED = 0               # notification BN_CLICKED
#         WM_COMMAND = win32con.WM_COMMAND

#         # WPARAM = (HIWORD = notif = BN_CLICKED) << 16 | (LOWORD = ctrl_id)
#         wparam = ((BN_CLICKED & 0xFFFF) << 16) | (ctrl_id & 0xFFFF)
#         lparam = child_hwnd          # HWND du contrôle

#         # Certains toolkits préfèrent SendMessage (synchrone) au parent
#         try:
#             win32gui.SendMessage(parent_hwnd, WM_COMMAND, wparam, lparam)
#             return True
#         except Exception:
#             # fallback asynchrone
#             win32api.PostMessage(parent_hwnd, WM_COMMAND, wparam, lparam)
#             return True
#     except Exception as e:
#         print(f"[WARN] _send_bn_clicked_to_parent failed: {e}")
#         return False


# --- version “par coordonnées relatives” sur le parent ---
def background_command_click_by_relative(
    parent_hwnd: int, rx: float, ry: float, dx: int = 0, dy: int = 0
) -> bool:
    try:
        l, t, r, b = win32gui.GetClientRect(parent_hwnd)
        W = max(1, r - l)
        H = max(1, b - t)
        x = int(max(0, min(1, rx)) * W) + int(dx)
        y = int(max(0, min(1, ry)) * H) + int(dy)
        child = _deep_child_from_client_point(parent_hwnd, x, y)
        if not child or not win32gui.IsWindow(child):
            print("[BG-CMD] aucun child sous le point")
            return False
        return send_command_to_top_by_child(child)
    except Exception as e:
        print(f"[BG-CMD] error: {e}")
        return False


# --- retrouver le deep child sous un point CLIENT du parent ---
def _deep_child_from_client_point(
    parent_hwnd: int, x_client: int, y_client: int
) -> int:
    CWP_SKIPINVISIBLE = 0x0001
    CWP_SKIPDISABLED = 0x0002
    CWP_SKIPTRANSPARENT = 0x0004
    pt = (x_client, y_client)
    h = parent_hwnd
    last = h
    while True:
        try:
            child = win32gui.ChildWindowFromPointEx(
                h, pt, CWP_SKIPINVISIBLE | CWP_SKIPDISABLED | CWP_SKIPTRANSPARENT
            )
        except Exception:
            child = 0
        if not child or child == h:
            return last
        try:
            pt = win32gui.MapWindowPoints(h, child, pt)  # dispo sur certaines versions
        except AttributeError:
            pt = _map_client_to_client(h, child, pt[0], pt[1])
        last = child
        h = child


# --- envoi WM_COMMAND au top-level avec l’ID d’un child ---
def send_command_to_top_by_child(child_hwnd: int) -> bool:
    try:
        if not child_hwnd or not win32gui.IsWindow(child_hwnd):
            return False
        try:
            ctrl_id = win32gui.GetDlgCtrlID(child_hwnd)
        except Exception:
            ctrl_id = 0
        if ctrl_id == 0:
            # certains owner-draw placent id=0 : essaye le parent immédiat
            parent = win32gui.GetParent(child_hwnd) or 0
            if parent:
                try:
                    ctrl_id = win32gui.GetDlgCtrlID(parent)
                except Exception:
                    ctrl_id = 0
        top = _get_toplevel(child_hwnd)
        if ctrl_id == 0 or not top:
            print(f"[WM_COMMAND] pas d’ID exploitable (child=0x{child_hwnd:08X})")
            return False
        # wParam = (HIWORD=notification=0, LOWORD=ID)
        wparam = (0 << 16) | (ctrl_id & 0xFFFF)
        # lParam = HWND du child (classique pour owner-draw)
        win32gui.SendMessage(top, win32con.WM_COMMAND, wparam, child_hwnd)
        print(
            f"[WM_COMMAND] id={ctrl_id} → top=0x{top:08X} (lParam=0x{child_hwnd:08X})"
        )
        return True
    except Exception as e:
        print(f"[WM_COMMAND] error: {e}")
        return False


class POINT(ctypes.Structure):
    _fields_ = [("x", wintypes.LONG), ("y", wintypes.LONG)]


def _client_to_screen(hwnd: int, x: int, y: int):
    pt = POINT(x, y)
    _user32.ClientToScreen(int(hwnd), ctypes.byref(pt))
    return pt.x, pt.y


def _screen_to_client(hwnd: int, x: int, y: int):
    pt = POINT(x, y)
    _user32.ScreenToClient(int(hwnd), ctypes.byref(pt))
    return pt.x, pt.y


def _map_client_to_client(src_hwnd: int, dst_hwnd: int, x: int, y: int):
    xs, ys = _client_to_screen(src_hwnd, x, y)
    return _screen_to_client(dst_hwnd, xs, ys)


# --- GA_ROOT pour remonter au top-level ---
def _get_toplevel(hwnd: int) -> int:
    try:
        return win32gui.GetAncestor(hwnd, win32con.GA_ROOT)
    except Exception:
        # fallback
        p = hwnd
        while True:
            n = win32gui.GetParent(p) or 0
            if not n:
                return p
            p = n


def find_dialog_for_instance(main_hwnd: int, title_substr: str, timeout: float = 5.0):
    """Cherche une fenêtre top-level du *même PID* dont le titre contient title_substr."""
    t0 = time.time()
    want = (title_substr or "").lower()
    pid = _get_pid(main_hwnd)

    while time.time() - t0 < timeout:
        found = []

        def enum_cb(h, _):
            if _is_toplevel_visible_of_pid(h, pid):
                title = win32gui.GetWindowText(h) or ""
                if want in title.lower():
                    found.append(h)

        win32gui.EnumWindows(enum_cb, None)

        if found:
            # Si plusieurs, on prend la plus récente (z-order) : dernière de EnumWindows ≈ plus “au dessus”
            return found[-1]
        time.sleep(0.05)
    return None


def _is_toplevel_visible_of_pid(hwnd: int, pid: int) -> bool:
    if not win32gui.IsWindow(hwnd) or not win32gui.IsWindowVisible(hwnd):
        return False
    return _get_pid(hwnd) == pid and (win32gui.GetParent(hwnd) == 0)


def _get_pid(hwnd: int) -> int:
    try:
        _, pid = win32gui.GetWindowThreadProcessId(hwnd)
        return int(pid or 0)
    except Exception:
        return 0


def _norm(s: str) -> str:
    # Normalisation simple pour ignorer accents/majuscules et ellipses
    if not s:
        return ""
    s = s.lower().replace("controleur", "contrôleur").replace("…", "...").strip()
    return s


def click_child_by_text(
    dialog_hwnd: int, targets: list[str], timeout: float = 2.0
) -> bool:
    """
    Cherche un enfant (ou petit-enfant…) dont le texte contient une des cibles,
    clique d’abord en BM_CLICK si c’est un BUTTON, sinon WM_COMMAND via son ID.
    """
    t0 = time.time()
    targets_n = [_norm(t) for t in targets]
    while time.time() - t0 < timeout:
        for h in _iter_children_recursive(dialog_hwnd):
            try:
                cls = win32gui.GetClassName(h) or ""
                txt = win32gui.GetWindowText(h) or ""
                if any(t in _norm(txt) for t in targets_n):
                    # 1) si vrai bouton WinForms/Win32
                    if cls.startswith("WindowsForms10.BUTTON") or cls == "Button":
                        if _bm_click(h):
                            return True
                    # 2) fallback WM_COMMAND
                    # if _send_command_to_top_by_child(h):
                    if send_command_to_top_by_child(h):
                        return True
            except Exception:
                pass
        time.sleep(0.05)
    return False


def close_gestionnaire_dialog(dlg_hwnd: int) -> bool:
    """
    Ferme la boîte soit en cliquant 'Charger' en bas (par WM_COMMAND via child sous position),
    soit en WM_CLOSE si échec. Background-safe.
    """
    # positions relatives testées pour le bouton 'Charger' (bas-centre)
    # on essaie 2-3 y pour couvrir les variations de layout
    tried = (
        (0.50, 0.86, 0, 0),
        (0.50, 0.88, 0, 0),
        (0.50, 0.84, 0, 0),
    )
    for rx, ry, dx, dy in tried:
        if background_command_click_by_relative(dlg_hwnd, rx, ry, dx, dy):
            # on laisse 200–400ms au form pour se fermer si c'est bien un 'OK'
            time.sleep(0.35)
            if not win32gui.IsWindow(dlg_hwnd):
                return True
            # parfois le form reste ouvert (ex: validation non bloquante) -> on continue
    # fallback soft
    win32gui.SendMessage(dlg_hwnd, win32con.WM_CLOSE, 0, 0)
    time.sleep(0.2)
    return not win32gui.IsWindow(dlg_hwnd)


#################################################### jusque la tout est utilisé
#################################################### jusque la tout est utilisé
#################################################### jusque la tout est utilisé
#################################################### jusque la tout est utilisé

#######################   OUVRIR FICHIER   #######################
#######################   OUVRIR FICHIER   #######################
#######################   OUVRIR FICHIER   #######################
#######################   OUVRIR FICHIER   #######################
#######################   OUVRIR FICHIER   #######################
#######################   OUVRIR FICHIER   #######################
#######################   OUVRIR FICHIER   #######################
#######################   OUVRIR FICHIER   #######################
#######################   OUVRIR FICHIER   #######################
#######################   OUVRIR FICHIER   #######################
#######################   OUVRIR FICHIER   #######################
#######################   OUVRIR FICHIER   #######################
#######################   OUVRIR FICHIER   #######################
#######################   OUVRIR FICHIER   #######################
#######################   OUVRIR FICHIER   #######################
#######################   OUVRIR FICHIER   #######################
#######################   OUVRIR FICHIER   #######################
# ======================= OUVERTURE DE FICHIER (BG, ciblée instance) =======================
import os, time, ctypes
from ctypes import wintypes
import win32gui, win32con, win32api


# --- Utils de base ---
def _pid(hwnd: int) -> int:
    try:
        _, p = win32gui.GetWindowThreadProcessId(hwnd)
        return int(p or 0)
    except:
        return 0


def _class(hwnd: int) -> str:
    try:
        return win32gui.GetClassName(hwnd) or ""
    except:
        return ""


def _text(hwnd: int) -> str:
    try:
        return win32gui.GetWindowText(hwnd) or ""
    except:
        return ""


def _root_owner(hwnd: int) -> int:
    GA_ROOTOWNER = 3
    try:
        return win32gui.GetAncestor(hwnd, GA_ROOTOWNER)
    except:
        # fallback simple
        p = hwnd
        while True:
            par = win32gui.GetParent(p) or 0
            if not par:
                return p
            p = par


def _enum_children(hwnd: int):
    out = []

    def cb(h, _):
        out.append(h)
        try:
            win32gui.EnumChildWindows(h, cb, None)
        except:
            pass

    try:
        win32gui.EnumChildWindows(hwnd, cb, None)
    except:
        pass
    return out


# IDs “classiques” pour Common File Dialog (legacy)
IDOK = 1
EDT_FILENAME = 0x047C  # champ "Nom du fichier :"


# 1) Trouver UNIQUEMENT la boîte d’ "Ouvrir" liée à cette instance (PID + owner)
def find_open_dialog_for_instance(main_hwnd: int, timeout: float = 5.0) -> int | None:
    want_pid = _pid(main_hwnd)
    want_owner = _root_owner(main_hwnd)
    t0 = time.time()
    while time.time() - t0 < timeout:
        found = []

        def enum_cb(h, _):
            if not win32gui.IsWindowVisible(h):
                return
            if _pid(h) != want_pid:
                return
            if _root_owner(h) != want_owner:
                return  # même owner = modal de CETTE fenêtre
            cls = _class(h)
            tit = _text(h).lower()
            # heuristiques: vrai #32770 “Ouvrir”, ou shell intégré (CabinetWClass) avec titre contenant "ouvrir"
            if cls in ("#32770", "CabinetWClass") or "ouvrir" in tit or "open" in tit:
                found.append(h)

        win32gui.EnumWindows(enum_cb, None)
        if found:
            dlg = found[-1]  # la plus haute dans le z-order
            print(f"[OPEN] cible=0x{dlg:08X} cls='{_class(dlg)}' title='{_text(dlg)}'")
            return dlg
        time.sleep(0.05)
    print("[OPEN] dialog introuvable (ciblage PID/owner)")
    return None


# 2) Récupérer le champ "Nom du fichier"
def find_filename_edit(dlg: int) -> int | None:
    h = win32gui.GetDlgItem(dlg, EDT_FILENAME)
    if h and win32gui.IsWindow(h):
        print(f"[OPEN] edit par ID (0x{EDT_FILENAME:04X}) = 0x{h:08X}")
        return h
    # fallback: 1er Edit en bas de la boîte
    edits = [c for c in _enum_children(dlg) if _class(c) == "Edit"]
    if not edits:
        print("[OPEN] aucun Edit trouvé")
        return None

    # on prend l’Edit avec la coordonnée 'top' la plus grande (le plus bas)
    def _top(hwnd):
        try:
            return win32gui.GetWindowRect(hwnd)[1]
        except:
            return 10**9

    edits.sort(key=_top)
    h = edits[-1]
    print(f"[OPEN] edit fallback = 0x{h:08X}")
    return h


# 3) Cliquer “Ouvrir” sans focus (IDOK)
def press_open_ok(dlg: int) -> bool:
    try:
        win32gui.SendMessage(dlg, win32con.WM_COMMAND, IDOK, 0)
        print("[OPEN] WM_COMMAND(IDOK) envoyé")
        return True
    except Exception as e:
        print(f"[OPEN] IDOK échec: {e}")
        # fallback: bouton “Ouvrir”
        for c in _enum_children(dlg):
            if _class(c).startswith("Button") and _text(c).strip().lower() in (
                "ouvrir",
                "open",
                "&ouvrir",
                "&open",
            ):
                win32gui.SendMessage(c, win32con.BM_CLICK, 0, 0)
                print(f"[OPEN] BM_CLICK sur bouton 0x{c:08X}")
                return True
    return False


# 4) Pipeline complet
def open_file_via_dialog_bg(main_hwnd: int, fullpath: str) -> bool:
    # normaliser → chemins Windows sans guillemets ni slashs
    want = os.path.normpath(fullpath.strip().strip('"')).replace("/", "\\")
    print(f"[OPEN-ASYNC] want='{want}'")

    dlg = find_open_dialog_for_instance(main_hwnd, timeout=4.0)
    if not dlg:
        return False

    ed = find_filename_edit(dlg)
    if not ed:
        return False

    # WM_SETTEXT direct (pas de guillemets)
    try:
        win32gui.SendMessage(ed, win32con.WM_SETTEXT, 0, want)
        print("[OPEN] WM_SETTEXT OK (Nom du fichier)")
    except Exception as e:
        print(f"[OPEN] SETTEXT échec: {e}")
        return False

    if not press_open_ok(dlg):
        print("[OPEN] validation KO")
        return False

    # laisser le temps de traiter
    time.sleep(0.25)
    still = win32gui.IsWindow(dlg) and win32gui.IsWindowVisible(dlg)
    print("[OPEN] terminé, dialog encore visible ?", still)
    return True


# ======================= / OUVERTURE DE FICHIER =======================


#######################   FIN OUVRIR FICHIER   #######################
#######################   FIN OUVRIR FICHIER   #######################
#######################   FIN OUVRIR FICHIER   #######################
#######################   FIN OUVRIR FICHIER   #######################
#######################   FIN OUVRIR FICHIER   #######################
#######################   FIN OUVRIR FICHIER   #######################
#######################   FIN OUVRIR FICHIER   #######################
#######################   FIN OUVRIR FICHIER   #######################
#######################   FIN OUVRIR FICHIER   #######################
#######################   FIN OUVRIR FICHIER   #######################
#######################   FIN OUVRIR FICHIER   #######################
#######################   FIN OUVRIR FICHIER   #######################
#######################   FIN OUVRIR FICHIER   #######################
#######################   FIN OUVRIR FICHIER   #######################
#######################   FIN OUVRIR FICHIER   #######################
#######################   FIN OUVRIR FICHIER   #######################
#######################   FIN OUVRIR FICHIER   #######################
#######################   FIN OUVRIR FICHIER   #######################
#######################   FIN OUVRIR FICHIER   #######################
#######################   FIN OUVRIR FICHIER   #######################
#######################   FIN OUVRIR FICHIER   #######################
#######################   FIN OUVRIR FICHIER   #######################
#######################   FIN OUVRIR FICHIER   #######################
#######################   FIN OUVRIR FICHIER   #######################
#######################   FIN OUVRIR FICHIER   #######################
#######################   FIN OUVRIR FICHIER   #######################
#######################   FIN OUVRIR FICHIER   #######################
#######################   FIN OUVRIR FICHIER   #######################
#######################   FIN OUVRIR FICHIER   #######################
#######################   FIN OUVRIR FICHIER   #######################
#######################   FIN OUVRIR FICHIER   #######################
#######################   FIN OUVRIR FICHIER   #######################
#######################   FIN OUVRIR FICHIER   #######################


#######################   BOUTON CHARGER UN SCRIPT   #######################
#######################   BOUTON CHARGER UN SCRIPT   #######################
#######################   BOUTON CHARGER UN SCRIPT   #######################
#######################   BOUTON CHARGER UN SCRIPT   #######################


def click_script_loader_icon_async(
    main_hwnd, y_band=(40, 80), max_h=40, max_w=60, index_from_left=2
):
    """
    Identique à ta détection d’icônes, mais l'activation se fait en ASYNCHRONE:
    -> WM_COMMAND(BN_CLICKED) via PostMessage (donc pas de blocage).
    """

    def _child_rect_client(parent_hwnd, child_hwnd):
        try:
            L, T, R, B = win32gui.GetWindowRect(child_hwnd)
            (x1, y1) = win32gui.ScreenToClient(parent_hwnd, (L, T))
            (x2, y2) = win32gui.ScreenToClient(parent_hwnd, (R, B))
            return x1, y1, x2, y2
        except:
            return None

    ymin, ymax = y_band
    icons = []
    for h in _iter_all_descendants(main_hwnd):
        cls = _safe_class(h)
        if not cls.startswith("WindowsForms10.BUTTON"):
            continue
        if _safe_text(h).strip():  # icônes: caption vide
            continue
        rc = _child_rect_client(main_hwnd, h)
        if not rc:
            continue
        l, t, r, b = rc
        w, hgt = (r - l), (b - t)
        if hgt <= max_h and w <= max_w and ymin <= t <= ymax:
            icons.append((h, rc))
    if not icons:
        print("[ICON-ASYNC] aucune icône trouvée")
        return False

    # tri gauche→droite
    icons.sort(key=lambda x: (x[1][0] + x[1][2]) // 2)
    idx = max(0, min(index_from_left, len(icons) - 1))
    h_btn, rc_btn = icons[idx]
    cid = safe_get_id(h_btn)
    parent = win32gui.GetParent(h_btn) or win32gui.GetAncestor(h_btn, win32con.GA_ROOT)
    if not parent or not _safe_is_window(parent):
        parent = main_hwnd

    # BN_CLICKED asynchrone
    BN_CLICKED = 0
    wparam = ((BN_CLICKED & 0xFFFF) << 16) | (cid & 0xFFFF)
    try:
        PostMessageW(parent, win32con.WM_COMMAND, wparam, h_btn)
        print(
            f"[ICON-ASYNC] Post WM_COMMAND(BN_CLICKED) parent=0x{parent:08X} child=0x{h_btn:08X} id={cid} rect={rc_btn}"
        )
        return True
    except Exception as e:
        print(f"[ICON-ASYNC] post failed: {e}")
        return False


def _iter_all_descendants(root):
    res, stack, seen = [], [root], set()
    while stack:
        h = stack.pop()
        try:
            kids = []
            win32gui.EnumChildWindows(h, lambda c, _: kids.append(c), None)
        except:
            kids = []
        for c in kids:
            if c in seen:
                continue
            seen.add(c)
            res.append(c)
            stack.append(c)
    return res


#######################   FIN BOUTON CHARGER UN SCRIPT   #######################
#######################   FIN BOUTON CHARGER UN SCRIPT   #######################
#######################   FIN BOUTON CHARGER UN SCRIPT   #######################
#######################   FIN BOUTON CHARGER UN SCRIPT   #######################


#######################   HELPERS DE MERDE     #######################
#######################   HELPERS DE MERDE     #######################
#######################   HELPERS DE MERDE     #######################


def safe_get_id(hwnd):
    try:
        return win32gui.GetDlgCtrlID(hwnd)
    except Exception:
        return None


def _safe_class(hwnd: int) -> str:
    try:
        return win32gui.GetClassName(hwnd) or ""
    except:
        return ""


def _safe_text(hwnd: int) -> str:
    try:
        return win32gui.GetWindowText(hwnd) or ""
    except:
        return ""


def _client_rect(hwnd: int):
    l, t, r, b = win32gui.GetClientRect(hwnd)
    return l, t, r, b


def _client_to_screen(hwnd: int, x: int, y: int):
    class POINT(ctypes.Structure):
        _fields_ = [("x", ctypes.c_long), ("y", ctypes.c_long)]

    pt = POINT(x, y)
    _user32.ClientToScreen(int(hwnd), ctypes.byref(pt))
    return pt.x, pt.y


def _deep_child_from_client_point(parent_hwnd: int, x: int, y: int) -> int:
    CWP = 0x0001 | 0x0002 | 0x0004  # SKIPINVISIBLE|SKIPDISABLED|SKIPTRANSPARENT
    h = parent_hwnd
    last = h
    pt = (x, y)
    while True:
        try:
            ch = win32gui.ChildWindowFromPointEx(h, pt, CWP)
        except Exception:
            ch = 0
        if not ch or ch == h:
            return last
        try:
            pt = win32gui.MapWindowPoints(h, ch, pt)
        except AttributeError:
            xs, ys = _client_to_screen(h, pt[0], pt[1])
            pt = win32gui.ScreenToClient(ch, (xs, ys))
        last = ch
        h = ch


############### TEMPORAIRE ###############


def _try_bm_click(child: int) -> bool:
    try:
        win32gui.SendMessage(child, win32con.BM_CLICK, 0, 0)
        return True
    except Exception as e:
        print(f"[BG] BM_CLICK failed: {e}")
        return False


def _try_wm_command_bn_clicked(target_parent: int, child: int) -> bool:
    try:
        ctrl_id = win32gui.GetDlgCtrlID(child) or 0
    except Exception:
        ctrl_id = 0
    if not target_parent or ctrl_id == 0:
        return False
    BN_CLICKED = 0
    wparam = ((BN_CLICKED & 0xFFFF) << 16) | (ctrl_id & 0xFFFF)
    try:
        win32gui.SendMessage(target_parent, win32con.WM_COMMAND, wparam, child)
        return True
    except Exception as e:
        print(f"[BG] WM_COMMAND(BN_CLICKED) failed: {e}")
        return False


def _try_wm_command_neutral(target_parent: int, child: int) -> bool:
    try:
        ctrl_id = win32gui.GetDlgCtrlID(child) or 0
    except Exception:
        ctrl_id = 0
    if not target_parent or ctrl_id == 0:
        return False
    wparam = (0 << 16) | (ctrl_id & 0xFFFF)
    try:
        win32gui.SendMessage(target_parent, win32con.WM_COMMAND, wparam, child)
        return True
    except Exception as e:
        print(f"[BG] WM_COMMAND(neutral) failed: {e}")
        return False


def background_click_robust_by_relative(
    parent_hwnd: int, rx: float, ry: float, dx: int = 0, dy: int = 0
) -> bool:
    """
    Vise le child SOUS le point (rx,ry) relatif au parent.
    Ordre d’essais:
      - si BUTTON -> BM_CLICK
      - sinon WM_COMMAND(BN_CLICKED) vers top-level
      - sinon WM_COMMAND(neutral) vers top-level
      - retry sur PARENT immédiat (certaines app gèrent au niveau parent)
    """
    try:
        l, t, r, b = win32gui.GetClientRect(parent_hwnd)
        W = max(1, r - l)
        H = max(1, b - t)
        x = int(max(0, min(1, rx)) * W) + int(dx)
        y = int(max(0, min(1, ry)) * H) + int(dy)

        child = _deep_child_from_client_point(parent_hwnd, x, y)
        if not child or not win32gui.IsWindow(child):
            print("[BG] aucun child sous le point")
            return False

        cls = _safe_class(child)
        txt = _safe_text(child)
        try:
            cid = win32gui.GetDlgCtrlID(child) or 0
        except:
            cid = 0
        print(f"[BG] hit child=0x{child:08X} class='{cls}' text='{txt}' id={cid}")

        top = win32gui.GetAncestor(child, win32con.GA_ROOT) or parent_hwnd
        par = win32gui.GetParent(child) or 0

        # 1) vrai bouton ?
        if cls.startswith("WindowsForms10.BUTTON") or cls == "Button":
            if _try_bm_click(child):
                print("[BG] BM_CLICK sent")
                return True
            # si BM_CLICK silence, on tente BN_CLICKED
            if _try_wm_command_bn_clicked(top, child):
                print("[BG] WM_COMMAND(BN_CLICKED) to top OK")
                return True
            if par and _try_wm_command_bn_clicked(par, child):
                print("[BG] WM_COMMAND(BN_CLICKED) to parent OK")
                return True

        # 2) pas un bouton → owner-draw/menu/panel : on force BN_CLICKED via ID
        if _try_wm_command_bn_clicked(top, child):
            print("[BG] WM_COMMAND(BN_CLICKED) to top OK")
            return True
        if _try_wm_command_neutral(top, child):
            print("[BG] WM_COMMAND(neutral) to top OK")
            return True
        if par and _try_wm_command_bn_clicked(par, child):
            print("[BG] WM_COMMAND(BN_CLICKED) to parent OK")
            return True
        if par and _try_wm_command_neutral(par, child):
            print("[BG] WM_COMMAND(neutral) to parent OK")
            return True

        print("[BG] all strategies failed")
        return False
    except Exception as e:
        print(f"[BG] error: {e}")
        return False


############### TEMPORAIRE ###############
############### TEMPORAIRE ###############
# ---------- helpers sûrs ----------
def _safe_is_window(h):
    try:
        return bool(h) and win32gui.IsWindow(h)
    except:
        return False


def _safe_get_toplevel(hwnd: int) -> int | None:
    """Toujours sûr même si hwnd est invalide."""
    try:
        if not _safe_is_window(hwnd):
            return None
        try:
            return win32gui.GetAncestor(hwnd, win32con.GA_ROOT)
        except Exception:
            p = hwnd
            for _ in range(32):
                if not _safe_is_window(p):
                    return None
                n = win32gui.GetParent(p) or 0
                if not n:
                    return p
                p = n
            return p
    except:
        return None


def _enum_children(parent: int):
    out = []

    def cb(h, _):
        out.append(h)

    try:
        win32gui.EnumChildWindows(parent, cb, None)
    except:
        pass
    return out


def _find_child_by_id(parent: int, ctrl_id: int) -> int | None:
    for h in _enum_children(parent):
        try:
            if win32gui.GetDlgCtrlID(h) == ctrl_id:
                return h
        except:
            pass
    return None


def _bm_click(hwnd_button: int) -> bool:
    try:
        if not _safe_is_window(hwnd_button):
            return False
        win32gui.SendMessage(hwnd_button, win32con.BM_CLICK, 0, 0)
        return True
    except:
        return False


def _send_bn_clicked_to_parent(child_hwnd: int) -> bool:
    try:
        if not _safe_is_window(child_hwnd):
            return False
        parent = win32gui.GetParent(child_hwnd) or _safe_get_toplevel(child_hwnd)
        if not _safe_is_window(parent):
            return False
        try:
            cid = win32gui.GetDlgCtrlID(child_hwnd)
        except:
            cid = 0
        wparam = ((0 & 0xFFFF) << 16) | (cid & 0xFFFF)  # notification=0, LOWORD=id
        win32gui.SendMessage(parent, win32con.WM_COMMAND, wparam, child_hwnd)
        return True
    except:
        return False


def click_child_by_id(parent_hwnd: int, ctrl_id: int) -> bool:
    """Re-trouve le bouton par ID puis tente BM_CLICK, sinon WM_COMMAND(BN_CLICKED)."""
    h = _find_child_by_id(parent_hwnd, ctrl_id)
    if not h:
        print(f"[BTN] id {ctrl_id} introuvable")
        return False
    if _bm_click(h):
        print(f"[BTN] BM_CLICK id={ctrl_id} hwnd=0x{h:08X}")
        return True
    if _send_bn_clicked_to_parent(h):
        print(f"[BTN] WM_COMMAND(BN_CLICKED) id={ctrl_id} hwnd=0x{h:08X}")
        return True
    print(f"[BTN] échec clic id={ctrl_id} hwnd=0x{h:08X}")
    return False


############### TEMPORAIRE ###############
############### TEMPORAIRE ###############

# ============ DUMPING UTILS ============


def _child_rect_client(parent_hwnd: int, child_hwnd: int):
    try:
        L, T, R, B = win32gui.GetWindowRect(child_hwnd)
        x1, y1 = _screen_to_client(parent_hwnd, L, T)
        x2, y2 = _screen_to_client(parent_hwnd, R, B)
        return x1, y1, x2, y2
    except Exception:
        return None


def dump_all_children_with_rects(parent_hwnd: int, only_buttons: bool = False):
    print(f"[DUMP-ALL] parent=0x{parent_hwnd:08X}")
    for h in _iter_children_recursive(parent_hwnd):
        cls = _safe_get_class(h)
        if only_buttons and not (
            cls.startswith("WindowsForms10.BUTTON") or cls == "Button"
        ):
            continue
        txt = _safe_get_text(h)
        rc = _child_rect_client(parent_hwnd, h)
        try:
            cid = win32gui.GetDlgCtrlID(h)
        except Exception:
            cid = None
        print(f"  hwnd=0x{h:08X} id={cid} class='{cls}' text='{txt}' rect={rc}")


def dump_near_point(
    parent_hwnd: int,
    rx: float,
    ry: float,
    radius_px: int = 40,
    only_buttons: bool = True,
):
    """Tri par distance depuis (rx, ry) en client du parent. Très utile pour identifier l’icône visée."""
    l, t, r, b = win32gui.GetClientRect(parent_hwnd)
    W, H = max(1, r - l), max(1, b - t)
    px = int(max(0, min(1, rx)) * W)
    py = int(max(0, min(1, ry)) * H)

    print(
        f"[DUMP-NEAR] parent=0x{parent_hwnd:08X} point=({px},{py}) radius={radius_px} only_buttons={only_buttons}"
    )
    rows = []
    for h in _iter_children_recursive(parent_hwnd):
        cls = _safe_get_class(h)
        if only_buttons and not (
            cls.startswith("WindowsForms10.BUTTON") or cls == "Button"
        ):
            continue
        rc = _child_rect_client(parent_hwnd, h)
        if not rc:
            continue
        x1, y1, x2, y2 = rc
        cx, cy = (x1 + x2) // 2, (y1 + y2) // 2
        dx, dy = cx - px, cy - py
        d2 = dx * dx + dy * dy
        if abs(dx) <= radius_px and abs(dy) <= radius_px:
            try:
                cid = win32gui.GetDlgCtrlID(h)
            except Exception:
                cid = None
            rows.append(
                (d2, h, cid, cls, _safe_get_text(h), (x1, y1, x2, y2), (cx, cy))
            )

    rows.sort(key=lambda z: z[0])
    if not rows:
        print("  (aucun contrôle dans le rayon)")
        return

    for d2, h, cid, cls, txt, rc, (cx, cy) in rows[:20]:
        print(
            f"  d2={d2:<6} hwnd=0x{h:08X} id={cid} class='{cls}' text='{txt}' rect={rc} center=({cx},{cy})"
        )


def dump_buttons(parent_hwnd: int):
    dump_all_children_with_rects(parent_hwnd, only_buttons=True)


#######################   FIN DES HELPERS DE MERDE     #######################
#######################   FIN DES HELPERS DE MERDE     #######################
#######################   FIN DES HELPERS DE MERDE     #######################

#######################   TREE ZEBI    #######################
#######################   TREE ZEBI    #######################
#######################   TREE ZEBI    #######################

TV_FIRST = 0x1100
TVM_GETNEXTITEM = TV_FIRST + 10
TVM_SELECTITEM = TV_FIRST + 11
TVM_ENSUREVISIBLE = TV_FIRST + 20
TVM_GETITEMRECT = TV_FIRST + 4

TVGN_ROOT = 0x0000
TVGN_NEXT = 0x0001
TVGN_CARET = 0x0009

WM_NOTIFY = 0x004E
NM_DBLCLK = -3


def _find_first_treeview(hwnd_parent: int) -> int | None:
    queue, seen = [hwnd_parent], set()
    while queue:
        h = queue.pop(0)
        if not h or h in seen:
            continue
        seen.add(h)
        try:
            cls = win32gui.GetClassName(h) or ""
            if "SysTreeView32" in cls:
                return h
        except Exception:
            pass
        try:
            kids = []
            win32gui.EnumChildWindows(h, lambda ch, _: kids.append(ch), None)
            queue.extend(kids)
        except Exception:
            pass
    return None


def _find_first_treeview(hwnd_parent: int) -> int | None:
    queue, seen = [hwnd_parent], set()
    while queue:
        h = queue.pop(0)
        if not h or h in seen:
            continue
        seen.add(h)
        try:
            cls = win32gui.GetClassName(h) or ""
            if "SysTreeView32" in cls:
                return h
        except Exception:
            pass
        try:
            kids = []
            win32gui.EnumChildWindows(h, lambda ch, _: kids.append(ch), None)
            queue.extend(kids)
        except Exception:
            pass
    return None


def _tv_select_by_index_safe(tv: int, index: int) -> int | None:
    try:
        if not tv or not win32gui.IsWindow(tv):
            return None
        h = win32gui.SendMessage(tv, TVM_GETNEXTITEM, TVGN_ROOT, 0)
        if not h:
            return None
        for _ in range(max(0, int(index))):
            nxt = win32gui.SendMessage(tv, TVM_GETNEXTITEM, TVGN_NEXT, h)
            if not nxt:
                break
            h = nxt
        win32gui.SendMessage(tv, TVM_ENSUREVISIBLE, 0, h)
        ok = win32gui.SendMessage(tv, TVM_SELECTITEM, TVGN_CARET, h)
        return h if ok else None
    except Exception:
        return None


def _tv_row_y_for_index(tv: int, index: int) -> int:
    try:
        l, t, r, b = win32gui.GetClientRect(tv)
        H = max(1, b - t)
    except Exception:
        H = 600
    row_h = max(16, min(24, H // 32 or 18))
    y = 8 + row_h * max(0, int(index))
    return max(4, min(max(5, H - 6), y))


def _notify_parent_nm_dblclk_safe(tv: int) -> bool:
    try:
        if not tv or not win32gui.IsWindow(tv):
            return False
        parent = win32gui.GetParent(tv) or win32gui.GetAncestor(tv, win32con.GA_PARENT)
        if not parent or not win32gui.IsWindow(parent):
            return False
        try:
            ctrl_id = win32gui.GetDlgCtrlID(tv) or 0
        except Exception:
            ctrl_id = 0
        nm = NMHDR()
        nm.hwndFrom = tv
        nm.idFrom = ctrl_id
        nm.code = ctypes.c_uint(NM_DBLCLK).value
        SendMessageW(parent, WM_NOTIFY, ctrl_id, ctypes.byref(nm))
        return True
    except Exception:
        return False


def _post_true_double_click(hwnd_target: int, x: int, y: int):
    try:
        lp = (y << 16) | (x & 0xFFFF)
        dct_ms = int(_user32.GetDoubleClickTime() or 500)
        PostMessageW(hwnd_target, win32con.WM_MOUSEMOVE, 0, lp)
        PostMessageW(hwnd_target, win32con.WM_LBUTTONDOWN, win32con.MK_LBUTTON, lp)
        PostMessageW(hwnd_target, win32con.WM_LBUTTONUP, 0, lp)
        time.sleep((dct_ms * 0.35) / 1000.0)
        PostMessageW(hwnd_target, win32con.WM_LBUTTONDOWN, win32con.MK_LBUTTON, lp)
        PostMessageW(hwnd_target, win32con.WM_LBUTTONDBLCLK, win32con.MK_LBUTTON, lp)
        PostMessageW(hwnd_target, win32con.WM_LBUTTONUP, 0, lp)
    except Exception:
        pass


def _tv_row_y_for_index(tv: int, index: int) -> int:
    """Heuristique : calcule un y plausible pour la ligne `index` (client coords)."""
    l, t, r, b = win32gui.GetClientRect(tv)
    H = max(1, b - t)
    # heuristique de hauteur de ligne (16–22px en général)
    row_h = max(16, min(24, H // 32 or 18))
    y = 8 + row_h * max(0, int(index))
    # clamp
    return max(4, min(H - 6, y))


def _post_true_double_click(hwnd_target: int, x: int, y: int):
    lp = (y << 16) | (x & 0xFFFF)
    dct_ms = int(_user32.GetDoubleClickTime() or 500)
    PostMessageW(hwnd_target, win32con.WM_MOUSEMOVE, 0, lp)
    PostMessageW(hwnd_target, win32con.WM_LBUTTONDOWN, win32con.MK_LBUTTON, lp)
    PostMessageW(hwnd_target, win32con.WM_LBUTTONUP, 0, lp)
    time.sleep((dct_ms * 0.35) / 1000.0)
    PostMessageW(hwnd_target, win32con.WM_LBUTTONDOWN, win32con.MK_LBUTTON, lp)
    PostMessageW(hwnd_target, win32con.WM_LBUTTONDBLCLK, win32con.MK_LBUTTON, lp)
    PostMessageW(hwnd_target, win32con.WM_LBUTTONUP, 0, lp)


def activate_controller_by_index_background_strong(
    main_hwnd: int, index: int = 0
) -> bool:
    """
    100% background, pas de foreground volé.
    - re-résout le TreeView à CHAQUE étape (jamais de handle périmé)
    - SELECT caret index
    - WM_SETFOCUS (focus logique interne au process, pas de vol de foreground)
    - Double-clic asynchrone sur plusieurs X plausibles à la ligne 'index'
    - WM_NOTIFY(NM_DBLCLK) au parent en backup
    """
    # 1) resolve tv vivant
    tv = _find_first_treeview(main_hwnd)
    if not tv or not win32gui.IsWindow(tv):
        print("[TV] introuvable")
        return False

    # 2) sélection caret
    hitem = _tv_select_by_index_safe(tv, index)
    if not hitem:
        print(f"[TV] select index={index} impossible")
        return False
    # print(f"[TV] caret on index={index} hItem=0x{int(hitem):08X}")

    # 3) re-resolve une seconde fois pour contrer recréation async
    tv2 = _find_first_treeview(main_hwnd)
    if tv2 and tv2 != tv and win32gui.IsWindow(tv2):
        tv = tv2

    # 4) focus logique interne (pas de SetForegroundWindow)
    try:
        SendMessageW(tv, win32con.WM_SETFOCUS, 0, 0)
    except Exception:
        pass

    # 5) calcule une ligne Y plausible
    y = _tv_row_y_for_index(tv, index)
    # plusieurs X candidats (selon indentation/icone/texte)
    try:
        l, t, r, b = win32gui.GetClientRect(tv)
    except Exception:
        l, t, r, b = 0, 0, 600, 400
    xs = [28, 42, 64, max(80, (r - l) // 4)]

    # 6) envoi des dblclicks de fond
    for x in xs:
        _post_true_double_click(tv, int(x), int(y))
        time.sleep(0.08)

    # 7) bonus : notifie aussi le parent
    _notify_parent_nm_dblclk_safe(tv)

    return True


#######################   FIN TREE ZEBI    #######################
#######################   FIN TREE ZEBI    #######################
#######################   FIN TREE ZEBI    #######################
#######################   FIN TREE ZEBI    #######################


def _persist_holdings_to_disk():
    """Sauvegarde holdings (TS/M) + timestamp dans HOLDINGS_STATE_PATH (écriture atomique)."""
    dirpath = os.path.dirname(HOLDINGS_STATE_PATH) or "."
    try:
        os.makedirs(dirpath, exist_ok=True)
    except Exception:
        pass

    with _revenue_lock:
        out = {
            "schema": 1,
            "last_player_update_ts": _revenue_data.get("last_player_update_ts", 0),
            "holdings": {
                "TS": dict((_revenue_data.get("holdings") or {}).get("TS", {})),
                "M": dict((_revenue_data.get("holdings") or {}).get("M", {})),
            },
        }

    tmp_fd, tmp_path = tempfile.mkstemp(prefix="holdings_", suffix=".json", dir=dirpath)
    try:
        with os.fdopen(tmp_fd, "w", encoding="utf-8") as f:
            json.dump(out, f, ensure_ascii=False, indent=2)
        os.replace(tmp_path, HOLDINGS_STATE_PATH)
    except Exception:
        try:
            os.remove(tmp_path)
        except Exception:
            pass


def _load_holdings_from_disk():
    """Charge holdings sauvegardés (TS/M). Accepte legacy 'Metier' et normalise vers 'M'."""
    try:
        if not os.path.exists(HOLDINGS_STATE_PATH):
            return False
        with open(HOLDINGS_STATE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        holds_in = data.get("holdings", {}) or {}
        last_ts = data.get("last_player_update_ts", 0)

        ts_map = holds_in.get("TS", {}) or {}
        m_map = holds_in.get("M", {}) or holds_in.get("Metier", {}) or {}

        with _revenue_lock:
            _revenue_data.setdefault("holdings", {"TS": {}, "M": {}})
            _revenue_data["holdings"].setdefault("TS", {})
            _revenue_data["holdings"].setdefault("M", {})
            for k, v in ts_map.items():
                try:
                    _revenue_data["holdings"]["TS"][k] = int(v)
                except Exception:
                    pass
            for k, v in m_map.items():
                try:
                    _revenue_data["holdings"]["M"][k] = int(v)
                except Exception:
                    pass
            if last_ts:
                _revenue_data["last_player_update_ts"] = last_ts
        return True
    except Exception:
        return False


def _holdings_snapshot():
    with _revenue_lock:
        holds = _revenue_data.get("holdings") or {"TS": {}, "M": {}}
        ts_map = holds.get("TS", {}) if isinstance(holds, dict) else {}
        m_map = holds.get("M", {}) if isinstance(holds, dict) else {}
        return {
            "holdings": {
                "TS": dict(ts_map) if isinstance(ts_map, dict) else {},
                "M": dict(m_map) if isinstance(m_map, dict) else {},
            },
            "last_player_update_ts": _revenue_data.get("last_player_update_ts", 0),
        }


def _normalize_holdings_payload(payload: dict) -> Dict[str, Dict[str, int]]:
    normalized = {"TS": {}, "M": {}}
    if not isinstance(payload, dict):
        return normalized

    alias_sets = {
        "TS": {"TS"},
        "M": {"M", "METIER", "MÉTIER", "METI"},
    }

    def add_entries(kind_key: str, entries):
        if not isinstance(entries, dict):
            return
        for server, value in entries.items():
            try:
                val_int = int(float(value))
            except Exception:
                continue
            normalized[kind_key][str(server)] = val_int

    for key, val in payload.items():
        if key is None:
            continue
        upper = str(key).strip().upper()
        for target_kind, aliases in alias_sets.items():
            if upper in aliases:
                add_entries(target_kind, val)
                break

    return normalized


def _apply_holdings_payload(payload: dict, last_ts=None) -> bool:
    normalized = _normalize_holdings_payload(payload)
    changed = False

    with _revenue_lock:
        holds = _revenue_data.setdefault("holdings", {"TS": {}, "M": {}})
        for kind in ("TS", "M"):
            if not normalized[kind]:
                continue
            kind_map = holds.setdefault(kind, {})
            for server, value in normalized[kind].items():
                if kind_map.get(server) != value:
                    kind_map[server] = value
                    changed = True
        if last_ts is not None:
            try:
                ts_val = float(last_ts)
                if _revenue_data.get("last_player_update_ts") != ts_val:
                    _revenue_data["last_player_update_ts"] = ts_val
                    changed = True
            except Exception:
                pass

    if changed:
        try:
            _persist_holdings_to_disk()
        except Exception:
            pass
        try:
            bus.revenue_updated.emit()
        except Exception:
            pass

    return changed


VALID_DAYS_KEYS = {"mon": 0, "tue": 1, "wed": 2, "thu": 3, "fri": 4, "sat": 5, "sun": 6}


def load_prefs() -> dict:
    try:
        if os.path.exists(PREFS_FILE):
            with open(PREFS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)

            def deep_merge(a, b):
                if isinstance(a, dict) and isinstance(b, dict):
                    out = dict(a)
                    for k, v in b.items():
                        if k in out:
                            out[k] = deep_merge(out[k], v)
                        else:
                            out[k] = v
                    return out
                return b if b is not None else a

            return deep_merge(DEFAULT_PREFS, data)
    except Exception:
        pass
    # Fichier absent : on le crée avec les valeurs par défaut
    default = json.loads(json.dumps(DEFAULT_PREFS))
    try:
        save_prefs(default)
    except Exception:
        pass
    return default


def save_prefs(prefs: dict):
    try:
        with open(PREFS_FILE, "w", encoding="utf-8") as f:
            json.dump(prefs, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print("WARN save_prefs:", e)


def get_bot_root(prefs: dict) -> str:
    """Retourne le chemin du répertoire bot (instance), normalisé. Utilise prefs['bot_root'] si présent."""
    raw = (prefs or {}).get("bot_root", "").strip()
    if raw:
        return os.path.normpath(os.path.abspath(raw))
    return os.path.normpath(_BOT_ROOT_DEFAULT)


def resolve_path_from_bot_root(
    prefs: dict, relative_path: str, default: str = ""
) -> str:
    """Si relative_path est relatif, le préfixe avec bot_root ; sinon retourne relative_path tel quel."""
    p = (relative_path or default).strip()
    if not p:
        return ""
    if os.path.isabs(p):
        return os.path.normpath(p)
    root = get_bot_root(prefs)
    return os.path.normpath(os.path.join(root, p))


def resolve_autopilot_config_path(prefs: dict, config_value: str) -> str:
    """Retourne le chemin complet du fichier config d'autopilot. Si relatif, sous SnowMaster/autopilot/."""
    if not (config_value or "").strip():
        return ""
    cv = config_value.strip()
    if os.path.isabs(cv):
        return os.path.normpath(cv)
    return os.path.normpath(os.path.join(AUTOPILOT_DIR, cv))


def get_autoload_instances_file(prefs: dict) -> str:
    """Fichier des instances à charger au démarrage : Instances.json dans SnowMaster, ou ancienne clé prefs."""
    if (prefs or {}).get("bot_root"):
        return INSTANCES_FILE
    return (prefs or {}).get("autoload_instances_file") or INSTANCES_FILE


def parse_time_hhmm(s: str) -> Optional[Tuple[int, int]]:
    try:
        hh, mm = s.strip().split(":")
        return int(hh), int(mm)
    except Exception:
        return None


def now_weekday_hour_min():
    now = datetime.now()
    return now.weekday(), now.hour, now.minute


def day_matches(spec: str, wd: int) -> bool:
    spec = (spec or "").strip().lower()
    if not spec or spec == "all" or spec == "*":
        return True
    parts = [p.strip() for p in spec.replace(" ", "").split(",") if p.strip()]
    wanted = set()
    for p in parts:
        if "-" in p:
            a, b = p.split("-", 1)
            if a in VALID_DAYS_KEYS and b in VALID_DAYS_KEYS:
                ai, bi = VALID_DAYS_KEYS[a], VALID_DAYS_KEYS[b]
                if ai <= bi:
                    rng = range(ai, bi + 1)
                else:
                    rng = list(range(ai, 7)) + list(range(0, bi + 1))
                for x in rng:
                    wanted.add(x)
        else:
            if p in VALID_DAYS_KEYS:
                wanted.add(VALID_DAYS_KEYS[p])
    if not wanted:
        return True
    return wd in wanted


def time_in_range(hhmm_from: str, hhmm_to: str, h: int, m: int) -> bool:
    t_from = parse_time_hhmm(hhmm_from) or (0, 0)
    t_to = parse_time_hhmm(hhmm_to) or (23, 59)
    t_now = (h, m)

    def to_minutes(t):
        return t[0] * 60 + t[1]

    f = to_minutes(t_from)
    t = to_minutes(t_to)
    n = to_minutes(t_now)
    if f <= t:
        return f <= n <= t
    return n >= f or n <= t


def match_autopilot_schedule(prefs: dict) -> Optional[dict]:
    ap = prefs.get("autopilot") or {}
    schedules = ap.get("schedules") or []
    wd, hh, mm = now_weekday_hour_min()
    for sch in schedules:
        days = sch.get("days", "all")
        fr = sch.get("from", "00:00")
        to = sch.get("to", "23:59")
        if day_matches(days, wd) and time_in_range(fr, to, hh, mm):
            return sch
    return None


# ======================= WIN32 UTILS ======================
def bring_to_front(hwnd):
    """Met la fenêtre au premier plan sur l'écran le plus à gauche."""
    try:
        # D'abord, déplacer la fenêtre sur l'écran de gauche
        monitor = get_leftmost_monitor()
        screen_left = monitor["left"]
        screen_top = monitor["top"]
        screen_width = monitor["width"]
        screen_height = monitor["height"]

        # Placer au coin en haut à gauche de l'écran de gauche
        x = screen_left
        y = screen_top

        # Restaurer si minimisée
        win32gui.ShowWindow(hwnd, win32con.SW_RESTORE)
        time.sleep(0.05)

        # Déplacer vers l'écran de gauche
        win32gui.SetWindowPos(
            hwnd,
            win32con.HWND_TOP,
            x,
            y,
            0,
            0,
            win32con.SWP_NOSIZE | win32con.SWP_SHOWWINDOW,
        )

        # Mettre au premier plan
        win32gui.SetForegroundWindow(hwnd)
    except Exception:
        try:
            win32gui.SetWindowPos(
                hwnd,
                win32con.HWND_TOPMOST,
                0,
                0,
                0,
                0,
                win32con.SWP_NOMOVE | win32con.SWP_NOSIZE,
            )
            win32gui.SetWindowPos(
                hwnd,
                win32con.HWND_NOTOPMOST,
                0,
                0,
                0,
                0,
                win32con.SWP_NOMOVE | win32con.SWP_NOSIZE,
            )
        except Exception:
            pass


def get_leftmost_monitor():
    """Retourne les coordonnées et dimensions du moniteur le plus à gauche (non principal)."""
    try:
        # EnumDisplayMonitors retourne directement une liste de moniteurs
        # Format: [(hMonitor, hdcMonitor, (left, top, right, bottom)), ...]
        monitors_info = win32api.EnumDisplayMonitors()

        monitors = []
        for monitor_data in monitors_info:
            # monitor_data[2] contient (left, top, right, bottom)
            rect = monitor_data[2]
            monitors.append(
                {
                    "left": rect[0],
                    "top": rect[1],
                    "right": rect[2],
                    "bottom": rect[3],
                    "width": rect[2] - rect[0],
                    "height": rect[3] - rect[1],
                }
            )

        if monitors:
            # Trier par coordonnée left (X) pour trouver l'écran le plus à gauche
            leftmost = min(monitors, key=lambda m: m["left"])
            print(
                f"✓ Écran le plus à gauche trouvé: position ({leftmost['left']}, {leftmost['top']}), taille {leftmost['width']}x{leftmost['height']}"
            )
            return leftmost
    except Exception as e:
        print(f"Erreur get_leftmost_monitor: {e}")

    # Fallback : écran principal
    return {
        "left": 0,
        "top": 0,
        "width": win32api.GetSystemMetrics(0),
        "height": win32api.GetSystemMetrics(1),
        "right": win32api.GetSystemMetrics(0),
        "bottom": win32api.GetSystemMetrics(1),
    }


def center_window_on_first_screen(hwnd):
    """Centre la fenêtre au milieu de l'écran le plus à gauche (pour garder l'écran principal libre)."""
    try:
        # Obtenir l'écran le plus à gauche
        monitor = get_leftmost_monitor()
        screen_left = monitor["left"]
        screen_top = monitor["top"]
        screen_width = monitor["width"]
        screen_height = monitor["height"]

        # Obtenir la taille de la fenêtre - appel unique
        rect = win32gui.GetWindowRect(hwnd)
        window_width = rect[2] - rect[0]
        window_height = rect[3] - rect[1]

        # Calculer la position pour centrer la fenêtre sur l'écran de gauche
        x = screen_left + (screen_width - window_width) // 2
        y = screen_top + (screen_height - window_height) // 2

        # Restaurer la fenêtre si elle est minimisée
        win32gui.ShowWindow(hwnd, win32con.SW_RESTORE)

        # Déplacer et mettre au premier plan en une seule opération
        win32gui.SetWindowPos(
            hwnd,
            win32con.HWND_TOP,
            x,
            y,
            0,
            0,
            win32con.SWP_NOSIZE | win32con.SWP_SHOWWINDOW,
        )

        # S'assurer que la fenêtre devient active (foreground)
        try:
            win32gui.SetForegroundWindow(hwnd)
        except Exception:
            # Si SetForegroundWindow échoue (restrictions Windows), on essaie avec TOPMOST
            try:
                win32gui.SetWindowPos(
                    hwnd,
                    win32con.HWND_TOPMOST,
                    x,
                    y,
                    0,
                    0,
                    win32con.SWP_NOSIZE | win32con.SWP_SHOWWINDOW,
                )
                win32gui.SetWindowPos(
                    hwnd,
                    win32con.HWND_NOTOPMOST,
                    x,
                    y,
                    0,
                    0,
                    win32con.SWP_NOSIZE | win32con.SWP_SHOWWINDOW,
                )
            except Exception:
                pass

        return True
    except Exception as e:
        print(f"Erreur lors du centrage de la fenêtre: {e}")
        return False


def ensure_button_visible(hwnd, rx, ry):
    """Vérifie rapidement que le bouton est visible. Retourne True si visible, False sinon."""
    try:
        # Un seul appel pour obtenir la géométrie de la fenêtre
        rect = win32gui.GetWindowRect(hwnd)
        window_left = rect[0]
        window_top = rect[1]
        window_width = rect[2] - rect[0]
        window_height = rect[3] - rect[1]

        # Calculer la position absolue du bouton
        button_x = window_left + int(window_width * rx)
        button_y = window_top + int(window_height * ry)

        # Obtenir la géométrie du premier écran - appel unique
        screen_width = win32api.GetSystemMetrics(0)
        screen_height = win32api.GetSystemMetrics(1)

        # Vérification simple : le bouton doit être dans les limites de l'écran
        # et la fenêtre doit être au moins partiellement visible
        is_visible = (
            0 <= button_x < screen_width
            and 0 <= button_y < screen_height
            and window_left < screen_width  # Au moins partiellement visible
            and window_top < screen_height
            and window_left + window_width > 0  # Au moins partiellement visible
            and window_top + window_height > 0
        )

        return is_visible
    except Exception:
        # En cas d'erreur, on considère que ce n'est pas visible pour forcer le centrage
        return False


# --- Flash du bouton de la fenêtre dans la barre des tâches (alerte rouge) ---
class FLASHWINFO(ctypes.Structure):
    _fields_ = [
        ("cbSize", wintypes.UINT),
        ("hwnd", wintypes.HWND),
        ("flags", wintypes.DWORD),
        ("uCount", wintypes.UINT),
        ("dwTimeout", wintypes.DWORD),
    ]


FLASHW_STOP = 0x00000000
FLASHW_CAPTION = 0x00000001
FLASHW_TRAY = 0x00000002
FLASHW_ALL = FLASHW_CAPTION | FLASHW_TRAY
FLASHW_TIMER = 0x00000004
FLASHW_TIMERNOFG = 0x0000000C


def flash_master_window(hwnd: int, count: int = 5):
    """
    Fait clignoter le bouton de la fenêtre dans la barre des tâches (Windows).
    Utilisé quand le voyant global passe au rouge et que la fenêtre n'est pas active.
    (Fenêtre de l'orchestrateur : SnowMaster ou AnkaMaster selon appVariant.)
    """
    try:
        if not hwnd or not win32gui.IsWindow(hwnd):
            return
        fi = FLASHWINFO()
        fi.cbSize = ctypes.sizeof(FLASHWINFO)
        fi.hwnd = int(hwnd)
        fi.flags = FLASHW_TRAY | FLASHW_TIMERNOFG
        fi.uCount = max(1, int(count))
        fi.dwTimeout = 0  # cadence par défaut
        _user32.FlashWindowEx(ctypes.byref(fi))
    except Exception as e:
        print(f"[FLASH] FlashWindowEx failed: {e}")


def get_main_hwnd(pid: int):
    result, max_area = None, 0

    def enum_handler(hwnd, _):
        nonlocal result, max_area
        try:
            if not win32gui.IsWindowVisible(hwnd):
                return
            _, wnd_pid = win32process.GetWindowThreadProcessId(hwnd)
            if wnd_pid != pid:
                return
            if win32gui.GetWindow(hwnd, win32con.GW_OWNER):
                return
            l, t, r, b = win32gui.GetWindowRect(hwnd)
            area = max(0, r - l) * max(0, b - t)
            if area > max_area:
                max_area = area
                result = hwnd
        except Exception:
            pass

    win32gui.EnumWindows(enum_handler, None)
    return result


def is_pid_alive(pid: Optional[int]) -> bool:
    try:
        return (
            bool(pid)
            and psutil.pid_exists(int(pid))
            and psutil.Process(int(pid)).is_running()
        )
    except Exception:
        return False


def is_hwnd_valid(hwnd: Optional[int]) -> bool:
    try:
        return (
            bool(hwnd)
            and win32gui.IsWindow(int(hwnd))
            and win32gui.IsWindowVisible(int(hwnd))
        )
    except Exception:
        return False


# ===================== SHARED STATE =======================
class InstanceState:
    def __init__(self, title: str):
        self.title: str = title
        self.pid: Optional[int] = None
        self.hwnd: Optional[int] = None
        # self.sub_map: Dict[str, float] = {}
        self.sub_map: Dict[str, Dict[str, float]] = {}
        self.last_heartbeat: float = 0.0
        self.last_reset: float = 0.0  # <-- NOUVEAU : date/heure du dernier lancement

        self.logs: Deque[str] = deque(maxlen=MAX_LOGS_PER_INSTANCE)
        self.stopped: bool = False
        self.awaiting_first_hb: bool = True
        self.controller_path: Optional[str] = None
        self.exe_path: Optional[str] = None
        self.images_dir: Optional[str] = None
        self.ratio: float = 0.5
        self.restored_recently: bool = False  # <--- nouveau flag
        self.manual_empty: bool = False


_instances: Dict[str, InstanceState] = {}
_state_lock = threading.Lock()

# ----------------- Shared revenue data (prices & holdings) -----------------
# Structure :
# _revenue_data = {
#     "servers": { "Imagiro": 8.25, "Orukam": 3.00, ... },   # prix unique €/M
#     "holdings": { "TS": {"Imagiro": 50}, "M": {"Orukam": 100} }
# }
_revenue_data = {
    "servers": {},  # prix unique du kama par serveur
    "holdings": {"TS": {}, "M": {}},  # millions de kamas
}
_revenue_lock = threading.Lock()

# ---- Global mouse lock (HTTP orchestrated) ----
_mouse_lock = threading.Lock()
_mouse_lock_owner: Optional[str] = None
_mouse_lock_until: float = 0.0


def _now():
    return time.time()


def _lock_is_free() -> bool:
    global _mouse_lock_owner, _mouse_lock_until
    return (not _mouse_lock_owner) or (_now() > _mouse_lock_until)


def _lock_remaining() -> float:
    return max(0.0, _mouse_lock_until - _now())


class EventBus(QObject):
    instance_updated = Signal(str)
    instance_removed = Signal(str)
    new_instance = Signal(str)
    # signal pour notifier que les prix / holdings ont changé
    revenue_updated = Signal()
    prices_fetch_finished = Signal(
        bool, int, str
    )  # <--- AJOUT (success, count, error_message)
    reset_instance = Signal(str)
    goodbye_kill = Signal(str)  # NEW


bus = EventBus()

# Prefs (global)
_prefs = load_prefs()

# Icônes globales : chemins résolus à partir des prefs (fichiers sous SnowMaster/images/ ou chemins absolus).
# On ne crée PAS de QIcon ici pour éviter l'erreur "QPixmap: Must construct a QGuiApplication before a QPixmap".
DEFAULT_ICON_FILENAMES = {
    "play": "play.png",
    "focus": "show.png",
    "stop": "stop.png",
    "trash": "poubelle.png",
}
# Dictionnaire de chemins effectifs par clé d'icône (play/focus/stop/trash)
ICON_PATHS: Dict[str, str] = {}


def _init_icons_from_prefs():
    """
    Initialise ICON_PATHS à partir des prefs + fallbacks (SnowMaster/images/).
    Ne construit aucun QIcon (pour rester safe avant la création du QApplication).
    """
    icons_cfg = _prefs.setdefault("icons", {})
    changed = False

    for key, default_name in DEFAULT_ICON_FILENAMES.items():
        raw = icons_cfg.get(key) or default_name
        if not raw:
            raw = default_name
            icons_cfg[key] = default_name
            changed = True
        if os.path.isabs(raw):
            path = os.path.normpath(raw)
        else:
            path = os.path.normpath(os.path.join(IMAGES_DIR, raw))
        ICON_PATHS[key] = path
        if not icons_cfg.get(key):
            icons_cfg[key] = default_name
            changed = True

    if changed:
        try:
            save_prefs(_prefs)
        except Exception:
            pass


def get_icon(name: str) -> QIcon:
    """
    Construit un QIcon à la demande à partir des chemins configurés.
    Retourne un QIcon vide si le fichier est introuvable.
    """
    path = ICON_PATHS.get(name)
    if not path and name in DEFAULT_ICON_FILENAMES:
        path = os.path.join(IMAGES_DIR, DEFAULT_ICON_FILENAMES[name])
    if not path:
        return QIcon()
    try:
        if os.path.exists(path):
            return QIcon(path)
    except Exception:
        return QIcon()
    return QIcon()


_init_icons_from_prefs()


# ====================== APP VARIANT (branding + heuristiques) ======================
# appVariant = le CLIENT orchestré (logiciel dont on lance des instances) :
#   - "snowbot"  → cette app s'appelle SnowMaster, client = SnowBot
#   - "ankabot"  → cette app s'appelle AnkaMaster, client = AnkaBot
# Ne pas confondre : le "master" (SnowMaster/AnkaMaster) est cette GUI ; le "bot" (SnowBot/AnkaBot) est l'exe des instances.
def _normalize_app_variant(v) -> str:
    s = str(v or "").strip().lower()
    if s in ("ankabot", "ankamaster", "anka", "am"):
        return "ankabot"
    if s in ("snowbot", "snow", "sb"):
        return "snowbot"
    return "snowbot"


APP_VARIANT = _normalize_app_variant((_prefs or {}).get("appVariant"))
if APP_VARIANT == "ankabot":
    APP_DISPLAY_NAME = "AnkaMaster"  # nom de cette application (orchestrateur)
    APP_EXE_NAME = "AnkaBot"  # nom de l'exe des instances
    APP_ALERT_TAG = "ANKAMASTER"
else:
    APP_DISPLAY_NAME = "SnowMaster"  # nom de cette application (orchestrateur)
    APP_EXE_NAME = "SnowBot"  # nom de l'exe des instances
    APP_ALERT_TAG = "SNOWMASTER"

# Version applicative (mise à jour manuelle avant chaque build).
APP_VERSION = "0.6.0"

# URL du manifest de mise à jour (JSON) hébergé sur le repo public d'updates.
# À adapter : par exemple
#   https://raw.githubusercontent.com/<user>/snowmaster-updates/main/manifest.json
UPDATE_MANIFEST_URL = "https://example.com/snowmaster/manifest.json"

# Nom de l'exécutable de mise à jour, attendu à côté de l'exe principal.
UPDATER_EXE_NAME = "SnowMasterUpdater.exe"

# Discord / webhooks
DISCORD_ALERT_SEARCH_TOKEN = f"ALERTE {APP_ALERT_TAG}"
DISCORD_ALERT_SYSTEM_FOOTER = f"{APP_DISPLAY_NAME} Alert System"
DISCORD_WEBHOOK_USER_AGENT = f"{APP_DISPLAY_NAME}/1.0 (Discord Webhook)"

# Détection du processus de notre GUI (orchestrateur), pour ne pas le confondre avec les instances.
# Tokens = noms possibles de l'app (SnowMaster / AnkaMaster), pas les clients (SnowBot / AnkaBot).
MASTER_GUI_NAME_TOKENS = tuple(
    {
        "snowmaster",
        "ankamaster",
        APP_DISPLAY_NAME.lower(),
    }
)


def _get_configured_client_basename() -> Optional[str]:
    """Nom d'exe (basename) du client à détecter/restaurer, basé sur la pref `exe`."""
    try:
        exe = str(EXE or "").strip()
        if exe:
            return os.path.basename(exe).lower()
    except Exception:
        pass
    return None


# ====================== FLASK SERVER ======================
flask_app = Flask(__name__)


def _read_payload():
    data = request.get_json(silent=True)
    if data:
        return data
    if request.form:
        return request.form.to_dict()
    raw = request.get_data(as_text=True) or ""
    raw = raw.strip()
    if raw.startswith("{") and raw.endswith("}"):
        try:
            return json.loads(raw)
        except Exception:
            pass
    if "&" in raw or "=" in raw:
        parsed = {}
        for part in raw.split("&"):
            if "=" in part:
                k, v = part.split("=", 1)
                parsed[k] = v
        if parsed:
            return parsed
    return {}


def _extract_title(data: dict) -> Optional[str]:
    val = data.get("title")
    return str(val) if val else None


def _parse_subcontrollers(data, now_ts) -> Dict[str, Dict[str, float]]:
    """
    Retourne un mapping id -> {"alias": str, "ts": float}
    Robustifie la détection : si la valeur dict contient une clé 'id'/'sid'/'uuid'
    on l'utilise comme identifiant stable. Sinon on tombe back sur la clé parent.
    Supporte dict/list/anciennes formes.
    """
    sub_map: Dict[str, Dict[str, float]] = {}
    try:
        if data is None:
            return sub_map

        def extract_id_and_alias_from_pair(parent_key, value, fallback_ts):
            # renvoie (sid, alias, tsf)
            sid = None
            alias = None
            tsf = float(fallback_ts)
            try:
                if isinstance(value, dict):
                    # check for explicit id field in the payload
                    for cand in ("id", "sid", "uuid", "key"):
                        if cand in value and value.get(cand) is not None:
                            sid = str(value.get(cand))
                            break
                    # alias fields
                    alias = (
                        value.get("alias")
                        or value.get("name")
                        or value.get("title")
                        or None
                    )
                    # timestamp fields
                    ts = (
                        value.get("ts")
                        or value.get("last")
                        or value.get("t")
                        or value.get("timestamp")
                    )
                    try:
                        if ts is not None:
                            tsf = float(ts)
                    except Exception:
                        tsf = float(fallback_ts)
                else:
                    # scalar -> no inner id/alias
                    alias = None
                # if no explicit sid found, use parent key (stringified)
                if not sid:
                    sid = str(parent_key)
                # fallback alias to sid if missing
                if not alias:
                    alias = sid
            except Exception:
                sid = str(parent_key)
                alias = sid
                tsf = float(fallback_ts)
            return sid, str(alias), tsf

        # dict case
        if isinstance(data, dict):
            for k, v in data.items():
                try:
                    sid, alias, tsf = extract_id_and_alias_from_pair(k, v, now_ts)
                    # if v is just a number/string timestamp, try to use it
                    if isinstance(v, (int, float)):
                        tsf = float(v)
                        alias = alias or str(k)
                    elif isinstance(v, str):
                        try:
                            tsf = float(v)
                        except Exception:
                            tsf = float(now_ts)
                    sub_map[str(sid)] = {"alias": str(alias), "ts": float(tsf)}
                except Exception:
                    continue

        # list case
        elif isinstance(data, list):
            for item in data:
                try:
                    if isinstance(item, str):
                        sid = str(item)
                        sub_map[sid] = {"alias": sid, "ts": float(now_ts)}
                    elif isinstance(item, dict):
                        # prefer explicit id inside the dict
                        sid = item.get("id") or item.get("sid") or item.get("uuid")
                        if not sid:
                            # try to detect a single top-level key if the dict is like {"<id>": {...}}
                            keys = [kk for kk in item.keys() if isinstance(kk, str)]
                            if len(keys) == 1 and isinstance(
                                item[keys[0]], (dict, str, int, float)
                            ):
                                # pattern: [{"subid": {...}}]
                                inner = item[keys[0]]
                                # if inner is dict, reuse parsing
                                if isinstance(inner, dict):
                                    s2, a2, t2 = extract_id_and_alias_from_pair(
                                        keys[0], inner, now_ts
                                    )
                                    sub_map[str(s2)] = {
                                        "alias": str(a2),
                                        "ts": float(t2),
                                    }
                                    continue
                                else:
                                    sub_map[str(keys[0])] = {
                                        "alias": str(keys[0]),
                                        "ts": float(now_ts),
                                    }
                                    continue
                        # normal dict with potential fields
                        alias = (
                            item.get("alias")
                            or item.get("name")
                            or item.get("title")
                            or None
                        )
                        ts = (
                            item.get("ts")
                            or item.get("last")
                            or item.get("t")
                            or item.get("timestamp")
                        )
                        try:
                            tsf = float(ts) if ts is not None else float(now_ts)
                        except Exception:
                            tsf = float(now_ts)
                        sid_key = (
                            str(sid)
                            if sid is not None
                            else (alias or str(tsf) or str(now_ts))
                        )
                        if not alias:
                            alias = sid_key
                        sub_map[str(sid_key)] = {"alias": str(alias), "ts": float(tsf)}
                    else:
                        continue
                except Exception:
                    continue

    except Exception:
        pass
    return sub_map


@flask_app.route("/reset_instance", methods=["POST"])
def api_reset_instance():
    """
    Body attendu: {"title": "Nom exact de l'instance"}
    → émet bus.reset_instance(title) qui fera kill + relaunch côté GUI.
    """
    data = _read_payload()
    title = (data.get("title") or data.get("name") or "").strip()
    if not title:
        return jsonify({"err": "missing title"}), 400

    try:
        bus.reset_instance.emit(title)
    except Exception as e:
        return jsonify({"err": f"emit failed: {e}"}), 500

    return jsonify({"ok": True})


@flask_app.route("/register", methods=["POST"])
def api_register():
    data = _read_payload()
    title = _extract_title(data)
    if not title:
        return jsonify({"err": "missing title"}), 400
    with _state_lock:
        inst = _instances.get(title) or InstanceState(title)
        inst.title = title
        inst.awaiting_first_hb = True
        inst.stopped = False
        if "pid" in data:
            try:
                inst.pid = int(data["pid"])
            except:
                pass
        if "hwnd" in data:
            try:
                inst.hwnd = int(data["hwnd"])
            except:
                pass
        if data.get("controller"):
            inst.controller_path = str(data["controller"])
        if data.get("exe"):
            inst.exe_path = str(data["exe"])
        if data.get("images"):
            inst.images_dir = str(data["images"])
        if "ratio" in data:
            try:
                inst.ratio = float(data["ratio"])
            except:
                pass
        if "subcontrollers" in data:
            inst.sub_map = _parse_subcontrollers(data["subcontrollers"], time.time())
        if data.get("touch", True):
            now_ts = time.time()
            inst.last_heartbeat = now_ts
            inst.last_reset = now_ts  # <-- NOUVEAU : marquer le lancement
        inst.restored_recently = False  # si un register arrive, on lève le flag
        _instances[title] = inst
    bus.new_instance.emit(title)
    bus.instance_updated.emit(title)
    return jsonify({"ok": True})


@flask_app.route("/heartbeat", methods=["POST"])
def api_heartbeat():
    data = _read_payload()
    title = _extract_title(data)
    if not title:
        return jsonify({"err": "missing title"}), 400
    now_ts = time.time()
    with _state_lock:
        inst = _instances.get(title) or InstanceState(title)
        inst.title = title
        inst.stopped = False

        # update pid/hwnd if fournis
        if "pid" in data:
            try:
                inst.pid = int(data["pid"])
            except:
                pass
        if "hwnd" in data:
            try:
                inst.hwnd = int(data["hwnd"])
            except:
                pass

        # subcontrollers
        if "subcontrollers" in data:
            parsed = _parse_subcontrollers(data["subcontrollers"], now_ts)
            for k, v in parsed.items():
                inst.sub_map[k] = v

        # autres métadonnées
        if data.get("controller"):
            inst.controller_path = str(data["controller"])
        if data.get("exe"):
            inst.exe_path = str(data["exe"])
        if data.get("images"):
            inst.images_dir = str(data["images"])
        if "ratio" in data:
            try:
                inst.ratio = float(data["ratio"])
            except:
                pass

        # **Heartbeat effectif : mettre à jour last_heartbeat et lever flags de restauration**
        inst.last_heartbeat = now_ts
        inst.awaiting_first_hb = False
        # si c'était marqué restauré, on le retire pour repasser au vert
        if getattr(inst, "restored_recently", False):
            inst.restored_recently = False

        # sauvegarde dans le state global
        _instances[title] = inst

    # s'assure que l'UI voit bien l'instance (créée si manquante) et met à jour
    try:
        bus.new_instance.emit(title)
    except Exception:
        pass
    try:
        bus.instance_updated.emit(title)
    except Exception:
        pass

    return jsonify({"ok": True})


@flask_app.route("/log", methods=["POST"])
def api_log():
    data = _read_payload()
    title = _extract_title(data)
    msg = data.get("message")
    level = data.get("level", "INFO")
    if not title or msg is None:
        return jsonify({"err": "missing fields"}), 400
    with _state_lock:
        inst = _instances.get(title) or InstanceState(title)
        inst.logs.append(f"{datetime.now().strftime('%H:%M:%S')} [{level}] {msg}")
        inst.last_heartbeat = time.time()
        inst.stopped = False
        _instances[title] = inst
    bus.instance_updated.emit(title)
    return jsonify({"ok": True})


@flask_app.route("/goodbye", methods=["POST"])
def api_goodbye():
    data = _read_payload()
    title = _extract_title(data)
    if not title:
        return jsonify({"err": "missing title"}), 400
    with _state_lock:
        inst = _instances.get(title)
        if inst:
            inst.stopped = True
            # NEW: purge aussi côté serveur pour éviter toute persistance
            try:
                inst.sub_map.clear()
            except Exception:
                inst.sub_map = {}
    bus.instance_updated.emit(title)
    return jsonify({"ok": True})


@flask_app.route("/goodbye_kill", methods=["POST"])
def api_goodbye_kill():
    """
    Appelée par une instance AnkaBot qui veut se fermer ET être kill par AnkaMaster.
    Body attendu: {"title": "Nom exact de l'instance"}
    """
    data = _read_payload()
    title = _extract_title(data)
    if not title:
        return jsonify({"err": "missing title"}), 400

    try:
        bus.goodbye_kill.emit(title)
    except Exception as e:
        return jsonify({"err": f"emit failed: {e}"}), 500

    return jsonify({"ok": True})


@flask_app.route("/bank_update", methods=["POST"])
def api_bank_update():
    """
    Attendu JSON/form :
      { "alias": "B TS Orukam", "kamas": 152 }   # kamas en millions (int), ou "kamas": "152"
    On parse l'alias pour extraire le type (TS / M) et le serveur,
    on met à jour _revenue_data['holdings'][kind][server] = kamas
    puis on émet bus.revenue_updated.
    """
    data = _read_payload()
    alias = (
        data.get("alias") or data.get("name") or data.get("aliasName") or ""
    ).strip()
    kamas_raw = (
        data.get("kamas")
        if "kamas" in data
        else data.get("value") or data.get("amount")
    )

    if not alias or kamas_raw is None:
        return jsonify({"err": "missing alias or kamas"}), 400

    # parse kamas -> int (millions)
    try:
        kamas_val = int(float(kamas_raw))
    except Exception:
        return jsonify({"err": "invalid kamas value"}), 400

    # parse alias: exemple "B TS Orukam" / "B M Orukam" / "TS Orukam" / "M Orukam"
    parts = [p.strip() for p in alias.split() if p.strip()]
    kind = None

    # détecter TS / M / (anciens) METIER/MÉTIER/METI
    for p in parts:
        up = p.upper()
        if up == "TS":
            kind = "TS"
        elif up in ("M", "METIER", "MÉTIER", "METI"):
            kind = "M"

    # tokens restants susceptibles d'être le serveur
    cands = [
        p
        for p in parts
        if p.upper() not in ("TS", "M", "METIER", "MÉTIER", "METI", "B", "BANK")
    ]
    if not cands:
        return jsonify({"err": "cannot detect server from alias"}), 400
    server = " ".join(cands)

    # recharge les holdings existants depuis le disque pour merger en temps réel
    _load_holdings_from_disk()

    # normalisation serveur (match keys existantes si possible)
    with _revenue_lock:
        servers_map = _revenue_data.setdefault("servers", {})
        found = None
        for s in servers_map.keys():
            if s.strip().lower() == server.strip().lower():
                found = s
                break
        server_key = found or server

        holds = _revenue_data.setdefault("holdings", {"TS": {}, "M": {}})
        if kind is None:
            kind = "TS"
        holds.setdefault(kind, {})
        holds[kind][server_key] = kamas_val

    # persist (HORS lock)
    try:
        _persist_holdings_to_disk()
    except Exception:
        pass

    # notify UI
    try:
        bus.revenue_updated.emit()
    except Exception:
        pass

    snapshot = _holdings_snapshot()
    return jsonify(
        {"ok": True, "server": server_key, "kind": kind, "kamas": kamas_val, **snapshot}
    )


@flask_app.route("/holdings/save", methods=["POST"])
def api_holdings_save():
    data = _read_payload()
    holdings_payload = data.get("holdings")

    if holdings_payload is None:
        # fallback simple: kind/server/value
        kind = data.get("kind") or data.get("type")
        server = data.get("server") or data.get("srv")
        value = (
            data.get("value")
            if "value" in data
            else data.get("kamas") or data.get("amount")
        )
        if kind and server and value is not None:
            holdings_payload = {str(kind): {str(server): value}}

    if not isinstance(holdings_payload, dict):
        return jsonify({"err": "missing holdings payload"}), 400

    last_ts = (
        data.get("last_player_update_ts")
        or data.get("ts")
        or data.get("timestamp")
        or data.get("updated_at")
    )

    changed = _apply_holdings_payload(holdings_payload, last_ts)
    snapshot = _holdings_snapshot()
    return jsonify({"ok": True, "changed": changed, **snapshot})


@flask_app.route("/holdings/load", methods=["GET", "POST"])
def api_holdings_load():
    updated = _load_holdings_from_disk()
    if updated:
        try:
            bus.revenue_updated.emit()
        except Exception:
            pass

    snapshot = _holdings_snapshot()
    return jsonify({"ok": True, **snapshot})


# mouse lock endpoints
@flask_app.route("/mouse_lock/acquire", methods=["POST"])
def api_mouse_lock_acquire():
    global _mouse_lock_owner, _mouse_lock_until
    data = _read_payload()
    owner = str(data.get("owner") or "").strip()
    ttl = float(data.get("ttl") or 90.0)
    if not owner:
        return jsonify({"ok": False, "err": "missing owner"}), 400
    with _mouse_lock:
        if _lock_is_free():
            _mouse_lock_owner = owner
            _mouse_lock_until = _now() + max(1.0, ttl)
            return jsonify({"ok": True, "until": _mouse_lock_until})
        else:
            return (
                jsonify(
                    {
                        "ok": False,
                        "owner": _mouse_lock_owner,
                        "remaining": _lock_remaining(),
                    }
                ),
                200,
            )


@flask_app.route("/mouse_lock/release", methods=["POST"])
def api_mouse_lock_release():
    global _mouse_lock_owner, _mouse_lock_until
    data = _read_payload()
    owner = str(data.get("owner") or "").strip()
    with _mouse_lock:
        if _lock_is_free() or (owner and owner == _mouse_lock_owner):
            _mouse_lock_owner = None
            _mouse_lock_until = 0.0
            return jsonify({"ok": True})
        return (
            jsonify(
                {
                    "ok": False,
                    "owner": _mouse_lock_owner,
                    "remaining": _lock_remaining(),
                }
            ),
            200,
        )


_number_re = re.compile(r"[-+]?\d+[.,]?\d*")


def _parse_number_to_float(s: str) -> float:
    """Extrait un nombre d'une chaîne et convertit en float (gère la virgule)."""
    if not s:
        return 0.0
    m = _number_re.search(s.replace("\xa0", " "))
    if not m:
        return 0.0
    raw = m.group(0).replace(" ", "").replace(",", ".")
    try:
        return float(raw)
    except Exception:
        return 0.0


def _normalize_server(s: str) -> str:
    # trim + collapse spaces
    return re.sub(r"\s+", " ", (s or "").strip())


def _scrape_leskamas_with_playwright(
    timeout=15000, headless=True, allowed_display_names=None, display_to_scrape_map=None
) -> dict:
    """
    allowed_display_names: liste de noms côté app (ex: 'Hell Mina')
    display_to_scrape_map: dict affichage -> nom dans la table (ex: 'Hell Mina' -> 'HellMina')
    """
    url = "https://www.leskamas.com/vendre-des-kamas.html"
    servers_map = {}
    rows = []

    # Prépare l’ensemble des noms “tels qu’affichés sur le site”
    allowed_display_names = allowed_display_names or []
    display_to_scrape_map = display_to_scrape_map or {}
    allowed_scrape_names = {
        _normalize_server(display_to_scrape_map.get(d, d))
        for d in allowed_display_names
    }

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=headless)
            page = browser.new_page()
            page.set_default_navigation_timeout(timeout)
            page.goto(url)

            page.wait_for_selector("table", timeout=8000)

            # trouve la table qui contient “Serveur” dans l’entête
            tables = page.query_selector_all("table")
            chosen = None
            for t in tables:
                txt = (t.inner_text() or "").lower()
                if "serveur" in txt:
                    chosen = t
                    break
            if chosen is None and tables:
                chosen = tables[0]

            # header -> index des colonnes
            header_cells = chosen.query_selector_all("thead th, thead td")
            col_names = []
            if header_cells:
                col_names = [h.inner_text().strip().lower() for h in header_cells]
            else:
                first_tr = chosen.query_selector("tr")
                if first_tr:
                    col_names = [
                        h.inner_text().strip().lower()
                        for h in first_tr.query_selector_all("td, th")
                    ]

            idx_server = None
            idx_skrill_sepa = None
            idx_status = None
            for i, name in enumerate(col_names):
                if "serveur" in name:
                    idx_server = i
                if "skrill" in name or "sepa" in name:
                    idx_skrill_sepa = i
                if "stat" in name or "stock" in name or "status" in name:
                    idx_status = i

            # lignes
            for tr in chosen.query_selector_all("tbody tr, tr"):
                tds = tr.query_selector_all("td, th")
                if not tds or len(tds) < 2:
                    continue
                texts = [(td.inner_text() or "").strip() for td in tds]

                # serveur
                server_raw = (
                    texts[idx_server]
                    if (idx_server is not None and idx_server < len(texts))
                    else texts[0]
                )
                server_scraped = _normalize_server(server_raw)

                # ➜ filtre whitelist
                if allowed_scrape_names and server_scraped not in allowed_scrape_names:
                    continue

                # status
                status = (
                    texts[idx_status]
                    if (idx_status is not None and idx_status < len(texts))
                    else texts[-1]
                )

                # prix Skrill/SEPA
                skrill_sepa_raw = ""
                if idx_skrill_sepa is not None and idx_skrill_sepa < len(texts):
                    skrill_sepa_raw = texts[idx_skrill_sepa]
                else:
                    for c in texts[1:4]:
                        if (
                            "€" in c
                            or "€/m" in c.lower()
                            or any(ch.isdigit() for ch in c)
                        ):
                            skrill_sepa_raw = c
                            break

                m = re.search(r"[-+]?\d+[.,]?\d*", skrill_sepa_raw.replace("\xa0", " "))
                price_val = float(m.group(0).replace(",", ".")) if m else 0.0

                # on garde la clef telle qu’affichée côté site, mais on peut aussi remapper vers le nom “display”
                rows.append(
                    {
                        "server_scraped": server_scraped,
                        "skrill_sepa_raw": skrill_sepa_raw,
                        "price": price_val,
                        "status": status,
                    }
                )
                servers_map[server_scraped] = price_val

            browser.close()
    except Exception as e:
        return {"servers": servers_map, "rows": rows, "error": str(e)}

    return {"servers": servers_map, "rows": rows}


def fetch_reference_prices_async(on_done=None):
    """Scrape en thread, met à jour _revenue_data['servers'] et notifie l'UI."""

    def _job():
        success = False
        count = 0
        err = ""
        try:
            scraped = _scrape_leskamas_with_playwright(
                timeout=15000,
                headless=True,
                allowed_display_names=ALLOWED_SERVERS_DISPLAY,
                display_to_scrape_map=SERVER_SCRAPE_NAME,
            )

            # --- remap des noms scrapés -> noms d'affichage ---
            scraped_map = scraped.get("servers", {}) or {}
            # reverse: "HellMina" -> "Hell Mina", "Ombre(Shadow)" -> "Ombre", ...
            SCRAPED_TO_DISPLAY = {
                _normalize_server(SERVER_SCRAPE_NAME.get(d, d)): d
                for d in ALLOWED_SERVERS_DISPLAY
            }
            servers_map = {}
            for scraped_name, price in scraped_map.items():
                display = SCRAPED_TO_DISPLAY.get(
                    _normalize_server(scraped_name), scraped_name
                )
                servers_map[display] = float(price)

            if servers_map:
                ordered_map = {}
                for name in iter_servers_in_display_order(servers_map.keys()):
                    ordered_map[name] = servers_map.get(name, 0.0)
                servers_map = ordered_map

            if ("error" in scraped) and not servers_map:
                raise Exception(
                    "Playwright scraping error: " + str(scraped.get("error"))
                )

            # stocker les statuts si on en a
            rows = scraped.get("rows", []) or []
            status_by_display = {}
            for r in rows:
                scraped_name = _normalize_server(r.get("server_scraped", ""))
                display = SCRAPED_TO_DISPLAY.get(scraped_name, scraped_name)
                status_by_display[display] = r.get("status", "")

            with _revenue_lock:
                _revenue_data["servers"] = servers_map
                _revenue_data.setdefault("status", {})
                _revenue_data["status"] = status_by_display
                _revenue_data.setdefault("holdings", {"TS": {}, "M": {}})
                for kind in ("TS", "M"):
                    for s in servers_map.keys():
                        _revenue_data["holdings"][kind].setdefault(s, 0)
                # >>> AJOUT : timestamp du dernier scraping
                _revenue_data["last_scrape_ts"] = time.time()

            count = len(servers_map)
            success = count > 0

            # notifier UI pour recalculer les euros
            try:
                bus.revenue_updated.emit()
            except Exception:
                pass

        except Exception as e:
            err = str(e)
            print("Error scraping prices:", err)
        finally:
            # notifier le statut du scraping (OK/KO, nb serveurs, message)
            try:
                bus.prices_fetch_finished.emit(success, count, err)
            except Exception:
                pass
            if on_done:
                try:
                    on_done()
                except Exception:
                    pass

    t = threading.Thread(target=_job, daemon=True)
    t.start()
    return t


def run_server():
    flask_app.run(host=API_HOST, port=API_PORT, threaded=True, use_reloader=False)


# ===================== DARK BLUE THEME (QSS) ======================
def apply_dark_blue_style(app: QApplication):
    app.setStyleSheet(
        """
        QWidget { background-color:#0f172a; color:#e5e7eb; font-family:'Segoe UI','Inter','Roboto',sans-serif; font-size:14px; }
        QLabel#TitleLabel { font-size:22px; font-weight:700; color:#60a5fa; }

        QLabel#CardTitle, QLabel#CardSubtitle, QLabel#ExtraLabel { background-color: transparent; }

        /* Réduction supplémentaire des espaces internes pour diminuer "l'épaisseur" visuelle */
        QGroupBox { background-color:#111827; border:1px solid #1f2937; border-radius:10px; margin-top:4px; padding:4px; }
        QGroupBox::title { subcontrol-origin:margin; subcontrol-position:top left; padding:0 6px; color:#93c5fd; }

        QPushButton { background-color:#2563eb; color:#e5e7eb; border:none; border-radius:10px; padding:6px 10px; font-weight:600; }
        QPushButton:hover { background-color:#3b82f6; }
        QPushButton:pressed { background-color:#1e40af; }

        QSplitter::handle { background:#1f2937; width:6px; margin:4px 0; border-radius:3px; }

        /* Cartes d'instance : légèrement plus claires et plus bleutées que le fond */
        QWidget#InstanceCard {
            background-color: #0b1936;                     /* bleu nuit légèrement éclairci */
            border:1px solid rgba(148,163,184,0.28);       /* bordure gris‑bleuté douce */
            border-radius:12px;
        }
        QWidget#InstanceCard:hover {
            border:1px solid rgba(191,219,254,0.75);
            background-color: #102347;                     /* un ton au‑dessus au survol */
        }
        QWidget#InstanceCard[selected="true"] { border:2px solid #7dd3fc; background-color: rgba(56,189,248,0.20); }

        /* Aura douce et moderne pour les différentes sévérités – légèrement atténuée */
        QWidget#InstanceCard[state="green"] {
            border: 2px solid rgba(74,222,128,0.82);          /* vert menthe clair légèrement adouci */
            background-color: rgba(22,163,74,0.13);           /* halo vert très léger */
        }
        QWidget#InstanceCard[state="green"][selected="true"] {
            border: 2px solid rgba(110,231,183,0.92);
            background-color: rgba(16,185,129,0.21);
        }

        QWidget#InstanceCard[state="yellow"] {
            border: 2px solid rgba(250,204,21,0.82);          /* jaune doré */
            background-color: rgba(202,138,4,0.13);           /* halo jaune très léger */
        }
        QWidget#InstanceCard[state="yellow"][selected="true"] {
            border: 2px solid rgba(252,211,77,0.92);
            background-color: rgba(217,119,6,0.21);
        }

        QWidget#InstanceCard[state="red"] {
            border: 2px solid rgba(248,113,113,0.82);         /* rouge clair */
            background-color: rgba(185,28,28,0.13);           /* halo rouge très léger */
        }
        QWidget#InstanceCard[state="red"][selected="true"] {
            border: 2px solid rgba(248,113,113,0.92);
            background-color: rgba(220,38,38,0.21);
        }

        QWidget#InstanceCard[state="purple"] {
            border: 2px solid rgba(196,181,253,0.82);         /* violet pastel */
            background-color: rgba(109,40,217,0.13);          /* halo violet léger */
        }
        QWidget#InstanceCard[state="purple"][selected="true"] {
            border: 2px solid rgba(167,139,250,0.92);
            background-color: rgba(124,58,237,0.21);
        }

        QWidget#InstanceCard[state="blue"] {
            border: 2px solid rgba(96,165,250,0.82);          /* bleu clair */
            background-color: rgba(30,64,175,0.13);           /* halo bleu léger */
        }
        QWidget#InstanceCard[state="blue"][selected="true"] {
            border: 2px solid rgba(59,130,246,0.92);
            background-color: rgba(37,99,235,0.21);
        }
                      
        /* Carte statique (pas d'effet hover), pour la liste de gauche */
        QWidget#StaticCard { 
            background-color:#0b1936;
            border:1px solid rgba(148,163,184,0.28); 
            border-radius:12px; 
        }

        /* Carte bleue cliquable (look proche d'un QPushButton bleu) */
        QWidget#BlueCard {
            background-color:#2563eb; 
            color:#e5e7eb; 
            border:none; 
            border-radius:12px; 
            padding:0;
        }
        QWidget#BlueCard:hover { background-color:#3b82f6; }
        QWidget#BlueCard:disabled { background-color:#1e3a8a; color:#94a3b8; }      


        QWidget#SubsCard { background-color:#0b1220; border:1px solid #1f2937; border-radius:10px; }

        QLabel#CardTitle { font-weight:700; color:#e5e7eb; padding-bottom:0px; }
        QLabel#CardSubtitle { color:#93c5fd; padding-bottom:0px; }
        QLabel#ExtraLabel { color:#9ca3af; }

        QListWidget { background-color:transparent; border:none; }
        QListWidget::item { margin:1px 2px; padding:0px; background:transparent; } /* Widgets encore plus serrés */
        QListWidget::item:selected { background:rgba(59,130,246,0.18); border-radius:12px; }
                      
        QListWidget::item:focus { outline: none; }
        QListWidget::item:selected:!active { background: transparent; }

        QListWidget#subsList::item { margin:2px 2px; }  /* Marges réduites pour maximiser l'espace horizontal */

        /* ---------- Scrollbar verticale : largeur augmentée + poignée plus large ---------- */
        QScrollBar:vertical {
            width: 20px;                      /* moins épais pour laisser plus de place au contenu */
            margin: 8px 6px 8px 6px;
            background: transparent;
        }

        /* Track légèrement contrasté */
        QScrollBar::add-page:vertical, QScrollBar::sub-page:vertical {
            background: rgba(11,18,32,0.18);
            border-radius: 11px;
        }

        /* Thumb (poignée) : plus grande, coins arrondis */
        QScrollBar::handle:vertical {
            min-height: 48px; /* taille minimale facilitant la prise */
            border-radius: 11px;
            border: 2px solid rgba(255,255,255,0.05);
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                         stop:0 rgba(96,165,250,0.98), stop:1 rgba(37,99,235,0.98));
        }
        QScrollBar::handle:vertical:hover {
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                         stop:0 rgba(120,185,255,1), stop:1 rgba(59,130,246,1));
            border: 2px solid rgba(255,255,255,0.14);
        }
        QScrollBar::handle:vertical:pressed {
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                         stop:0 rgba(59,130,246,1), stop:1 rgba(37,99,235,1));
            border: 2px solid rgba(0,0,0,0.22);
        }

        /* Enlever flèches pour un look moderne */
        QScrollBar::sub-line:vertical, QScrollBar::add-line:vertical {
            height: 0px;
        }

        /* Les pages restent transparentes */
        QScrollBar::add-page:vertical, QScrollBar::sub-page:vertical {
            background: transparent;
        }

        /* ---------------------------------------------------------
           Style minimal ajouté uniquement pour le compteur d'instances
           (QLabel#InstancesBigCount). Rien d'autre n'est modifié.
           --------------------------------------------------------- */
        QLabel#InstancesBigCount {
            font-size: 26px;
            font-weight: 800;
            color: #60a5fa; /* bleu doux cohérent avec le titre */
            padding: 10px 14px;
            border-radius: 10px;
            background: rgba(11,18,32,0.12); /* léger fond pour occuper l'espace */
            border: 1px solid rgba(59,130,246,0.07);
        }
                      
        /* Compteur € total (vert) */
        QLabel#EuroBigCounter {
            border-radius: 14px;
            padding: 12px 18px;
            background: qlineargradient(x1:0, y1:0, x2:1, y2:0,
                        stop:0 rgba(6,78,59,0.98), stop:1 rgba(5,150,105,0.98));
            border: 1px solid rgba(16,185,129,0.35);
            color: #e7fef5;
            font-weight: 900;
            font-size: 28px;
        }
        QLabel#EuroBigCounter:hover {
            border-color: rgba(16,185,129,0.55);
        }
                      
        QSpinBox {
            background: rgba(15,23,42,0.42);
            border: 1px solid rgba(255,255,255,0.12);
            border-radius: 8px;
            padding: 2px 24px 2px 6px;
            min-height: 28px;
        }
        
        QSpinBox::up-button, QSpinBox::down-button {
            background-color: #2563eb;
            border: none;
            width: 20px;
            min-width: 20px;
            subcontrol-origin: border;
            subcontrol-position: right;
        }
        
        QSpinBox::up-button {
            top: 1px;
            bottom: 50%;
            border-top-right-radius: 7px;
            border-bottom-right-radius: 0px;
        }
        
        QSpinBox::down-button {
            top: 50%;
            bottom: 1px;
            border-top-right-radius: 0px;
            border-bottom-right-radius: 7px;
        }
        
        QSpinBox::up-button:hover, QSpinBox::down-button:hover {
            background-color: #3b82f6;
        }
        
        QSpinBox::up-button:pressed, QSpinBox::down-button:pressed {
            background-color: #1e40af;
        }
        
        QSpinBox::up-arrow {
            image: none;
            border-left: 3.5px solid transparent;
            border-right: 3.5px solid transparent;
            border-bottom: 4px solid #ffffff;
            width: 0px;
            height: 0px;
            margin-left: -3.5px;
            margin-top: 2px;
        }
        
        QSpinBox::down-arrow {
            image: none;
            border-left: 3.5px solid transparent;
            border-right: 3.5px solid transparent;
            border-top: 4px solid #ffffff;
            width: 0px;
            height: 0px;
            margin-left: -3.5px;
            margin-bottom: 2px;
        }
                      
        QSpinBox:focus {
            border: 1px solid rgba(59,130,246,0.55);
        }
        
        /* Styles pour les checkboxes - assure la visibilité dans tous les cas */
        QCheckBox {
            spacing: 8px;
            color: #e5e7eb;
        }
        
        QCheckBox::indicator {
            width: 18px;
            height: 18px;
            border: 2px solid rgba(255,255,255,0.4);
            border-radius: 4px;
            background-color: rgba(15,23,42,0.8);
        }
        
        QCheckBox::indicator:hover {
            border: 2px solid rgba(59,130,246,0.9);
            background-color: rgba(37,99,235,0.3);
        }
        
        QCheckBox::indicator:checked {
            background-color: #2563eb;
            border: 2px solid #3b82f6;
            /* La coche sera dessinée par Qt avec une couleur contrastée */
        }
        
        QCheckBox::indicator:checked:hover {
            background-color: #3b82f6;
            border: 2px solid #60a5fa;
        }
        
        QCheckBox::indicator:indeterminate {
            background-color: rgba(59,130,246,0.5);
            border: 2px solid #3b82f6;
        }
                      
    """
    )


# =================== Small re-usable widgets ====================
class StatusDot(QLabel):
    def __init__(self, diameter=10, color=CLR_GREY):
        super().__init__()
        self.d = diameter
        self.setFixedSize(QSize(diameter, diameter))
        self.set_color(color)

    def set_color(self, color_hex: str):
        if not (color_hex or "").strip():
            color_hex = CLR_GREY
        self.setStyleSheet(
            f"background-color:{color_hex}; border-radius:{self.d//2}px;"
        )


class ClickablePanel(QLabel):
    clicked = Signal()

    def __init__(self, text=""):
        super().__init__(text)

    def mousePressEvent(self, e):
        try:
            self.clicked.emit()
        except Exception:
            pass
        return super().mousePressEvent(e)


class StaticPriceRow(QWidget):
    """Ligne immuable 'serveur — montant' (non cliquable), style panel."""

    def __init__(self, server: str, amount_eur: float):
        super().__init__()
        self.setObjectName("StaticCard")
        self.setAttribute(Qt.WA_StyledBackground, True)
        self.setMinimumHeight(30)

        lay = QHBoxLayout(self)
        lay.setContentsMargins(10, 6, 10, 6)
        lay.setSpacing(8)

        self.lbl_server = QLabel(server)
        self.lbl_server.setStyleSheet("font-weight:600;")
        self.lbl_amount = QLabel(f"{amount_eur:.3f} €")
        self.lbl_amount.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        self.lbl_amount.setStyleSheet("font-weight:700;")

        lay.addWidget(self.lbl_server, 1)
        lay.addWidget(self.lbl_amount, 0)

    def setAmount(self, amount_eur: float):
        self.lbl_amount.setText(f"{amount_eur:.3f} €")

    # >>> AJOUT : couleur du montant selon status
    def setAmountColor(self, color_hex: str | None):
        base = "font-weight:700;"
        if color_hex:
            self.lbl_amount.setStyleSheet(f"{base} color:{color_hex};")
        else:
            self.lbl_amount.setStyleSheet(base)


class BlueClickableCard(QWidget):
    """Carte bleue cliquable (titre à gauche, montant à droite)."""

    clicked = Signal()

    def __init__(self, title: str = "", amount_eur: float = 0.0):
        super().__init__()
        self.setObjectName("BlueCard")
        self.setAttribute(Qt.WA_StyledBackground, True)
        self.setMinimumHeight(56)
        self.setCursor(Qt.PointingHandCursor)

        lay = QHBoxLayout(self)
        lay.setContentsMargins(12, 10, 12, 10)
        lay.setSpacing(8)

        self.lbl_title = QLabel(title)
        self.lbl_title.setStyleSheet("font-weight:700;")
        self.lbl_amount = QLabel(f"{amount_eur:.2f} €")
        self.lbl_amount.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        self.lbl_amount.setStyleSheet("font-weight:800;")

        self.setAutoFillBackground(True)  # force la peinture du fond
        self.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)

        self.lbl_title.setStyleSheet("font-weight:700; background: transparent;")
        self.lbl_amount.setStyleSheet("font-weight:800; background: transparent;")

        lay.addWidget(self.lbl_title, 1)
        lay.addWidget(self.lbl_amount, 0)

    def mousePressEvent(self, e):
        try:
            self.clicked.emit()
        except Exception:
            pass
        return super().mousePressEvent(e)

    def setTitle(self, title: str):
        self.lbl_title.setText(title)

    def setAmount(self, amount_eur: float):
        self.lbl_amount.setText(f"{amount_eur:.2f} €")


class NoFocusDelegate(QStyledItemDelegate):
    def paint(self, painter: QPainter, option, index):
        # Retire l'état "HasFocus" pour empêcher le rectangle pointillé
        if option.state & QStyle.State_HasFocus:
            option.state &= ~QStyle.State_HasFocus
        super().paint(painter, option, index)


class RevenueKindRow(QWidget):
    """Une ligne : Serveur | Kamas (M) | Valeur € — compacte, bordurée."""

    valueChanged = Signal(str, int)  # (server, new_kamas)

    def __init__(self, server: str, price_per_million: float, kamas_m: int):
        super().__init__()
        self.server = server
        self.price = float(price_per_million)

        self.setObjectName("StaticCard")
        self.setAttribute(Qt.WA_StyledBackground, True)
        self.setMinimumHeight(28)

        # --- colonnes fixes : KAMAS (milieu) = 60px, EUROS (droite) = 110px ---
        KAMAS_COL_W = 60
        EUROS_COL_W = 110

        # widgets
        self.lbl_server = QLabel(server)
        self.lbl_server.setStyleSheet("font-weight:600;")

        # Champ texte numérique-only, centré, sans menu contextuel si tu veux
        self.edit_kamas = QLineEdit(str(int(kamas_m)))
        self.edit_kamas.setValidator(QIntValidator(0, 1_000_000, self))
        self.edit_kamas.setFixedWidth(60)
        self.edit_kamas.setAlignment(Qt.AlignCenter)
        # Select-all dès qu'on focus (après que Qt ait placé le caret)
        self.edit_kamas.installEventFilter(self)
        # Enter = valider le champ (ne ferme pas la fenêtre)
        self.edit_kamas.returnPressed.connect(self._commit_kamas)
        self.edit_kamas.editingFinished.connect(
            self._commit_kamas
        )  # <-- AJOUT : focus perdu = commit
        # Mise à jour en live de la colonne €
        self.edit_kamas.textChanged.connect(self._on_text_changed)

        self.lbl_value = QLabel(self._fmt_value(self._current_kamas()))
        self.lbl_value.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        self.lbl_value.setStyleSheet("font-weight:700;")

        # layout
        lay = QHBoxLayout(self)
        lay.setContentsMargins(8, 4, 8, 4)
        lay.setSpacing(8)

        # Colonne Kamas AU MILIEU : conteneur largeur fixe, champ centré dedans
        km_holder = QWidget()
        km_holder.setFixedWidth(KAMAS_COL_W)
        km_lay = QHBoxLayout(km_holder)
        km_lay.setContentsMargins(0, 0, 0, 0)
        km_lay.setSpacing(0)
        km_lay.addWidget(self.edit_kamas, 1, Qt.AlignCenter)

        # Colonne Euros à droite : largeur fixe, valeur alignée à droite
        eur_holder = QWidget()
        eur_holder.setFixedWidth(EUROS_COL_W)
        eur_lay = QHBoxLayout(eur_holder)
        eur_lay.setContentsMargins(0, 0, 0, 0)
        eur_lay.setSpacing(0)
        eur_lay.addWidget(self.lbl_value, 1, Qt.AlignRight | Qt.AlignVCenter)

        # Placement des 3 colonnes
        lay.addWidget(self.lbl_server, 1)  # s'étire
        lay.addWidget(km_holder, 0)  # milieu fixe
        lay.addWidget(eur_holder, 0)  # droite fixe

    # --- helpers ---
    def _current_kamas(self) -> int:
        t = self.edit_kamas.text().strip()
        try:
            return int(t) if t else 0
        except ValueError:
            return 0

    def _fmt_value(self, kamas_m: int) -> str:
        return f"{kamas_m * self.price:.2f} €"

    def _on_text_changed(self, _):
        v = self._current_kamas()
        self.lbl_value.setText(self._fmt_value(v))

    def _commit_kamas(self):
        v = self._current_kamas()
        # En cas de saisie vide/non numérique, on normalise l'affichage
        self.edit_kamas.setText(str(v))
        self.valueChanged.emit(self.server, v)
        # Enlève le focus visuel après validation
        self.edit_kamas.clearFocus()

    # Select-All à la prise de focus
    def eventFilter(self, obj, ev):
        if obj is self.edit_kamas and ev.type() == QEvent.FocusIn:
            QTimer.singleShot(0, self.edit_kamas.selectAll)
        return super().eventFilter(obj, ev)

        # --- MAJ depuis l'extérieur (reception d'un update) ---

    def setKamas(self, kamas_m: int, do_flash: bool = True):
        """Met à jour la valeur affichée (champ Kamas + €)."""
        kamas_m = int(kamas_m)
        if self._current_kamas() == kamas_m:
            return
        # MAJ champ et colonne €
        self.edit_kamas.setText(str(kamas_m))
        self.lbl_value.setText(self._fmt_value(kamas_m))
        if do_flash:
            self.flash()

    def flash(self, duration_ms: int = 1000):
        """Petit flash blanc (1s) sur la ligne (type 'bombe flash')."""
        overlay = QWidget(self)
        overlay.setAttribute(Qt.WA_TransparentForMouseEvents, True)
        overlay.setGeometry(self.rect())
        overlay.setStyleSheet("background:#ffffff; border-radius:8px;")
        effect = QGraphicsOpacityEffect(overlay)
        overlay.setGraphicsEffect(effect)
        overlay.show()

        anim = QPropertyAnimation(effect, b"opacity", overlay)
        anim.setDuration(duration_ms)
        anim.setStartValue(0.9)
        anim.setEndValue(0.0)
        anim.setEasingCurve(QEasingCurve.OutCubic)

        # Nettoyage de l'overlay en fin d'anim
        def _cleanup():
            overlay.hide()
            overlay.deleteLater()

        anim.finished.connect(_cleanup)
        anim.start(QPropertyAnimation.DeleteWhenStopped)


class RevenueKindDialog(QDialog):
    """Éditeur des comptes TS / Métier (une ligne par serveur)."""

    def __init__(self, parent, kind: str, servers_ref: dict, holdings: dict):
        super().__init__(parent)
        self.setWindowTitle(f"Détail {kind}")
        self.setMinimumSize(420, 540)
        self.kind = kind
        self.servers_ref = servers_ref  # {"Imagiro":{"TS":..,"M":..}, ...}
        self.holdings = holdings  # {"Imagiro": 152, ...} en millions

        # mêmes largeurs que dans la row
        KAMAS_COL_W = 60
        EUROS_COL_W = 110

        root = QVBoxLayout(self)
        root.setContentsMargins(8, 8, 8, 8)
        root.setSpacing(6)

        # ---------- Panel "Détail" ----------
        grp = QGroupBox("Détail")
        grp_v = QVBoxLayout(grp)
        grp_v.setContentsMargins(8, 8, 8, 8)
        grp_v.setSpacing(6)

        # En-tête : Serveur | Kamas | Euros
        header = QWidget()
        header.setObjectName("StaticCard")
        header.setAttribute(Qt.WA_StyledBackground, True)
        h = QHBoxLayout(header)
        h.setContentsMargins(8, 6, 8, 6)
        h.setSpacing(8)

        lbl_srv = QLabel("Serveur")
        lbl_srv.setStyleSheet("font-weight:700;")

        lbl_km = QLabel("Kamas")
        lbl_km.setStyleSheet("font-weight:700;")
        lbl_km.setAlignment(Qt.AlignCenter)
        km_head_holder = QWidget()
        km_head_holder.setFixedWidth(KAMAS_COL_W)
        km_head_lay = QHBoxLayout(km_head_holder)
        km_head_lay.setContentsMargins(0, 0, 0, 0)
        km_head_lay.setSpacing(0)
        km_head_lay.addWidget(lbl_km, 1, Qt.AlignCenter)

        lbl_eur = QLabel("Euros")
        lbl_eur.setStyleSheet("font-weight:700;")
        lbl_eur.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        eur_head_holder = QWidget()
        eur_head_holder.setFixedWidth(EUROS_COL_W)
        eur_head_lay = QHBoxLayout(eur_head_holder)
        eur_head_lay.setContentsMargins(0, 0, 0, 0)
        eur_head_lay.setSpacing(0)
        eur_head_lay.addWidget(lbl_eur, 1, Qt.AlignRight | Qt.AlignVCenter)

        h.addWidget(lbl_srv, 1)
        h.addWidget(km_head_holder, 0)
        h.addWidget(eur_head_holder, 0)
        grp_v.addWidget(header, 0)

        # Liste
        self.list = QListWidget()
        self.list.setSelectionMode(QAbstractItemView.NoSelection)
        self.list.setFrameShape(QListWidget.NoFrame)
        self.list.setFocusPolicy(
            Qt.ClickFocus
        )  # <-- AJOUT : la liste peut prendre le focus au clic
        self.list.viewport().installEventFilter(self)
        self.list.setItemDelegate(NoFocusDelegate(self.list))
        self.list.setSpacing(0)
        grp_v.addWidget(self.list, 1)

        root.addWidget(grp, 1)

        self._rows_by_server = {}

        # Remplir
        for srv in iter_servers_in_display_order(self.servers_ref.keys()):
            ref = self.servers_ref.get(srv)
            price = float(ref) if isinstance(ref, (int, float, str)) else 0.0
            kamas = int(self.holdings.get(srv, 0))
            item = QListWidgetItem()
            item.setSizeHint(QSize(10, 30))
            row = RevenueKindRow(srv, price, kamas)
            row.valueChanged.connect(self._on_row_changed)
            self.list.addItem(item)
            self.list.setItemWidget(item, row)
            self._rows_by_server[srv] = row

        # Bouton fermer (NON défaut)
        btn_close = QPushButton("Fermer")
        btn_close.setAutoDefault(False)
        btn_close.setDefault(False)
        btn_close.clicked.connect(self.accept)
        btn_close.setFixedHeight(32)
        root.addWidget(btn_close, 0, Qt.AlignRight)

        # Pas de focus initial sur la 1ère ligne
        self.setFocusPolicy(Qt.NoFocus)
        self.list.setCurrentRow(-1)

        # Écoute les mises à jour globales des revenus
        try:
            bus.revenue_updated.connect(self._on_bus_revenue_updated)
        except Exception:
            pass

    def _on_row_value_changed(self, server: str, v: int):
        """Reçoit les changements d'une ligne (RevenueKindRow). Met à jour les holdings + persiste + notifie."""
        try:
            v = int(v)
        except Exception:
            v = 0

        changed = False
        with _revenue_lock:
            _revenue_data.setdefault("holdings", {"TS": {}, "Metier": {}})
            _revenue_data["holdings"].setdefault(self.kind, {})
            prev = _revenue_data["holdings"][self.kind].get(server)
            if prev != v:
                _revenue_data["holdings"][self.kind][server] = v
                _revenue_data["last_player_update_ts"] = time.time()
                changed = True

        if changed:
            # 1) Sauvegarde sur disque
            try:
                _persist_holdings_to_disk()
            except Exception:
                pass

            # 2) Notifie le reste de l’app (cartes TS/Métier, etc.)
            try:
                bus.revenue_updated.emit()
            except Exception:
                pass

            # 3) (optionnel) petit flash local sur la ligne éditée
            try:
                row = self._rows_by_server.get(server)
                if row:
                    row.flash(800)
            except Exception:
                pass

    def _on_bus_revenue_updated(self):
        """Quand un update arrive, on recharge les holdings et on met à jour les lignes avec un flash."""
        try:
            with _revenue_lock:
                # holdings de ce kind uniquement
                holdings_all = _revenue_data.get("holdings", {})
                holdings_kind = (
                    holdings_all.get(self.kind, {})
                    if isinstance(holdings_all, dict)
                    else {}
                )
        except Exception:
            holdings_kind = {}

        # Applique sur les lignes affichées
        for srv, row in self._rows_by_server.items():
            try:
                new_kamas = int(holdings_kind.get(srv, 0))
                row.setKamas(new_kamas, do_flash=True)
            except Exception:
                continue

    def closeEvent(self, e):
        try:
            bus.revenue_updated.disconnect(self._on_bus_revenue_updated)
        except Exception:
            pass
        return super().closeEvent(e)

    def _on_row_changed(self, server: str, new_kamas: int):
        # Persiste la valeur
        self.holdings[server] = new_kamas
        # Notifie toute l’UI (dialog + compteur principal) qu'il faut recalculer
        try:
            bus.revenue_updated.emit()
        except Exception:
            pass

    def showEvent(self, e):
        super().showEvent(e)
        # Enlève tout focus automatique que Qt pourrait mettre
        QTimer.singleShot(0, lambda: self.setFocus(Qt.FocusReason.NoFocusReason))

    def eventFilter(self, obj, ev):
        # Si on clique dans le "fond" de la liste, on lui donne le focus
        if obj is self.list.viewport() and ev.type() == QEvent.MouseButtonPress:
            self.list.setFocus()
        return super().eventFilter(obj, ev)

    def mousePressEvent(self, e):
        # Tout clic sur le fond du dialog enlève le focus à l’éditeur courant
        self.setFocus()
        super().mousePressEvent(e)


class RevenueDialog(QDialog):
    def __init__(self, parent, revenue_data: dict):
        super().__init__(parent)
        self.setWindowTitle("€ générés - Détails")
        self.setMinimumSize(520, 400)

        root = QHBoxLayout(self)
        root.setContentsMargins(8, 8, 8, 8)
        root.setSpacing(8)

        # --------- GAUCHE : Panel "Prix de référence" avec une belle liste -----------
        # --------- GAUCHE : Panel "Prix de référence" avec liste + bouton Refresh -----------
        left_group = QGroupBox("Prix de référence")
        left_v = QVBoxLayout(left_group)
        left_v.setContentsMargins(8, 8, 8, 8)
        left_v.setSpacing(8)

        self.list = QListWidget()
        self.list.setSelectionMode(QAbstractItemView.NoSelection)
        self.list.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOff)  # pas de scrollbar
        self.list.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        # self.list.setSpacing(2)                                      # espacement entre lignes
        self.list.setSizePolicy(
            QSizePolicy.Expanding, QSizePolicy.Fixed
        )  # ne s’étire pas en hauteur

        self.list.setFrameShape(QListWidget.NoFrame)
        self.list.setFocusPolicy(Qt.NoFocus)
        self.list.setItemDelegate(NoFocusDelegate(self.list))

        left_v.addWidget(self.list, 0)  # 0 = n'étire pas, on contrôle la hauteur
        # --- Bouton Refresh ---
        self.btn_refresh_prices = QPushButton("Refresh")
        self.btn_refresh_prices.setCursor(Qt.PointingHandCursor)
        self.btn_refresh_prices.setFixedHeight(24)
        self.btn_refresh_prices.clicked.connect(
            self.on_refresh_prices
        )  # handler vide pour l'instant

        left_v.addWidget(self.btn_refresh_prices, 0)
        # Label d’état du dernier scraping
        self.lbl_fetch_status = QLabel("")
        self.lbl_fetch_status.setStyleSheet("color:#9aa4b2; font-size:12px;")
        left_v.addWidget(self.lbl_fetch_status, 0)

        # Affiche la dernière MAJ si elle existe
        try:
            with _revenue_lock:
                ts0 = _revenue_data.get("last_scrape_ts", 0)
            when0 = (
                datetime.fromtimestamp(ts0).strftime("%d/%m/%Y %H:%M") if ts0 else "-"
            )
        except Exception:
            when0 = "-"
        self._set_fetch_status(f"Last update : {when0}", "neutral")

        root.addWidget(left_group, 1)

        # --------- DROITE : Panel "Revenus" avec deux widgets bleus cliquables -------
        right_group = QGroupBox("Revenus")
        right_v = QVBoxLayout(right_group)
        right_v.setContentsMargins(8, 24, 8, 8)
        right_v.setSpacing(10)

        self.card_ts = BlueClickableCard("TS", 0.0)
        self.card_metier = BlueClickableCard("Métier", 0.0)

        right_v.addWidget(self.card_ts)
        right_v.addWidget(self.card_metier)
        right_v.addStretch(1)

        root.addWidget(right_group, 1)

        bus.prices_fetch_finished.connect(self._on_prices_fetch_finished)
        bus.revenue_updated.connect(self._on_bus_revenue_updated)

        # ----- Données & remplissage -----
        self.data = revenue_data or {}
        servers = self.data.get("servers") or {}
        server_order = list(iter_servers_in_display_order(servers.keys()))
        if not server_order:
            server_order = list(SERVER_KAMAS_DISPLAY_ORDER)

        # --- holdings: millions de kamas par serveur et par type ---
        # structure: self.data["holdings"] = {"TS":{srv:int}, "M":{srv:int}}
        holds = self.data.setdefault("holdings", {"TS": {}, "M": {}})
        servers_all = list(
            iter_servers_in_display_order((self.data.get("servers") or {}).keys())
        )
        for kind in ("TS", "M"):
            for srv in servers_all:
                holds[kind].setdefault(srv, 0)

        # Liste serveurs -> cartes statiques, taille calculée pour tout voir sans scroll
        self._rows = []
        ITEM_H = 28  # ta valeur actuelle
        for name in server_order:
            price = float(
                (servers or {}).get(name, 0.0)
            )  # <--- ICI: servers[name] est un float
            it = QListWidgetItem()
            it.setSizeHint(QSize(10, ITEM_H))
            row = StaticPriceRow(name, price)  # affiche "19.50 €" etc.
            self.list.addItem(it)
            self.list.setItemWidget(it, row)
            self._rows.append((it, row))

        # Ajuste la hauteur de la liste pour afficher les 13 items sans scroll
        self._fit_list_height(item_height=ITEM_H, count=len(server_order))

        self._apply_status_colors()

        # Totaux
        self._recompute_totals()

        # Click -> détail
        self.card_ts.clicked.connect(lambda: self._open_kind_editor("TS"))
        self.card_metier.clicked.connect(lambda: self._open_kind_editor("M"))

    def closeEvent(self, e):
        try:
            bus.revenue_updated.disconnect(self._on_bus_revenue_updated)
        except Exception:
            pass
        return super().closeEvent(e)

    def _apply_status_colors(self):
        try:
            with _revenue_lock:
                status_map = _revenue_data.get("status", {}) or {}
            for i in range(self.list.count()):
                it = self.list.item(i)
                row = self.list.itemWidget(it)
                if not row:
                    continue
                name = row.lbl_server.text()
                st = (status_map.get(name, "") or "").strip().lower()
                # Incomplet -> vert ; Stock complet -> rouge ; sinon neutre
                if "stock complet" in st:
                    row.setAmountColor("#ef4444")  # rouge
                elif "incomplet" in st:
                    row.setAmountColor("#10b981")  # vert
                else:
                    row.setAmountColor(None)  # style par défaut
        except Exception:
            pass

    def _on_bus_revenue_updated(self):
        """Recalcule les totaux TS/Métier dans ce dialog quand holdings/prix changent."""
        try:
            data = self.data  # alias local
            servers = data.get("servers") or {}
            holds = data.get("holdings") or {"TS": {}, "M": {}}

            total_ts = 0.0
            total_metier = 0.0
            for srv, price in servers.items():
                p = float(price)
                total_ts += int(holds.get("TS", {}).get(srv, 0)) * p
                total_metier += int(holds.get("M", {}).get(srv, 0)) * p

            self._apply_status_colors()

            try:
                servers = self.data.get("servers") or {}
                for i in range(self.list.count()):
                    it = self.list.item(i)
                    row = self.list.itemWidget(it)
                    if not row:
                        continue
                    name = row.lbl_server.text()
                    price = float(servers.get(name, 0.0))
                    row.setAmount(price)
            except Exception:
                pass

            self.card_ts.setAmount(total_ts)
            self.card_metier.setAmount(total_metier)
        except Exception:
            pass

            # MAJ des montants affichés à gauche

    # def closeEvent(self, e):
    #     try:
    #         bus.revenue_updated.disconnect(self._on_bus_revenue_updated)
    #     except Exception:
    #         pass
    #     super().closeEvent(e)

    def _fit_list_height(self, item_height: int, count: int):
        """Calcule la hauteur exacte pour afficher 'count' items sans scroll."""
        try:
            spacing = self.list.spacing() if hasattr(self.list, "spacing") else 6
            total = (
                count * item_height + max(0, count - 1) * spacing + 4
            )  # +4 marge douce
            self.list.setFixedHeight(total)
        except Exception:
            pass

    def on_refresh_prices(self):
        """Bouton 'Refresh les prix' -> lance le fetch asynchrone et actualise l'UI."""
        try:
            # Bloque le bouton + change son texte
            self.btn_refresh_prices.setEnabled(False)
            self.btn_refresh_prices.setText("Refresh en cours …")
            # Affiche un statut neutre pendant le fetch
            self._set_fetch_status("Refresh en cours …", "neutral")

            def _done():
                # Re-active le bouton et remet le texte dans le thread UI
                QTimer.singleShot(
                    0,
                    lambda: (
                        self.btn_refresh_prices.setEnabled(True),
                        self.btn_refresh_prices.setText("Refresh"),
                    ),
                )

            fetch_reference_prices_async(on_done=_done)
        except Exception:
            self.btn_refresh_prices.setEnabled(True)
            self.btn_refresh_prices.setText("Refresh")

    def _recompute_totals(self):
        """Total € = somme_s( holdings[kind][s] * price[s] ), avec price[s] = €/M unique."""
        servers = self.data.get("servers") or {}  # {"Mikhal": 12.5, ...}
        holds = self.data.get("holdings") or {"TS": {}, "M": {}}

        total_ts = 0.0
        total_metier = 0.0
        for srv in iter_servers_in_display_order(servers.keys()):
            p = float(servers.get(srv, 0.0))
            total_ts += int(holds.get("TS", {}).get(srv, 0)) * p
            total_metier += int(holds.get("M", {}).get(srv, 0)) * p

        self.card_ts.setAmount(total_ts)
        self.card_metier.setAmount(total_metier)

    def _open_kind_editor(self, kind: str):
        servers_ref = self.data.get("servers") or {}  # dict[str,float]
        holds = (self.data.get("holdings") or {}).setdefault(kind, {})
        for srv in iter_servers_in_display_order(servers_ref.keys()):
            holds.setdefault(srv, 0)

        dlg = RevenueKindDialog(self, kind, servers_ref, holds)  # <-- ok: float
        dlg.exec()
        self._recompute_totals()

    def _set_fetch_status(self, text: str, state: str):
        """state in {'ok','err','neutral'}"""
        color = {"ok": "#22c55e", "err": "#f97316", "neutral": "#9aa4b2"}.get(
            state, "#9aa4b2"
        )
        self.lbl_fetch_status.setText(text)
        self.lbl_fetch_status.setStyleSheet(f"color:{color}; font-size:12px;")

    def _on_prices_fetch_finished(self, success: bool, count: int, err: str):
        # Exécutons la MAJ dans le thread UI
        def _apply():

            # Toujours remettre le bouton dans un état normal à la fin du fetch
            try:
                self.btn_refresh_prices.setEnabled(True)
                self.btn_refresh_prices.setText("Refresh")
            except Exception:
                pass

            if success:
                # 1) Récupérer ts et servers une seule fois (thread-safe)
                try:
                    with _revenue_lock:
                        ts = _revenue_data.get("last_scrape_ts", 0)
                        servers = dict(_revenue_data.get("servers", {}))
                except Exception:
                    ts = 0
                    servers = {}

                # 2) Formatter la date/heure
                try:
                    when = (
                        datetime.fromtimestamp(ts).strftime("%d/%m/%Y %H:%M")
                        if ts
                        else "-"
                    )
                except Exception:
                    when = "-"

                # 3) Afficher le label AVEC la date/heure ✅
                self._set_fetch_status(f"Last update : {when}", "neutral")

                # 4) Mettre à jour les montants affichés
                try:
                    for i in range(self.list.count()):
                        it = self.list.item(i)
                        row = self.list.itemWidget(it)
                        if not row:
                            continue
                        name = row.lbl_server.text()
                        price = float(servers.get(name, 0.0))
                        row.setAmount(price)
                    # 5) Appliquer les couleurs UNE SEULE FOIS (après la boucle)
                    self._apply_status_colors()
                except Exception:
                    pass
            else:
                self._set_fetch_status(f"Échec du scraping : {err or 'inconnu'}", "err")

            # 6) Recalcule les totaux TS/Métier
            try:
                self._recompute_totals()
            except Exception:
                pass

        QTimer.singleShot(0, _apply)

    def closeEvent(self, e):
        try:
            bus.prices_fetch_finished.disconnect(self._on_prices_fetch_finished)
        except Exception:
            pass
        # si tu avais connecté bus.revenue_updated → déconnecte-le ici aussi
        try:
            bus.revenue_updated.disconnect(self._on_bus_revenue_updated)
        except Exception:
            pass
        return super().closeEvent(e)


class CustomButtonDialog(QDialog):
    """Dialogue pour ajouter ou modifier un bouton programmable."""

    def __init__(
        self,
        parent=None,
        title="",
        close_keywords="",
        open_keywords="",
        is_edit=False,
        button_id=None,
    ):
        super().__init__(parent)
        self.button_id = button_id
        self.is_edit = is_edit
        self.setWindowTitle("Modifier un bouton" if is_edit else "Ajouter un bouton")
        self.setMinimumWidth(500)

        layout = QVBoxLayout(self)
        layout.setSpacing(12)
        layout.setContentsMargins(16, 16, 16, 16)

        # Titre
        title_label = QLabel("Titre du bouton :")
        self.title_edit = QLineEdit()
        # self.title_edit.setPlaceholderText("Ex: Vidage rapide (peut être vide)")
        self.title_edit.setText(title)
        self.title_edit.setMaxLength(50)

        # Fermer
        close_label = QLabel("Fermer :")
        self.close_edit = QLineEdit()
        # self.close_edit.setPlaceholderText(
        #     "Ex: Metiers,Halouf (mots-clés séparés par des virgules)"
        # )
        self.close_edit.setText(close_keywords)

        # Lancer
        open_label = QLabel("Lancer :")
        self.open_edit = QLineEdit()
        # self.open_edit.setPlaceholderText(
        #     "Ex: Vidage (mots-clés séparés par des virgules)"
        # )
        self.open_edit.setText(open_keywords)

        # Boutons
        btn_layout = QHBoxLayout()
        if is_edit:
            btn_delete = QPushButton("🗑️ Supprimer")
            btn_delete.setStyleSheet(
                "QPushButton { background-color: #ef4444; } QPushButton:hover { background-color: #dc2626; }"
            )
            btn_delete.clicked.connect(self.on_delete)
            btn_layout.addWidget(btn_delete)
        btn_layout.addStretch()
        btn_ok = QPushButton("OK")
        btn_cancel = QPushButton("Annuler")
        btn_ok.clicked.connect(self.accept)
        btn_cancel.clicked.connect(self.reject)
        btn_layout.addWidget(btn_ok)
        btn_layout.addWidget(btn_cancel)

        layout.addWidget(title_label)
        layout.addWidget(self.title_edit)
        layout.addWidget(close_label)
        layout.addWidget(self.close_edit)
        layout.addWidget(open_label)
        layout.addWidget(self.open_edit)
        layout.addLayout(btn_layout)

        # Focus sur le titre
        self.title_edit.setFocus()

    def on_delete(self):
        """Supprime le bouton."""
        self.delete_requested = True
        self.reject()

    def get_title(self) -> str:
        return self.title_edit.text().strip()

    def get_close_keywords(self) -> str:
        return self.close_edit.text().strip()

    def get_open_keywords(self) -> str:
        return self.open_edit.text().strip()

    def get_code(self) -> str:
        """Génère le code à partir des champs séparés."""
        lines = []
        close = self.get_close_keywords()
        open_kw = self.get_open_keywords()
        if close:
            lines.append(f"close : {close}")
        if open_kw:
            lines.append(f"open : {open_kw}")
        return "\n".join(lines)


class CollapsibleGroupBox(QWidget):
    """QGroupBox réductible avec bouton collapse/expand."""

    toggle = Signal()  # Signal émis lors du toggle

    def __init__(self, title: str, parent=None):
        super().__init__(parent)
        self._expanded = True

        # Définir un objectName pour le style CSS
        self.setObjectName("CollapsibleGroupBox")
        # Forcer le rendu du background
        self.setAttribute(Qt.WA_StyledBackground, True)

        # Définir le fond et la bordure directement via la palette
        from PySide6.QtGui import QPalette, QColor

        palette = self.palette()
        palette.setColor(self.backgroundRole(), QColor("#1e293b"))
        self.setPalette(palette)
        self.setAutoFillBackground(True)

        # Style du widget principal avec cadre harmonisé et fond bleu sobre
        # + style pour rendre transparents les widgets enfants
        self.setStyleSheet(
            "QWidget#CollapsibleGroupBox { "
            "background-color: #1e293b; "
            "border: 1px solid #334155; "
            "border-radius: 10px; "
            "} "
            "QWidget#CollapsibleGroupBox QCheckBox { "
            "background-color: transparent; "
            "spacing: 8px; "
            "color: #e5e7eb; "
            "} "
            "QWidget#CollapsibleGroupBox QCheckBox::indicator { "
            "width: 18px; "
            "height: 18px; "
            "border: 2px solid rgba(255,255,255,0.4); "
            "border-radius: 4px; "
            "background-color: rgba(15,23,42,0.8); "
            "} "
            "QWidget#CollapsibleGroupBox QCheckBox::indicator:hover { "
            "border: 2px solid rgba(59,130,246,0.9); "
            "background-color: rgba(37,99,235,0.3); "
            "} "
            "QWidget#CollapsibleGroupBox QCheckBox::indicator:checked { "
            "background-color: #2563eb; "
            "border: 2px solid #3b82f6; "
            "} "
            "QWidget#CollapsibleGroupBox QCheckBox::indicator:checked:hover { "
            "background-color: #3b82f6; "
            "border: 2px solid #60a5fa; "
            "} "
            "QWidget#CollapsibleGroupBox QPushButton { "
            "background-color: #2563eb; "
            "border: none; "
            "border-radius: 8px; "
            "padding: 6px 12px; "
            "font-weight: 600; "
            "} "
            "QWidget#CollapsibleGroupBox QPushButton:hover { "
            "background-color: #3b82f6; "
            "} "
            "QWidget#CollapsibleGroupBox QPushButton:pressed { "
            "background-color: #1e40af; "
            "} "
            "QWidget#CollapsibleGroupBox QLabel { "
            "background-color: transparent; "
            "} "
            "QWidget#CollapsibleGroupBox QSpinBox { "
            "background-color: transparent; "
            "border: 1px solid rgba(255,255,255,0.2); "
            "}"
        )

        # Layout principal
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(0)

        # Header avec titre et bouton (cliquable)
        self._header = QWidget()
        self._header.setFixedHeight(36)
        self._header.setCursor(Qt.PointingHandCursor)
        self._header.setStyleSheet(
            "QWidget { background-color:transparent; border:none; }"
        )
        # Rendre le header cliquable
        self._header.mousePressEvent = self._on_header_clicked

        header_layout = QHBoxLayout(self._header)
        header_layout.setContentsMargins(12, 8, 12, 8)
        header_layout.setSpacing(8)

        self._toggle_btn = QPushButton("▼")
        self._toggle_btn.setFixedSize(22, 22)
        self._toggle_btn.setCursor(Qt.PointingHandCursor)
        self._toggle_btn.setStyleSheet(
            "QPushButton { "
            "border:none; "
            "background:transparent; "
            "font-size:14px; "
            "color:#93c5fd; "
            "font-weight:bold; "
            "padding:0px; "
            "text-align:center; "
            "} "
            "QPushButton:hover { color:#60a5fa; }"
        )
        self._toggle_btn.clicked.connect(self._on_toggle_clicked)

        self._title_label = QLabel(title)
        self._title_label.setStyleSheet(
            "QLabel { "
            "font-weight:600; "
            "color:#93c5fd; "
            "background:transparent; "
            "}"
        )
        self._title_label.setCursor(Qt.PointingHandCursor)
        # Rendre le label cliquable aussi
        self._title_label.mousePressEvent = self._on_header_clicked

        header_layout.addWidget(self._toggle_btn)
        header_layout.addWidget(self._title_label)
        header_layout.addStretch()

        main_layout.addWidget(self._header)

        # QGroupBox pour le contenu (style cohérent avec l'application)
        self._group_box = QGroupBox()
        self._group_box.setStyleSheet(
            "QGroupBox { "
            "background-color:transparent; "
            "border:none; "
            "border-radius:0px; "
            "margin-top:0px; "
            "padding:0px; "
            "}"
        )
        self._content_layout = QVBoxLayout(self._group_box)
        self._content_layout.setContentsMargins(10, 4, 10, 10)
        self._content_layout.setSpacing(8)

        main_layout.addWidget(self._group_box)

    def _on_header_clicked(self, event):
        """Gère le clic sur le header (titre ou zone vide)."""
        self._on_toggle_clicked()

    def _on_toggle_clicked(self):
        """Gère le clic sur le bouton toggle."""
        self.toggle_state()
        self.toggle.emit()

    def toggle_state(self):
        """Bascule l'état expanded/collapsed."""
        self._expanded = not self._expanded
        self._group_box.setVisible(self._expanded)
        self._toggle_btn.setText("▼" if self._expanded else "▶")

    def setExpanded(self, expanded: bool):
        """Définit l'état expanded/collapsed."""
        if self._expanded != expanded:
            self.toggle_state()

    def isExpanded(self) -> bool:
        """Retourne True si le groupe est étendu."""
        return self._expanded

    def addWidget(self, widget: QWidget):
        """Ajoute un widget au contenu."""
        self._content_layout.addWidget(widget)

    def addLayout(self, layout: QVBoxLayout | QHBoxLayout | QFormLayout | QGridLayout):
        """Ajoute un layout au contenu."""
        self._content_layout.addLayout(layout)

    def addStretch(self, stretch: int = 0):
        """Ajoute un stretch au contenu."""
        self._content_layout.addStretch(stretch)


class InstanceItemWidget(QWidget):
    requestFocus = Signal(str)
    requestRelaunch = Signal(str)
    requestKill = Signal(str)
    requestDelete = Signal(str)

    def __init__(self, title: str):
        super().__init__()
        self.title_id = title
        self.setObjectName("InstanceCard")
        self.setAttribute(Qt.WA_StyledBackground, True)

        # Aura / glow pour les instances actives / en warning / en erreur / en recovery / manuelles
        self._active = False
        self._glow_mode = "none"  # "green", "yellow", "red", "purple", "blue" ou "none"
        try:
            self._glow_effect = QGraphicsDropShadowEffect(self)
            self._glow_effect.setOffset(0, 0)
            self._glow_effect.setBlurRadius(0)
            self._glow_effect.setColor(QColor(0, 0, 0, 0))
            self.setGraphicsEffect(self._glow_effect)

            # Animation de couleur pour un effet de pulsation moderne
            self._glow_anim = QPropertyAnimation(self._glow_effect, b"color", self)
            # Légèrement plus lente et cyclique (pas de "reset" brutal en fin de cycle)
            self._glow_anim.setDuration(1700)
            base_color = QColor(74, 222, 128, 28)  # vert menthe très doux
            peak_color = QColor(52, 211, 153, 150)  # vert menthe plus lumineux
            self._glow_anim.setStartValue(base_color)
            self._glow_anim.setKeyValueAt(0.5, peak_color)
            self._glow_anim.setEndValue(base_color)  # boucle fluide: fin == début
            self._glow_anim.setEasingCurve(QEasingCurve.InOutSine)
            self._glow_anim.setLoopCount(-1)
        except Exception:
            self._glow_effect = None
            self._glow_anim = None

        lay = QHBoxLayout(self)
        lay.setContentsMargins(10, 6, 10, 6)
        lay.setSpacing(8)

        self.dot = StatusDot(10)
        lay.addWidget(self.dot, 0, Qt.AlignVCenter)

        textcol = QVBoxLayout()
        textcol.setSpacing(1)
        self.lbl_title = QLabel(title)
        self.lbl_title.setObjectName("CardTitle")
        self.lbl_extra = QLabel("")
        self.lbl_extra.setObjectName("ExtraLabel")
        textcol.addWidget(self.lbl_title)
        textcol.addWidget(self.lbl_extra)
        lay.addLayout(textcol, 1)

        btns = QHBoxLayout()
        btns.setSpacing(5)
        # Boutons actions : on privilégie les icônes (issues des prefs),
        # avec fallback en emojis si les fichiers ne sont pas disponibles.
        self.btn_focus = QPushButton("")
        self.btn_reload = QPushButton("")
        self.btn_kill = QPushButton("")
        self.btn_del = QPushButton("")
        for b in (self.btn_focus, self.btn_reload, self.btn_kill, self.btn_del):
            b.setCursor(Qt.PointingHandCursor)
            # Fixe une taille carrée pour que l'icône n'agrandisse pas le bouton
            b.setFixedSize(32, 32)

        self.btn_focus.setToolTip("Mettre au premier plan")
        self.btn_reload.setToolTip("Relancer l'instance")
        self.btn_kill.setToolTip("Terminer le processus")
        self.btn_del.setToolTip("Supprimer le widget")

        # Icônes depuis les préférences (chemins) avec fallback en texte
        focus_icon = get_icon("focus")
        if not focus_icon.isNull():
            self.btn_focus.setIcon(focus_icon)
            self.btn_focus.setIconSize(QSize(20, 20))
        else:
            self.btn_focus.setText("👁")

        play_icon = get_icon("play")
        if not play_icon.isNull():
            self.btn_reload.setIcon(play_icon)
            self.btn_reload.setIconSize(QSize(20, 20))
        else:
            self.btn_reload.setText("▶")

        stop_icon = get_icon("stop")
        if not stop_icon.isNull():
            self.btn_kill.setIcon(stop_icon)
            self.btn_kill.setIconSize(QSize(20, 20))
        else:
            self.btn_kill.setText("⛔")

        trash_icon = get_icon("trash")
        if not trash_icon.isNull():
            self.btn_del.setIcon(trash_icon)
            self.btn_del.setIconSize(QSize(20, 20))
        else:
            self.btn_del.setText("🗑️")

        btns.addWidget(self.btn_focus)
        btns.addWidget(self.btn_reload)
        btns.addWidget(self.btn_kill)
        btns.addWidget(self.btn_del)
        lay.addLayout(btns, 0)

        self.btn_focus.clicked.connect(lambda: self.requestFocus.emit(self.title_id))
        self.btn_reload.clicked.connect(
            lambda: self.requestRelaunch.emit(self.title_id)
        )
        self.btn_kill.clicked.connect(lambda: self.requestKill.emit(self.title_id))
        self.btn_del.clicked.connect(lambda: self.requestDelete.emit(self.title_id))

    def update_status(self, color_hex: str, extra_text: str = ""):
        self.dot.set_color(color_hex)
        self.lbl_extra.setText(extra_text)

        # Met à jour la "propriété d'aura" en fonction de la couleur du voyant
        # -> vert = active, jaune = warning, rouge = error, violet = recovery, bleu = manuel
        try:
            sev = "none"
            if color_hex:
                ch = color_hex.lower()
                if ch == CLR_GREEN.lower():
                    sev = "green"
                elif ch == CLR_YELLOW.lower():
                    sev = "yellow"
                elif ch == CLR_RED.lower():
                    sev = "red"
                elif ch == CLR_PURPLE.lower():
                    sev = "purple"
                elif ch == CLR_BLUE.lower():
                    sev = "blue"

            # Propriétés QSS
            self.setProperty("state", sev)
            self.setProperty("active", "true" if sev == "green" else "false")
            self.setProperty("warning", "true" if sev == "yellow" else "false")
            self.setProperty("error", "true" if sev == "red" else "false")

            # Gestion du glow animé : démarrer/arrêter / changer la couleur selon la sévérité
            if sev != self._glow_mode:
                # Stopper l'ancien mode si besoin
                if self._glow_anim is not None:
                    self._glow_anim.stop()

                if (
                    sev in ("green", "yellow", "red", "purple", "blue")
                    and self._glow_effect is not None
                    and self._glow_anim is not None
                ):
                    # Choix des couleurs selon la sévérité
                    if sev == "green":
                        base_color = QColor(74, 222, 128, 28)
                        peak_color = QColor(52, 211, 153, 150)
                    elif sev == "yellow":
                        base_color = QColor(252, 211, 77, 28)  # jaune doux
                        peak_color = QColor(250, 204, 21, 150)  # jaune soleil
                    elif sev == "red":
                        base_color = QColor(248, 113, 113, 28)  # rouge doux
                        peak_color = QColor(239, 68, 68, 150)  # rouge plus intense
                    elif sev == "purple":
                        base_color = QColor(196, 181, 253, 28)  # violet doux
                        peak_color = QColor(167, 139, 250, 150)  # violet plus lumineux
                    else:  # blue
                        base_color = QColor(96, 165, 250, 28)  # bleu doux
                        peak_color = QColor(59, 130, 246, 150)  # bleu plus intense

                    self._glow_anim.setStartValue(base_color)
                    self._glow_anim.setKeyValueAt(0.5, peak_color)
                    self._glow_anim.setEndValue(base_color)

                    self._glow_effect.setBlurRadius(26)
                    self._glow_anim.start()
                    self._active = True
                else:
                    # Aucun glow (none)
                    self._active = False
                    if self._glow_effect is not None:
                        self._glow_effect.setColor(QColor(0, 0, 0, 0))
                        self._glow_effect.setBlurRadius(0)

                self._glow_mode = sev

            # Reforcer le rafraîchissement du style QSS
            self.style().unpolish(self)
            self.style().polish(self)
        except Exception:
            pass

    def set_title(self, title: str):
        self.title_id = title
        shead = title
        self.lbl_title.setText(shead)

    def set_selected(self, selected: bool):
        self.setProperty("selected", "true" if selected else "false")
        self.style().unpolish(self)
        self.style().polish(self)


class SubctrlItemWidget(QWidget):
    requestDelete = Signal(str)
    """
    Widget simple : point de statut à gauche (centré verticalement), puis une colonne verticale contenant :
      ID (ligne 1, bold)
      Alias (ligne 2, bold/colorée, police réduite)
      Last Update (ligne 3, petit, aligné à gauche)
    """

    def __init__(self, alias: str, sid: str, last_ts: float):
        super().__init__()
        self.sid = sid  # <--- AJOUT obligatoire
        self.setObjectName("SubsCard")
        self.setAttribute(Qt.WA_StyledBackground, True)  # <-- AJOUT

        lay = QHBoxLayout(self)
        lay.setContentsMargins(4, 6, 4, 6)  # Marges réduites pour maximiser l'espace
        lay.setSpacing(8)  # Espacement réduit entre le dot et le texte

        # dot (centré verticalement)
        self.dot = StatusDot(10, CLR_GREY)
        # <-- ALIGNMENT CHANGE: center the dot vertically so it no longer sits at the top
        lay.addWidget(self.dot, 0, Qt.AlignVCenter)

        # colonne verticale : id / alias / last
        col = QVBoxLayout()
        col.setSpacing(2)
        col.setContentsMargins(0, 0, 0, 0)  # Pas de marges internes pour le texte

        self.lbl_id = QLabel()
        self.lbl_id.setObjectName("CardTitle")
        self.lbl_id.setTextFormat(Qt.PlainText)
        self.lbl_id.setWordWrap(False)
        # id en gras (utilise le style défini pour CardTitle)
        self.lbl_id.setStyleSheet("font-weight:700;")

        self.lbl_alias = QLabel()
        self.lbl_alias.setObjectName("CardSubtitle")
        self.lbl_alias.setTextFormat(Qt.PlainText)
        self.lbl_alias.setWordWrap(True)
        # alias en gras et couleur secondaire, police réduite (~25% smaller than base 14px)
        # ajuste '11px' pour augmenter/diminuer la réduction (ex: 12px ~ -14%, 10px ~ -29%)
        self.lbl_alias.setStyleSheet("font-weight:700; color:#93c5fd; font-size:11px;")

        self.lbl_last = QLabel()
        self.lbl_last.setObjectName("ExtraLabel")
        self.lbl_last.setTextFormat(Qt.PlainText)
        self.lbl_last.setWordWrap(False)
        self.lbl_last.setAlignment(Qt.AlignLeft | Qt.AlignVCenter)

        col.addWidget(self.lbl_id)
        col.addWidget(self.lbl_alias)
        col.addWidget(self.lbl_last)

        lay.addLayout(col, 1)

        # hauteur optimisée pour tenir 3 lignes tout en restant compact
        self.setMinimumHeight(64)
        self.setMaximumHeight(100)

        # initialisation
        self.set(alias, sid, last_ts)

    def set(self, alias: str, sid: str, last_ts: float):
        """
        Note: on n'échappe plus '<' ou '>' — QLabel est en PlainText donc il n'interprétera pas du HTML.
        On remplace seulement '_' -> ':' comme demandé.
        """
        try:
            # ne pas échapper '<' '>' (cela provoquait =&gt;), uniquement remplacer '_' -> ':'
            alias_txt = alias or ""
            sid_txt = sid or ""
            alias_txt = alias_txt.replace("_", ":")
            sid_txt = sid_txt.replace("_", ":")
        except Exception:
            alias_txt = alias or ""
            sid_txt = sid or ""

        self.lbl_id.setText(sid_txt)
        self.lbl_alias.setText(alias_txt)

        try:
            if not last_ts or last_ts <= 0:
                last_text = "-"
            else:
                last_text = datetime.fromtimestamp(float(last_ts)).strftime("%H:%M")
            # affichage compact et localisé
            self.lbl_last.setText(
                f"Last Update : {last_text}" if last_text != "-" else "-"
            )
        except Exception:
            self.lbl_last.setText("-")


class ItemPerWidgetList(QListWidget):
    """
    QListWidget qui scroll widget-par-widget quand l'utilisateur utilise la molette.
    On utilise CARD_HEIGHT pour avancer/reculer d'un item à chaque 'crantage'.
    """

    orderChanged = Signal(list)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.setVerticalScrollMode(QAbstractItemView.ScrollPerPixel)

        # DnD natif pour réordonner
        self.setDragEnabled(True)
        self.setAcceptDrops(True)
        self.setDefaultDropAction(Qt.MoveAction)
        self.setDragDropMode(QAbstractItemView.InternalMove)
        self.setDropIndicatorShown(True)

    def wheelEvent(self, event):
        delta = event.angleDelta().y()
        if delta == 0:
            event.ignore()
            return
        steps = int(delta / 120) if abs(delta) >= 120 else (1 if delta > 0 else -1)
        sb = self.horizontalScrollBar()
        try:
            if self.count() > 0 and self.item(0) is not None:
                col_w = max(80, self.item(0).sizeHint().width())
            else:
                col_w = max(80, int(self.viewport().width() / 2))
        except Exception:
            col_w = 300
        new_val = sb.value() - steps * col_w
        new_val = max(sb.minimum(), min(sb.maximum(), new_val))
        sb.setValue(new_val)
        event.accept()

    def dropEvent(self, event):
        super().dropEvent(event)
        titles = []
        for i in range(self.count()):
            it = self.item(i)
            if it is not None:
                titles.append(it.data(Qt.UserRole))
        self.orderChanged.emit(titles)

    def mousePressEvent(self, event):
        # On mémorise le mode multi-sélection par défaut
        if not hasattr(self, "_default_sel_mode"):
            self._default_sel_mode = QAbstractItemView.ExtendedSelection

        if event.button() == Qt.LeftButton:
            item = self.itemAt(event.pos())
            mods = event.modifiers()

            if item is not None:
                # Clic sur un widget
                if mods & (Qt.ControlModifier | Qt.ShiftModifier):
                    # On garde ExtendedSelection pour permettre toggle / range
                    self.setSelectionMode(self._default_sel_mode)
                else:
                    # Clic simple → on passe en SingleSelection pour autoriser le drag propre
                    self.setSelectionMode(QAbstractItemView.SingleSelection)
            else:
                # Clic dans une zone vide → rubber band de sélection multiple
                self.setSelectionMode(self._default_sel_mode)

        super().mousePressEvent(event)

    def mouseReleaseEvent(self, event):
        # Toujours restaurer le mode étendu après le clic, pour ne JAMAIS casser Ctrl/Shift
        if hasattr(self, "_default_sel_mode"):
            self.setSelectionMode(self._default_sel_mode)
        super().mouseReleaseEvent(event)


# ======================== Custom Widgets =============================
class CustomSpinBox(QWidget):
    """Widget personnalisé pour remplacer QSpinBox avec des boutons + et - bien visibles."""

    valueChanged = Signal(int)

    def __init__(
        self, parent=None, min_value=0, max_value=600, initial_value=0, suffix=""
    ):
        super().__init__(parent)
        self._min = min_value
        self._max = max_value
        self._value = initial_value
        self._suffix = suffix
        self._is_processing = False  # Flag pour éviter la double exécution

        # Layout horizontal
        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        # Champ de texte éditable pour afficher et saisir la valeur
        self.value_edit = QLineEdit()
        self.value_edit.setAlignment(Qt.AlignCenter)
        self.value_edit.setValidator(QIntValidator(min_value, max_value))
        self.value_edit.setStyleSheet(
            """
            QLineEdit {
                background: rgba(15,23,42,0.42);
                border: 1px solid rgba(255,255,255,0.12);
                border-top-left-radius: 8px;
                border-bottom-left-radius: 8px;
                border-right: none;
                padding: 6px 12px;
                min-height: 28px;
                color: #e5e7eb;
                font-size: 14px;
            }
            QLineEdit:focus {
                border: 1px solid rgba(59,130,246,0.6);
                background: rgba(15,23,42,0.6);
            }
        """
        )
        self._update_display()
        # Connecter le signal returnPressed (touche Entrée)
        self.value_edit.returnPressed.connect(self._on_manual_input)
        # Connecter aussi editingFinished (perte de focus)
        self.value_edit.editingFinished.connect(self._on_editing_finished)
        layout.addWidget(self.value_edit)

        # Container pour les boutons
        buttons_container = QWidget()
        buttons_container.setFixedWidth(24)
        buttons_layout = QVBoxLayout(buttons_container)
        buttons_layout.setContentsMargins(0, 0, 0, 0)
        buttons_layout.setSpacing(0)

        # Bouton +
        self.btn_plus = QPushButton("▲")
        self.btn_plus.setFixedSize(24, 14)
        self.btn_plus.setCursor(Qt.PointingHandCursor)
        self.btn_plus.setStyleSheet(
            """
            QPushButton {
                background-color: rgba(37,99,235,0.6);
                color: #ffffff;
                border: none;
                border-top-right-radius: 8px;
                border-bottom-right-radius: 0px;
                font-weight: bold;
                font-size: 9px;
                padding: 0px;
            }
            QPushButton:hover {
                background-color: rgba(59,130,246,0.8);
            }
            QPushButton:pressed {
                background-color: rgba(30,64,175,0.9);
            }
            QPushButton:disabled {
                background-color: rgba(30,58,138,0.4);
                color: #94a3b8;
            }
        """
        )
        self.btn_plus.clicked.connect(self._increment)
        buttons_layout.addWidget(self.btn_plus)

        # Bouton -
        self.btn_minus = QPushButton("▼")
        self.btn_minus.setFixedSize(24, 14)
        self.btn_minus.setCursor(Qt.PointingHandCursor)
        self.btn_minus.setStyleSheet(
            """
            QPushButton {
                background-color: rgba(37,99,235,0.6);
                color: #ffffff;
                border: none;
                border-top-right-radius: 0px;
                border-bottom-right-radius: 8px;
                border-top: 1px solid rgba(255,255,255,0.08);
                font-weight: bold;
                font-size: 9px;
                padding: 0px;
            }
            QPushButton:hover {
                background-color: rgba(59,130,246,0.8);
            }
            QPushButton:pressed {
                background-color: rgba(30,64,175,0.9);
            }
            QPushButton:disabled {
                background-color: rgba(30,58,138,0.4);
                color: #94a3b8;
            }
        """
        )
        self.btn_minus.clicked.connect(self._decrement)
        buttons_layout.addWidget(self.btn_minus)

        layout.addWidget(buttons_container)

        # Activer le scroll de la souris
        self.setMouseTracking(True)
        self._update_buttons_state()

    def _update_display(self):
        """Met à jour l'affichage de la valeur."""
        text = f"{self._value}{self._suffix}"
        self.value_edit.setText(text)

    def _on_manual_input(self):
        """Gère la saisie manuelle de la valeur (touche Entrée)."""
        # Éviter la double exécution si déjà en cours de traitement
        if self._is_processing:
            return

        self._is_processing = True

        try:
            text = self.value_edit.text().strip()
            # Enlever le suffixe si présent
            if self._suffix and text.endswith(self._suffix):
                text = text[: -len(self._suffix)].strip()

            # Tenter de convertir en entier
            try:
                new_value = int(text)
                # Contraindre dans les limites
                new_value = max(self._min, min(self._max, new_value))

                if new_value != self._value:
                    self._value = new_value
                    self._update_display()
                    self._update_buttons_state()
                    self.valueChanged.emit(self._value)
                else:
                    # Même valeur, mais réafficher avec le bon format
                    self._update_display()
            except ValueError:
                # Si la conversion échoue, remettre l'ancienne valeur
                self._update_display()
        finally:
            # Réinitialiser le flag après un court délai pour permettre les prochaines modifications
            QTimer.singleShot(100, self._reset_processing_flag)

    def _on_editing_finished(self):
        """Gère la perte de focus du champ (mais pas si Entrée a déjà été pressée)."""
        # Si on traite déjà une entrée (via returnPressed), ne rien faire
        if not self._is_processing:
            self._on_manual_input()

    def _reset_processing_flag(self):
        """Réinitialise le flag de traitement."""
        self._is_processing = False

    def _update_buttons_state(self):
        """Met à jour l'état des boutons selon les limites min/max."""
        self.btn_plus.setEnabled(self._value < self._max)
        self.btn_minus.setEnabled(self._value > self._min)

    def _increment(self):
        """Incrémente la valeur."""
        if self._value < self._max:
            self._value += 1
            self._update_display()
            self._update_buttons_state()
            self.valueChanged.emit(self._value)

    def _decrement(self):
        """Décrémente la valeur."""
        if self._value > self._min:
            self._value -= 1
            self._update_display()
            self._update_buttons_state()
            self.valueChanged.emit(self._value)

    def wheelEvent(self, event):
        """Gère le scroll de la souris pour modifier la valeur."""
        delta = event.angleDelta().y()
        if delta > 0:
            self._increment()
        elif delta < 0:
            self._decrement()
        event.accept()

    def value(self):
        """Retourne la valeur actuelle."""
        return self._value

    def setValue(self, value):
        """Définit la valeur."""
        value = max(self._min, min(self._max, int(value)))
        if value != self._value:
            self._value = value
            self._update_display()
            self._update_buttons_state()
            self.valueChanged.emit(self._value)

    def setRange(self, min_value, max_value):
        """Définit les limites min et max."""
        self._min = int(min_value)
        self._max = int(max_value)
        # Mettre à jour le validator
        self.value_edit.setValidator(QIntValidator(self._min, self._max))
        self._value = max(self._min, min(self._max, self._value))
        self._update_display()
        self._update_buttons_state()

    def setSuffix(self, suffix):
        """Définit le suffixe à afficher."""
        self._suffix = suffix
        self._update_display()


# ======================== GUI =============================
class SnowMasterGUI(QWidget):
    # Signal pour demander un relaunch depuis un thread d'arrière-plan
    relaunch_requested = Signal(str)

    def __init__(self):
        super().__init__()
        self._bg_executor = ThreadPoolExecutor(
            max_workers=4, thread_name_prefix="ankaworker"
        )
        # Connecter le signal à un slot qui relance de manière asynchrone
        # Cela garantit l'exécution dans le thread principal mais sans bloquer
        self.relaunch_requested.connect(self._on_relaunch_requested)
        self._auto_relaunch_scan_lock = threading.Lock()
        # self.setWindowTitle("❄️ SnowMaster")
        self.setWindowTitle(APP_DISPLAY_NAME)
        self.resize(1440, 850)  # +62 pour afficher une instance de plus

        # --- État interne pour le voyant global / alertes ---
        self._last_global_color = CLR_GREY  # dernière couleur vue du voyant global
        self._first_global_dot_update = (
            True  # pour ignorer l'alerte au tout premier update
        )
        self._tray_icon = None

        # Icône systray pour les notifications Windows
        try:
            if QSystemTrayIcon.isSystemTrayAvailable():
                self._tray_icon = QSystemTrayIcon(self)
                self._tray_icon.setToolTip(APP_DISPLAY_NAME)

                # Essaie de réutiliser l'icône de l'application si elle existe
                try:
                    app = QApplication.instance()
                    if app is not None and (not app.windowIcon().isNull()):
                        self._tray_icon.setIcon(app.windowIcon())
                except Exception:
                    pass

                self._tray_icon.show()
        except Exception:
            self._tray_icon = None

        # Flags / state
        self.auto_relaunch_enabled = bool(
            _prefs.get("autoRelaunch", AUTO_RELAUNCH_DEFAULT)
        )
        # Discord alert config (voyant global rouge)
        discord_cfg = _prefs.get("discord", {})
        self.discord_alert_enabled = bool(discord_cfg.get("enabled", False))
        self._last_auto_relaunch_attempt: Dict[str, float] = {}  # title -> ts
        self._pending_resets: List[str] = []  # Queue de titres à reset

        # Autopilot state
        ap = _prefs.get("autopilot", {})
        self.autopilot_enabled = bool(ap.get("enabled", False))
        self.autopilot_mode = str(ap.get("mode", "load_only"))
        self.autopilot_schedules: List[dict] = list(ap.get("schedules", []))
        self._last_autopilot_active_key = None
        self._autopilot_status_text = "-"

        # Header
        header = QHBoxLayout()
        self.title_label = QLabel(f"❄️ {APP_DISPLAY_NAME}")
        self.title_label.setObjectName("TitleLabel")
        self.global_dot = StatusDot(12, CLR_GREY)

        self.btn_new = QPushButton("＋ Nouvelle instance")
        self.btn_all_reload = QPushButton("Relancer tout")
        self.btn_all_kill = QPushButton("Terminer tout")
        self.btn_all_del = QPushButton("Supprimer tout")
        for b in (
            self.btn_new,
            self.btn_all_reload,
            self.btn_all_kill,
            self.btn_all_del,
        ):
            b.setCursor(Qt.PointingHandCursor)

        # Icônes configurables pour les actions globales (relancer / terminer / supprimer tout)
        play_icon_bulk = get_icon("play")
        if not play_icon_bulk.isNull():
            self.btn_all_reload.setIcon(play_icon_bulk)
            self.btn_all_reload.setIconSize(QSize(18, 18))

        stop_icon_bulk = get_icon("stop")
        if not stop_icon_bulk.isNull():
            self.btn_all_kill.setIcon(stop_icon_bulk)
            self.btn_all_kill.setIconSize(QSize(18, 18))

        trash_icon_bulk = get_icon("trash")
        if not trash_icon_bulk.isNull():
            self.btn_all_del.setIcon(trash_icon_bulk)
            self.btn_all_del.setIconSize(QSize(18, 18))

        self.btn_new.clicked.connect(self.on_new_instance)
        self.btn_all_reload.clicked.connect(self.on_bulk_reload)
        self.btn_all_kill.clicked.connect(self.on_bulk_kill)
        self.btn_all_del.clicked.connect(self.on_bulk_delete)

        header.addWidget(self.title_label)
        header.addWidget(self.global_dot)
        header.addStretch(1)
        header.addWidget(self.btn_new)
        header.addWidget(self.btn_all_reload)
        header.addWidget(self.btn_all_kill)
        header.addWidget(self.btn_all_del)

        # Left column
        left_col = QVBoxLayout()
        left_col.addLayout(header)

        group = QGroupBox("Instances actives")
        group_layout = QVBoxLayout(group)
        # Marges internes : on supprime la marge droite pour maximiser l'espace horizontal des cartes
        group_layout.setContentsMargins(8, 8, 0, 8)
        group_layout.setSpacing(6)
        # self.list = QListWidget()
        self.list = ItemPerWidgetList()
        self.list.setObjectName("instancesList")
        # s'assurer que les petits pas de la scrollbar sont ceux d'un widget
        self.list.verticalScrollBar().setSingleStep(CARD_HEIGHT)
        self.list.setFocusPolicy(Qt.NoFocus)
        # self.list.setSelectionMode(QAbstractItemView.SingleSelection)
        self.list.setSelectionMode(
            QAbstractItemView.ExtendedSelection
        )  # <--- ALLOWS CTRL+CLICK
        self.list.setSelectionBehavior(QAbstractItemView.SelectItems)

        # --- IMPORTANT : layout en colonnes ---
        # flow TopToBottom + wrapping True  => remplit une colonne top->bottom puis passe à la suivante
        self.list.setViewMode(QListView.ListMode)
        self.list.setFlow(QListView.TopToBottom)
        self.list.setWrapping(True)
        self.list.setResizeMode(QListView.Adjust)
        self.list.setUniformItemSizes(False)
        # self.list.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.list.setHorizontalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        self.list.setHorizontalScrollMode(QAbstractItemView.ScrollPerPixel)

        self.list.currentItemChanged.connect(self.on_select_instance)
        self.list.installEventFilter(self)
        group_layout.addWidget(self.list)

        left_col.addWidget(group, 3)
        left_w = QWidget()
        left_w.setLayout(left_col)
        left_w.setMinimumWidth(500)
        left_w.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)

        # Dock config (modifié : ajoute "délai entre lancements" + "mode")
        self.btn_save_cfg = QPushButton("💾 Enregistrer config")
        self.btn_load_cfg = QPushButton("📂 Charger config")
        for b in (self.btn_save_cfg, self.btn_load_cfg):
            b.setCursor(Qt.PointingHandCursor)
        self.btn_save_cfg.clicked.connect(self.on_save_config)
        self.btn_load_cfg.clicked.connect(self.on_load_config)

        self.chk_auto_relaunch = QCheckBox("Relance auto crash")
        self.chk_auto_relaunch.setChecked(self.auto_relaunch_enabled)
        self.chk_auto_relaunch.setToolTip(
            "Si coché : scan PID régulier pour détecter et relancer les instances crashées"
        )
        self.chk_auto_relaunch.stateChanged.connect(self.on_toggle_auto_relaunch)

        # Checkbox : envoi d'un Discord hook quand le voyant global passe au rouge
        self.chk_discord_alert = QCheckBox("Discord alert (voyant rouge)")
        self.chk_discord_alert.setChecked(self.discord_alert_enabled)
        self.chk_discord_alert.setToolTip(
            f"Si coché : envoie un webhook Discord quand le voyant global {APP_DISPLAY_NAME} passe au ROUGE."
        )
        self.chk_discord_alert.stateChanged.connect(self.on_toggle_discord_alert)

        # Lecture initiale des prefs pour les instances
        instances_prefs = _prefs.get("instances", {})
        default_delay = int(instances_prefs.get("launch_delay", 1))
        default_inst_mode = instances_prefs.get("mode", "load_and_launch")

        # Widget personnalisé pour le délai entre lancements (secondes)
        self.spin_launch_delay = CustomSpinBox(
            self, min_value=0, max_value=600, initial_value=default_delay, suffix=""
        )
        self.spin_launch_delay.setToolTip(
            "Délai entre deux relances lors de 'Relancer tout' (en secondes). Utilisez la molette de la souris pour modifier."
        )
        self.spin_launch_delay.valueChanged.connect(self.on_change_launch_delay)

        # Widget pour le délai du reddot (heartbeat timeout)
        default_reddot = int(_prefs.get("reddot", 480))
        self.spin_reddot = CustomSpinBox(
            self, min_value=60, max_value=3600, initial_value=default_reddot, suffix=""
        )
        self.spin_reddot.setToolTip(
            "Délai avant qu'un voyant passe au ROUGE en l'absence de heartbeat (en secondes). Utilisez la molette de la souris pour modifier."
        )
        self.spin_reddot.valueChanged.connect(self.on_change_reddot)

        # ComboBox : Mode pour le chargement d'une config (load_only / load_and_launch)
        self.chk_instance_launch = QCheckBox("Load and launch")
        self.chk_instance_launch.setChecked(str(default_inst_mode) == "load_and_launch")
        self.chk_instance_launch.setToolTip(
            "Coché : load_and_launch — Décoché : load_only"
        )
        self.chk_instance_launch.stateChanged.connect(
            self.on_toggle_instance_launch_mode
        )

        # Widgets autopilote (seront ajoutés au CollapsibleGroupBox)
        self.chk_autopilot = QCheckBox("Activer l'autopilote")
        self.chk_autopilot.setChecked(self.autopilot_enabled)
        self.chk_autopilot.stateChanged.connect(self.on_toggle_autopilot)

        # Checkbox : décoché = load_only, coché = load_and_launch
        self.chk_autopilot_launch = QCheckBox("Load and launch")
        self.chk_autopilot_launch.setChecked(
            str(self.autopilot_mode) == "load_and_launch"
        )
        self.chk_autopilot_launch.setToolTip(
            "Coché : load_and_launch — Décoché : load_only"
        )
        self.chk_autopilot_launch.stateChanged.connect(self.on_toggle_autopilot_mode)

        self.lbl_autopilot_status = QLabel("-")
        self.lbl_autopilot_status.setObjectName("ExtraLabel")

        self.btn_ap_load_plan = QPushButton("📂 Charger planning")
        self.btn_ap_save_plan = QPushButton("💾 Enregistrer planning")
        self.btn_ap_force_eval = QPushButton("⚡ Forcer évaluation")
        for b in (self.btn_ap_load_plan, self.btn_ap_save_plan, self.btn_ap_force_eval):
            b.setCursor(Qt.PointingHandCursor)
        self.btn_ap_load_plan.clicked.connect(self.on_autopilot_load_plan)
        self.btn_ap_save_plan.clicked.connect(self.on_autopilot_save_plan)
        self.btn_ap_force_eval.clicked.connect(self.evaluate_autopilot_now)

        # --- Nouveau: empêcher l'autopilote d'écraser les instances ---
        self.chk_autopilot_overwrite = QCheckBox("Overwrite")
        try:
            init_ap_overwrite = bool(
                _prefs.get("autopilot", {}).get("overwrite", False)
            )
        except Exception:
            init_ap_overwrite = False
        self.chk_autopilot_overwrite.setChecked(init_ap_overwrite)
        self.chk_autopilot_overwrite.stateChanged.connect(
            self.on_toggle_autopilot_overwrite
        )

        self.configDock = QWidget()
        self.configDock.setObjectName("ConfigDock")
        cfg_v = QVBoxLayout(self.configDock)
        cfg_v.setContentsMargins(10, 12, 10, 12)
        cfg_v.setSpacing(10)

        # Groupes réductibles avec sauvegarde des préférences
        inst_group_collapsible = CollapsibleGroupBox("Configs")
        try:
            inst_expanded = bool(_prefs.get("ui", {}).get("instances_expanded", True))
        except Exception:
            inst_expanded = True
        inst_group_collapsible.setExpanded(inst_expanded)
        inst_group_collapsible.toggle.connect(
            lambda: self._save_collapse_pref(
                "instances_expanded", inst_group_collapsible.isExpanded()
            )
        )

        inst_group_collapsible.addWidget(self.chk_auto_relaunch)
        inst_group_collapsible.addWidget(self.chk_discord_alert)
        # inst_group_collapsible.addWidget(self.btn_save_cfg)
        # inst_group_collapsible.addWidget(self.btn_load_cfg)
        # Checkbox: overwrite existing instances when loading a config
        # self.chk_overwrite_instances = QCheckBox("Overwrite")
        # récupérer valeur initiale depuis les prefs (par défaut True)
        # try:
        #     init_overwrite = bool(
        #         _prefs.get("instances", {}).get("overwrite_on_load", True)
        #     )
        # except Exception:
        #     init_overwrite = True
        # self.chk_overwrite_instances.setChecked(init_overwrite)
        # self.chk_overwrite_instances.stateChanged.connect(
        #     self.on_toggle_overwrite_instances
        # )
        # inst_group_collapsible.addWidget(self.chk_overwrite_instances)

        # nouveaux champs pour instances
        inst_group_collapsible.addWidget(QLabel("Délai entre lancements (s) :"))
        inst_group_collapsible.addWidget(self.spin_launch_delay)
        inst_group_collapsible.addWidget(QLabel("Délai reddot (60s minimum) :"))
        inst_group_collapsible.addWidget(self.spin_reddot)
        # inst_group_collapsible.addWidget(self.chk_instance_launch)

        ap_group_collapsible = CollapsibleGroupBox("Autopilote")
        try:
            ap_expanded = bool(_prefs.get("ui", {}).get("autopilot_expanded", True))
        except Exception:
            ap_expanded = True
        ap_group_collapsible.setExpanded(ap_expanded)
        ap_group_collapsible.toggle.connect(
            lambda: self._save_collapse_pref(
                "autopilot_expanded", ap_group_collapsible.isExpanded()
            )
        )

        # Déplacer le contenu de ap_group vers ap_group_collapsible
        ap_group_collapsible.addWidget(self.chk_autopilot)
        ap_group_collapsible.addWidget(self.chk_autopilot_overwrite)
        ap_group_collapsible.addWidget(self.lbl_autopilot_status)
        ap_group_collapsible.addWidget(self.btn_ap_load_plan)
        # ap_group_collapsible.addWidget(self.btn_ap_save_plan)
        ap_group_collapsible.addWidget(self.btn_ap_force_eval)
        ap_group_collapsible.addWidget(self.chk_autopilot_launch)

        cfg_v.addWidget(inst_group_collapsible)
        cfg_v.addWidget(ap_group_collapsible)

        # GroupBox "Boutons" pour les boutons programmables
        buttons_group_collapsible = CollapsibleGroupBox("Boutons")
        try:
            buttons_expanded = bool(_prefs.get("ui", {}).get("buttons_expanded", True))
        except Exception:
            buttons_expanded = True
        buttons_group_collapsible.setExpanded(buttons_expanded)
        buttons_group_collapsible.toggle.connect(
            lambda: self._save_collapse_pref(
                "buttons_expanded", buttons_group_collapsible.isExpanded()
            )
        )

        # Conteneur pour les boutons programmables
        self.custom_buttons_container = QWidget()
        self.custom_buttons_layout = QVBoxLayout(self.custom_buttons_container)
        self.custom_buttons_layout.setContentsMargins(0, 0, 0, 0)
        self.custom_buttons_layout.setSpacing(6)

        # Bouton "Ajouter un bouton"
        self.btn_add_custom = QPushButton("➕ Ajouter un bouton")
        self.btn_add_custom.setCursor(Qt.PointingHandCursor)
        self.btn_add_custom.clicked.connect(self.on_add_custom_button)
        # Style plus sombre pour différencier des boutons programmables
        self.btn_add_custom.setStyleSheet(
            """
            QPushButton {
                background-color: #1e293b;
                color: #94a3b8;
                border: 1px solid #334155;
                border-radius: 10px;
                padding: 6px 10px;
                font-weight: 600;
            }
            QPushButton:hover {
                background-color: #334155;
                color: #cbd5e1;
                border: 1px solid #475569;
            }
            QPushButton:pressed {
                background-color: #0f172a;
            }
        """
        )

        buttons_group_collapsible.addWidget(self.custom_buttons_container)
        buttons_group_collapsible.addWidget(self.btn_add_custom)

        cfg_v.addWidget(buttons_group_collapsible)

        # Charger les boutons sauvegardés
        self._load_custom_buttons()

        # Stretch pour pousser les éléments fixes en bas
        cfg_v.addStretch(1)

        # Zone fixe en bas : bouton PANIC TS → bouton instance vide → compteur instances → bouton argent
        # --- Bouton PANIC TS : lance le script contrôleur sur le 1er contrôleur des instances sélectionnées ---
        self.btn_panic = QPushButton("PANIC TS")
        self.btn_panic.setCursor(Qt.PointingHandCursor)
        self.btn_panic.setToolTip(
            "Lance immédiatement le script contrôleur sur le premier contrôleur des instances sélectionnées."
        )
        # Styles PANIC (normal / cooldown) + état interne de cooldown
        self._panic_style_normal = """
            QPushButton {
                background-color: #b91c1c;
                color: #f9fafb;
                border-radius: 10px;
                border: 1px solid #7f1d1d;
                padding: 6px 10px;
                font-weight: 700;
            }
            QPushButton:hover {
                background-color: #dc2626;
                border-color: #991b1b;
            }
            QPushButton:pressed {
                background-color: #7f1d1d;
            }
        """
        self._panic_style_cooldown = """
            QPushButton {
                background-color: #4b5563;
                color: #e5e7eb;
                border-radius: 10px;
                border: 1px solid #374151;
                padding: 6px 10px;
                font-weight: 700;
            }
        """
        self._panic_cooldown_active = False
        self.btn_panic.setStyleSheet(self._panic_style_normal)
        self.btn_panic.clicked.connect(self.on_panic_selected_instances)
        cfg_v.addWidget(self.btn_panic)

        # --- Nouveau: bouton pour lancer une "instance vide" (juste exe, sans automatisation) ---
        self.btn_launch_empty = QPushButton("＋ Instance vide")
        self.btn_launch_empty.setCursor(Qt.PointingHandCursor)
        self.btn_launch_empty.setToolTip(
            "Lance l'exécutable EXE sans automatisation. Donne un nom à l'instance."
        )
        self.btn_launch_empty.clicked.connect(self.on_launch_empty_instance)
        cfg_v.addWidget(self.btn_launch_empty)

        # Compteur d'instances (grand, centré)
        self.lbl_instances_big = QLabel("0 instances")
        self.lbl_instances_big.setObjectName("InstancesBigCount")
        self.lbl_instances_big.setAlignment(Qt.AlignCenter)
        # occupe un peu d'espace visuel dans le bas du dock
        self.lbl_instances_big.setFixedHeight(92)
        cfg_v.addWidget(self.lbl_instances_big)

        # ---------- Compteur € cliquable (même look que InstancesBigCount) ----------
        self.panel_euros = ClickablePanel("0\n€ générés")
        self.panel_euros.setObjectName("EuroBigCounter")
        self.panel_euros.setAlignment(Qt.AlignCenter)
        self.panel_euros.setFixedHeight(92)
        self.panel_euros.setCursor(Qt.PointingHandCursor)
        self.panel_euros.clicked.connect(self.open_revenue_window)

        # Ombre douce (si l'import manque : from PySide6.QtWidgets import QGraphicsDropShadowEffect ; from PySide6.QtGui import QColor)
        try:
            shadow = QGraphicsDropShadowEffect(self.panel_euros)
            shadow.setBlurRadius(24)
            shadow.setXOffset(0)
            shadow.setYOffset(8)
            shadow.setColor(QColor(16, 185, 129, 90))
            self.panel_euros.setGraphicsEffect(shadow)
        except Exception:
            pass

        # Bouton argent (fixe en bas)
        cfg_v.addWidget(self.panel_euros)

        # ----- Données & remplissage -----
        # On utilise le store global _revenue_data (protégé par _revenue_lock)
        with _revenue_lock:
            # si le fichier a déjà des valeurs (demo dans l'ancien code), on ne les perd pas :
            if not _revenue_data["servers"]:
                _revenue_data["servers"] = {
                    "Mikhal": 0.0,
                    "Dakal": 0.0,
                    "Kourial": 0.0,
                    "Rafal": 0.0,
                    "Salar": 0.0,
                    "Brial": 0.0,
                    "Draconiros": 0.0,
                    "Imagiro": 0.0,
                    "Orukam": 0.0,
                    "Hell Mina": 0.0,
                    "Tylezia": 0.0,
                    "Ombre": 0.0,
                    "Tal Kasha": 0.0,
                }
            # ensure holds exist
            _revenue_data.setdefault("holdings", {"TS": {}, "M": {}})
            for kind in ("TS", "M"):
                for s in iter_servers_in_display_order(_revenue_data["servers"].keys()):
                    _revenue_data["holdings"][kind].setdefault(s, 0)

        # expose local handle to the shared data (kept in sync via bus.revenue_updated)
        self.revenue_data = _revenue_data

        # Calcul initial
        self.update_revenue_counter()

        self.configDock.setFixedWidth(208)
        self.configDock.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Expanding)
        self.configDock.setStyleSheet(
            "#ConfigDock QPushButton { padding:5px 8px; font-size:13px; } "
            "#ConfigDock QLabel:not(#EuroBigCounter) { font-size:13px; } "
            "#ConfigDock { font-size:13px; }"
        )

        # Right column
        right = QVBoxLayout()
        cap2 = QLabel("Sous-contrôleurs")
        cap2.setStyleSheet("color:#93c5fd; font-weight:600;")
        right.addWidget(cap2)
        self.subs_list = QListWidget()
        self.subs_list.setObjectName("subsList")
        self.subs_list.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.subs_list.setSelectionMode(QAbstractItemView.NoSelection)
        # Réduire les marges internes du QListWidget pour maximiser l'espace des items
        self.subs_list.setContentsMargins(0, 0, 0, 0)
        right.addWidget(self.subs_list, 1)

        self.details = QGroupBox("Détails de l'instance")
        form = QFormLayout()
        # self.lbl_title       = QLabel("-")
        self.edit_title = QLineEdit()
        self.edit_title.setPlaceholderText("Nom de l'instance")
        self.edit_title.setClearButtonEnabled(True)
        self.edit_title.setMaxLength(120)
        self.edit_title.editingFinished.connect(self.on_instance_rename)
        form.addRow("Titre :", self.edit_title)
        # form.addRow("Titre :", self.edit_title)

        self.lbl_script = QLabel("-")
        self.lbl_last_reset = QLabel("-")  # <-- NOUVEAU
        self.lbl_status = QLabel("-")
        self.lbl_pid = QLabel("-")
        self.lbl_subs = QLabel("-")

        # form.addRow("Titre :", self.lbl_title)
        form.addRow("Script :", self.lbl_script)
        form.addRow("Last Update :", self.lbl_status)
        form.addRow(
            "Last Reset :", self.lbl_last_reset
        )  # <-- NOUVEAU (avant Last Update)

        form.addRow("PID :", self.lbl_pid)
        # form.addRow("Sous-contrôleurs :", self.lbl_subs)

        grid_btns = QGridLayout()
        grid_btns.setHorizontalSpacing(8)
        grid_btns.setVerticalSpacing(6)

        # Boutons de la fiche détail : icônes issues des prefs (ICONS) + libellés explicites
        self.btn_focus_d = QPushButton("Premier plan")
        self.btn_reload_d = QPushButton("Relancer")
        self.btn_kill_d = QPushButton("Terminer")
        self.btn_del_d = QPushButton("Supprimer")

        for b in (self.btn_focus_d, self.btn_reload_d, self.btn_kill_d, self.btn_del_d):
            b.setCursor(Qt.PointingHandCursor)
            b.setMinimumWidth(120)
            b.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)

        # Appliquer les icônes configurables aux boutons de détail (si disponibles)
        focus_icon_d = get_icon("focus")
        if not focus_icon_d.isNull():
            self.btn_focus_d.setIcon(focus_icon_d)
            self.btn_focus_d.setIconSize(QSize(20, 20))

        play_icon_d = get_icon("play")
        if not play_icon_d.isNull():
            self.btn_reload_d.setIcon(play_icon_d)
            self.btn_reload_d.setIconSize(QSize(20, 20))

        stop_icon_d = get_icon("stop")
        if not stop_icon_d.isNull():
            self.btn_kill_d.setIcon(stop_icon_d)
            self.btn_kill_d.setIconSize(QSize(20, 20))

        trash_icon_d = get_icon("trash")
        if not trash_icon_d.isNull():
            self.btn_del_d.setIcon(trash_icon_d)
            self.btn_del_d.setIconSize(QSize(20, 20))

        grid_btns.addWidget(self.btn_focus_d, 0, 0)
        grid_btns.addWidget(self.btn_reload_d, 0, 1)
        grid_btns.addWidget(self.btn_kill_d, 1, 0)
        grid_btns.addWidget(self.btn_del_d, 1, 1)

        grid_btns.setColumnStretch(0, 1)
        grid_btns.setColumnStretch(1, 1)

        form.addRow(grid_btns)
        self.details.setLayout(form)
        right.addWidget(self.details, 0)

        right_w = QWidget()
        right_w.setLayout(right)
        right_w.setFixedWidth(288)
        right_w.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Expanding)

        # Splitter
        splitter = QSplitter(Qt.Horizontal)
        splitter.addWidget(left_w)
        splitter.addWidget(self.configDock)
        splitter.addWidget(right_w)

        splitter.setStretchFactor(0, 1)
        splitter.setStretchFactor(1, 0)
        splitter.setStretchFactor(2, 0)
        splitter.setCollapsible(1, False)
        splitter.setCollapsible(2, False)
        splitter.setSizes([1200, 208, 288])

        root = QVBoxLayout(self)
        root.addWidget(splitter)

        # Timer / autopilot
        self.timer = QTimer(self)
        self.timer.timeout.connect(self.refresh_cards_and_details)
        self.timer.start(1000)
        self.timer_autopilot = QTimer(self)
        self.timer_autopilot.timeout.connect(self.evaluate_autopilot_periodic)
        self.timer_autopilot.start(30000)
        # Timer pour scan périodique des PID (toutes les minutes) - démarre seulement si auto_relaunch_enabled
        self.timer_pid_scan = QTimer(self)
        self.timer_pid_scan.timeout.connect(self._scan_pids_periodic)
        if self.auto_relaunch_enabled:
            self.timer_pid_scan.start(30000)  # 30000ms = 30 secondes (plus réactif)

        bus.new_instance.connect(self.on_bus_update)
        bus.instance_updated.connect(self.on_bus_update)
        bus.instance_removed.connect(self.on_bus_remove)
        # NOUVEAU : reset => kill + relaunch
        bus.reset_instance.connect(self.on_bus_reset_instance)
        bus.goodbye_kill.connect(self.on_bus_goodbye_kill)  # NEW

        # connect bus signal pour recevoir les mises à jour venant de Flask / fetch
        bus.revenue_updated.connect(self._on_revenue_updated)

        self.btn_focus_d.clicked.connect(
            lambda: self.on_card_focus(self.selected_title() or "")
        )
        self.btn_reload_d.clicked.connect(
            lambda: self.on_card_relaunch(self.selected_title() or "")
        )
        self.btn_kill_d.clicked.connect(
            lambda: self.on_card_kill(self.selected_title() or "")
        )
        self.btn_del_d.clicked.connect(
            lambda: self.on_card_delete(self.selected_title() or "")
        )

        # Charger ordre existant (sinon liste vide).
        # On conserve l'ordre pour le DnD, mais on ne pré-remplit plus _instances
        # afin de n'afficher par défaut que les instances définies dans Instances.json
        # (via la préférence "autoload_instances_file").
        self.instance_order: List[str] = list(
            _prefs.get("instances", {}).get("order", [])
        )

        # brancher le signal DnD
        self.list.orderChanged.connect(self.on_instances_reordered)
        self.list.itemSelectionChanged.connect(self.update_card_selection_styles)

        # fetch initial (placeholder) en arrière-plan au démarrage
        fetch_reference_prices_async(
            on_done=lambda: QTimer.singleShot(0, self.update_revenue_counter)
        )

        # self.refresh_list_full()
        self.update_card_selection_styles()
        self.update_instances_count()
        self.update_global_dot()
        self.update_autopilot_state_label()
        self.refresh_list_full()

        try:
            autoload_file = get_autoload_instances_file(_prefs)
            if autoload_file and os.path.exists(autoload_file):
                with open(autoload_file, "r", encoding="utf-8") as f:
                    autos = json.load(f)

                if isinstance(autos, list):
                    for inst_data in autos:
                        title = inst_data.get("title")
                        ctrl = inst_data.get("controller")

                        if not title or not ctrl:
                            continue

                        with _state_lock:
                            # si déjà présent, on SKIP pour éviter doublons
                            if title in _instances:
                                continue

                        inst = InstanceState(title)
                        inst.controller_path = ctrl

                        # Force état ARRETÉ propre
                        inst.pid = None
                        inst.last_heartbeat = 0
                        inst.stopped = True  # <--- IMPORTANT : couleur = gris
                        inst.awaiting_first_hb = False  # <--- sinon jaune
                        inst.restored_recently = False  # <--- évite violet au lancement

                        # On enregistre l'instance
                        _instances[title] = inst

                        # création du widget visuel (sur thread GUI)
                        QTimer.singleShot(
                            0, lambda t=title, i=inst: self._ensure_item(t, i)
                        )

                QTimer.singleShot(
                    0,
                    lambda: (
                        self.refresh_list_full(),
                        self.update_global_dot(),
                        self.update_instances_count(),
                    ),
                )

        except Exception as e:
            print("Autoload instances failed:", e)

        # Démarrage du bot Discord si activé dans les préférences
        try:
            global _discord_bot
            discord_bot_cfg = _prefs.get("discord_bot", {})
            if discord_bot_cfg.get("enabled", False):
                token = discord_bot_cfg.get("token", "")
                channel_id = discord_bot_cfg.get("channel_id", 0)

                if token and channel_id:
                    _discord_bot.start(token, channel_id)
                    app_log_info(f"Bot Discord démarré : channel_id={channel_id}")
                else:
                    app_log_warn(
                        "Bot Discord activé mais token ou channel_id manquant dans les préférences"
                    )
        except Exception as e:
            app_log_error(f"Erreur lors du démarrage du bot Discord: {e}")

    def eventFilter(self, obj, event):
        if obj is self.list and event.type() == QEvent.Resize:
            self._adjust_item_widths()
        return super().eventFilter(obj, event)

    def on_instance_rename(self):
        """Renomme une instance lorsqu'on modifie le champ 'Titre' dans le panneau de détails."""
        if not hasattr(self, "edit_title"):
            return

        new_title = self.edit_title.text().strip()
        if not new_title:
            return

        old_title = getattr(self, "current_instance_title", None)
        if not old_title or new_title == old_title:
            return

        # Ne jamais afficher de QMessageBox sous _state_lock (deadlock/crash)
        with _state_lock:
            inst = _instances.pop(old_title, None)
        if not inst:
            QMessageBox.warning(
                self, "Erreur", f"L'instance '{old_title}' n'existe plus."
            )
            return

        with _state_lock:
            new_title_exists = new_title in _instances
        if new_title_exists:
            with _state_lock:
                _instances[old_title] = inst
            QMessageBox.warning(
                self, "Erreur", f"Une instance '{new_title}' existe déjà."
            )
            self.edit_title.setText(old_title)
            return

        # 1) maj instance + ordre
        with _state_lock:
            inst.title = new_title
            _instances[new_title] = inst
            self.current_instance_title = new_title
            try:
                if hasattr(self, "instance_order") and self.instance_order:
                    self.instance_order = [
                        new_title if t == old_title else t for t in self.instance_order
                    ]
                    _prefs.setdefault("instances", {})["order"] = list(
                        self.instance_order
                    )
                    save_prefs(_prefs)
            except Exception:
                pass

        # 2) rafraîchir la liste + resélectionner le nouvel item
        self.refresh_list_full()
        try:
            it = self._get_item_by_title(new_title)
            if it:
                self.list.setCurrentItem(it)
        except Exception:
            pass

        self.update_selected_details()
        app_log_info(f"Instance renommée : {old_title} -> {new_title}")

    def _on_revenue_updated(self):
        """Handler appelé quand _revenue_data a changé (Flask ou fetch)."""
        try:
            # recharge la référence locale (pointer vers le store global)
            with _revenue_lock:
                self.revenue_data = _revenue_data
            # met à jour le compteur (UI thread)
            QTimer.singleShot(0, self.update_revenue_counter)
        except Exception:
            pass

    def _update_card_buttons_state(
        self, title: str, inst: InstanceState, card_widget: QWidget
    ):
        """Active/désactive les boutons de la carte selon l'état courant."""
        try:
            running = self._is_instance_running(inst)
            # bouton "Relancer" de la carte
            if hasattr(card_widget, "btn_reload") and card_widget.btn_reload:
                card_widget.btn_reload.setEnabled(not running)
                card_widget.btn_reload.setToolTip(
                    "L'instance tourne déjà (heartbeat/PID/HWND OK)"
                    if running
                    else "Relancer l'instance arrêtée"
                )
        except Exception:
            pass

    def _reset_instance_order(self):
        """Vide l'ordre des instances côté mémoire + prefs, puis persiste."""
        try:
            self.instance_order.clear()  # vide la liste en mémoire
        except AttributeError:
            self.instance_order = []

        _prefs.setdefault("instances", {})["order"] = []
        save_prefs(_prefs)

    def _adjust_item_widths(self):
        for i in range(self.list.count()):
            it = self.list.item(i)
            if it:
                it.setSizeHint(QSize(CARD_WIDTH, CARD_HEIGHT))

    @staticmethod
    def fmt_last_update(ts: float, with_label: bool = False) -> str:
        if not ts or ts <= 0:
            return "Last Update : -" if with_label else "-"
        try:
            hhmm = datetime.fromtimestamp(ts).strftime("%H:%M")
            return f"Last Update : {hhmm}" if with_label else hhmm
        except Exception:
            return "Last Update : -" if with_label else "-"

    def _age_to_red(self, age_s: float, stopped: bool) -> bool:
        if stopped:
            return False
        return age_s >= HEARTBEAT_RED_S

    def instance_color(self, inst: InstanceState) -> str:
        # instance "vide" : jaune pendant attente fenêtre, bleu une fois détectée, gris si stoppée
        if getattr(inst, "manual_empty", False):
            if inst.stopped:
                return CLR_GREY
            if getattr(inst, "awaiting_first_hb", False):
                return CLR_YELLOW
            return CLR_BLUE

        if inst.stopped:
            return CLR_GREY
        # restored_recently doit primer pour montrer violet
        if getattr(inst, "restored_recently", False):
            return CLR_PURPLE
        if inst.awaiting_first_hb:
            return CLR_YELLOW
        now = time.time()
        age_main = (now - inst.last_heartbeat) if inst.last_heartbeat else 1e9
        worst_red = self._age_to_red(age_main, inst.stopped)
        # sub_map values are dicts {"alias":..., "ts": ...}
        for _, info in inst.sub_map.items():
            try:
                ts = (
                    float(info.get("ts"))
                    if isinstance(info, dict) and info.get("ts") is not None
                    else float(info)
                )
            except Exception:
                ts = 0.0
            age = (now - ts) if ts else 1e9
            if self._age_to_red(age, inst.stopped):
                worst_red = True
                break
        return CLR_RED if worst_red else CLR_GREEN

    def sub_status_color(self, last_ts: float, stopped: bool) -> str:
        if stopped:
            return CLR_GREY
        now = time.time()
        age = (now - last_ts) if last_ts else 1e9
        return CLR_RED if self._age_to_red(age, stopped) else CLR_GREEN

    def _is_instance_running(self, inst: InstanceState) -> bool:
        """Considère l'instance 'en cours' si (non stoppée) et (PID/HWND vivants ou HB récent)."""
        try:
            if inst.stopped:
                return False
            pid_ok = is_pid_alive(inst.pid)
            hwnd_ok = is_hwnd_valid(inst.hwnd)
            hb_ok = False
            if inst.last_heartbeat and inst.last_heartbeat > 0:
                hb_ok = (time.time() - float(inst.last_heartbeat)) < float(
                    HEARTBEAT_RED_S
                )
            return pid_ok or hwnd_ok or hb_ok
        except Exception:
            return False

    def instance_severity(self, inst: InstanceState) -> int:
        if inst.stopped:
            return 3
        if inst.awaiting_first_hb:
            return 1
        now = time.time()
        age_main = (now - inst.last_heartbeat) if inst.last_heartbeat else 1e9
        if self._age_to_red(age_main, inst.stopped):
            return 2
        for _, info in inst.sub_map.items():
            try:
                ts = (
                    float(info.get("ts"))
                    if isinstance(info, dict) and info.get("ts") is not None
                    else float(info)
                )
            except Exception:
                ts = 0.0
            age = (now - ts) if ts else 1e9
            if self._age_to_red(age, inst.stopped):
                return 2
        return 0

    def global_status_color(self) -> str:
        with _state_lock:
            if not _instances:
                return CLR_GREY
            any_red = False
            any_yellow = False
            all_grey = True
            has_non_empty_instance = (
                False  # Tracker si on a au moins une instance non-vide
            )
            for inst in _instances.values():
                # Ignorer les instances vides (voyant bleu) pour le calcul du statut global
                if getattr(inst, "manual_empty", False):
                    continue
                has_non_empty_instance = True
                sev = self.instance_severity(inst)
                if sev != 3:
                    all_grey = False
                if sev == 2:
                    any_red = True
                    break
                elif sev == 1:
                    any_yellow = True
            # Si toutes les instances sont vides, retourner gris
            if not has_non_empty_instance:
                return CLR_GREY
            if any_red:
                return CLR_RED
            if any_yellow:
                return CLR_YELLOW
            if all_grey:
                return CLR_GREY
            return CLR_GREEN

    def update_global_dot(self):
        """
        Met à jour le voyant global AnkaMaster.
        Si le voyant passe au ROUGE, déclenche une alerte (flash + notif).
        Si le voyant passe au VERT (depuis ROUGE uniquement), déclenche l'action Discord.
        Note : Le passage JAUNE → VERT ne déclenche rien.
        """
        try:
            new_color = self.global_status_color()
        except Exception:
            # Très défensif : ne jamais casser la boucle d'update
            try:
                self.global_dot.set_color(self.global_status_color())
            except Exception:
                pass
            return

        # Applique toujours la nouvelle couleur
        try:
            self.global_dot.set_color(new_color)
        except Exception:
            pass

        old_color = getattr(self, "_last_global_color", CLR_GREY)
        first = getattr(self, "_first_global_dot_update", False)

        # Transition vers ROUGE (on ignore la toute première mise à jour au démarrage)
        if (not first) and new_color == CLR_RED and old_color != CLR_RED:
            try:
                self._on_global_red_alert()
            except Exception as e:
                print(f"[ALERT] erreur alerte rouge globale: {e}")

        # Transition vers VERT (UNIQUEMENT depuis ROUGE) - Action Discord
        # Note : Le passage JAUNE → VERT ne déclenche RIEN
        if (not first) and new_color == CLR_GREEN and old_color == CLR_RED:
            app_log_info(
                f"🔄 Transition ROUGE → VERT détectée (old={old_color}, new={new_color}, first={first})"
            )
            try:
                self._on_global_green_recovery()
            except Exception as e:
                app_log_error(f"[RECOVERY] erreur lors de la récupération verte: {e}")
        elif new_color == CLR_GREEN and old_color != CLR_RED and not first:
            app_log_info(
                f"ℹ️ Voyant passe au VERT depuis {old_color} (pas ROUGE) → Pas d'action Discord"
            )

        self._last_global_color = new_color
        self._first_global_dot_update = False

    def _on_global_red_alert(self):
        """
        Appelé une seule fois à chaque transition du voyant global vers ROUGE.
        - Si la fenêtre AnkaMaster n'est pas active, on fait clignoter le bouton
          dans la barre des tâches via FlashWindowEx.
        - On affiche également une notification système (systray) si possible.
        """
        # 1) Flash dans la barre des tâches si la fenêtre n'a pas le focus
        try:
            if not self.isActiveWindow():
                try:
                    hwnd = int(self.winId())
                except Exception:
                    hwnd = 0
                if hwnd:
                    flash_master_window(hwnd)
        except Exception as e:
            print(f"[ALERT] flash window failed: {e}")

        # 2) Notification systray (optionnelle)
        try:
            if getattr(self, "_tray_icon", None) is not None:
                self._tray_icon.showMessage(
                    f"{APP_DISPLAY_NAME} - alerte",
                    "Au moins une instance est en ROUGE (plus de heartbeat / sous-contrôleur).",
                    QSystemTrayIcon.Warning,
                    5000,  # ms
                )
        except Exception as e:
            print(f"[ALERT] systray notification failed: {e}")

        # 3) Envoi optionnel d'un webhook Discord (simple)
        try:
            if getattr(self, "discord_alert_enabled", False):
                send_discord_hook()
        except Exception as e:
            print(f"[ALERT] discord hook failed: {e}")

        # 4) Envoi de l'alerte via le bot Discord (si activé)
        # Permet au bot d'éditer le message plus tard (un bot ne peut éditer que ses propres messages)
        try:
            global _discord_bot
            discord_bot_enabled = _prefs.get("discord_bot", {}).get("enabled", False)

            if discord_bot_enabled and _discord_bot.is_running:
                # Collecter les instances en rouge pour l'alerte
                red_instances = []
                now_ts = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                now = time.time()

                try:
                    with _state_lock:
                        for title, inst in _instances.items():
                            if inst.stopped:
                                continue
                            age_main = (
                                (now - inst.last_heartbeat)
                                if inst.last_heartbeat
                                else 1e9
                            )
                            is_red = age_main >= HEARTBEAT_RED_S
                            if not is_red:
                                for _, info in inst.sub_map.items():
                                    try:
                                        ts = (
                                            float(info.get("ts"))
                                            if isinstance(info, dict)
                                            and info.get("ts") is not None
                                            else float(info)
                                        )
                                    except Exception:
                                        ts = 0.0
                                    age = (now - ts) if ts else 1e9
                                    if age >= HEARTBEAT_RED_S:
                                        is_red = True
                                        break
                            if is_red:
                                red_instances.append(title)
                except Exception:
                    pass

                red_count = len(red_instances)
                instances_list = (
                    ", ".join(red_instances[:20]) if red_instances else "Aucune"
                )
                if len(red_instances) > 20:
                    instances_list += f" ... (+{len(red_instances)-20} autres)"

                # Envoyer l'alerte via le bot
                _discord_bot.send_red_alert(red_count, instances_list, now_ts)
        except Exception as e:
            app_log_error(f"Erreur lors de l'envoi de l'alerte via le bot Discord: {e}")

    def _on_global_green_recovery(self):
        """
        Appelé une seule fois à chaque transition du voyant global vers VERT.
        Vérifie d'abord si le dernier message Discord est une alerte rouge avant d'agir.
        """
        global _discord_bot
        global _prefs

        app_log_info("🟢 Transition vers VERT détectée, vérification Discord...")

        try:
            if getattr(self, "discord_alert_enabled", False):
                send_discord_hook_success()
        except Exception as e:
            print(f"[ALERT] discord hook failed: {e}")

        # try:
        #     # Vérifier si le bot Discord est activé dans les préférences
        #     discord_bot_enabled = _prefs.get("discord_bot", {}).get("enabled", False)
        #     app_log_info(f"   discord_bot_enabled = {discord_bot_enabled}")

        #     if _discord_bot:
        #         app_log_info(f"   _discord_bot.is_running = {_discord_bot.is_running}")
        #     else:
        #         app_log_info(f"   _discord_bot = None")

        #     if discord_bot_enabled and _discord_bot and _discord_bot.is_running:
        #         # Récupérer le mode de récupération (par défaut: "edit_and_notify")
        #         recovery_mode = _prefs.get("discord_bot", {}).get("recovery_mode", "edit_and_notify")
        #         limit = _prefs.get("discord_bot", {}).get("clear_limit", 100)

        #         app_log_info(f"✅ Conditions remplies, appel de check_and_resolve_alert (mode={recovery_mode})")

        #         # NOUVELLE LOGIQUE : Vérifier d'abord si le dernier message est une alerte rouge
        #         _discord_bot.check_and_resolve_alert(recovery_mode, limit)

        #     else:
        #         app_log_info("❌ Bot Discord non activé ou non démarré, pas d'action")

        # except Exception as e:
        #     app_log_error(f"❌ Erreur lors de la récupération verte: {e}")
        #     import traceback
        #     app_log_error(f"   Traceback: {traceback.format_exc()}")

    def update_instances_count(self):
        """Met à jour le grand label 'Instances' avec total + actives."""
        try:
            with _state_lock:
                instances_snapshot = list(_instances.values())
        except Exception:
            instances_snapshot = []

        active = 0
        for inst in instances_snapshot:
            try:
                if self._is_instance_running(inst):
                    active += 1
            except Exception:
                continue

        active_label = "instance" if active == 1 else "instances"
        txt = f"{active} {active_label}"

        try:
            self.lbl_instances_big.setText(txt)
        except Exception:
            pass

    def update_revenue_counter(self):
        """Calcule la somme totale en € : (TS + Métier) * prix(serveur) sur tous les serveurs."""
        try:
            with _revenue_lock:
                servers = (
                    _revenue_data.get("servers") or {}
                )  # {"Imagiro": 8.25, ...} = €/M
                holds = _revenue_data.get("holdings") or {"TS": {}, "M": {}}  # millions

            total = 0.0
            for srv, price in servers.items():
                p = float(price)
                qty = int(holds.get("TS", {}).get(srv, 0)) + int(
                    holds.get("M", {}).get(srv, 0)
                )
                total += qty * p

            self.panel_euros.setText(f"{total:.2f} €")
        except Exception:
            self.panel_euros.setText("0 €")

    def open_revenue_window(self):
        """Ouvre la fenêtre de détails €."""
        try:
            dlg = RevenueDialog(self, self.revenue_data)
            dlg.exec()
        except Exception as e:
            QMessageBox.warning(
                self, "Erreur", f"Impossible d'ouvrir la fenêtre €:\n{e}"
            )

    def _get_item_by_title(self, title: str) -> Optional[QListWidgetItem]:
        for i in range(self.list.count()):
            it = self.list.item(i)
            if it.data(Qt.UserRole) == title:
                return it
        return None

    def _ensure_item(self, title: str, inst: InstanceState):
        it = self._get_item_by_title(title)
        if it is None:
            it = QListWidgetItem()
            it.setData(Qt.UserRole, title)
            it.setSizeHint(QSize(CARD_WIDTH, CARD_HEIGHT))

            card = InstanceItemWidget(title=inst.title)

            # def _bind_click(ev, _it=it, _card=card):
            #     if ev.button() == Qt.LeftButton:
            #         sm  = self.list.selectionModel()
            #         idx = self.list.indexFromItem(_it)
            #         mods = QApplication.keyboardModifiers()

            #         if mods & Qt.ControlModifier:
            #             # Toggle uniquement, ne touche pas aux autres
            #             sm.select(idx, QItemSelectionModel.Toggle)
            #             # on change l'item courant sans toucher à la sélection existante
            #             self.list.setCurrentItem(_it, QItemSelectionModel.NoUpdate)

            #         elif mods & Qt.ShiftModifier:
            #             # Sélection en plage, on ajoute sans effacer l’existant
            #             cur = self.list.currentRow()
            #             new = self.list.row(_it)
            #             if cur < 0:
            #                 self.list.setCurrentItem(_it)
            #                 sm.select(idx, QItemSelectionModel.Select)
            #             else:
            #                 start, end = sorted((cur, new))
            #                 for r in range(start, end + 1):
            #                     sm.select(self.list.indexFromItem(self.list.item(r)),
            #                             QItemSelectionModel.Select)
            #                 self.list.setCurrentRow(new)

            #         else:
            #             # Clic simple : Clear + Select
            #             self.list.clearSelection()
            #             sm.select(idx, QItemSelectionModel.ClearAndSelect)
            #             self.list.setCurrentItem(_it)

            #         # >>> INDISPENSABLE : on consomme l’event pour éviter que Qt re-traite la sélection
            #         ev.accept()

            #         # Mets à jour visuel + panneau détails tout de suite
            #         self.update_card_selection_styles()
            #         self.update_selected_details()
            #         return

            #     # Autres boutons : comportement par défaut
            #     QWidget.mousePressEvent(_card, ev)

            # def _bind_click(ev, _it=it, _card=card, self=self):
            #     # CLIC GAUCHE sur la carte :
            #     # -> on délègue TOTALEMENT à la QListWidget pour qu’elle gère
            #     #    sélection (Ctrl/Shift OK) + drag&drop (InternalMove).
            #     if ev.button() == Qt.LeftButton:
            #         # Position du clic convertie dans les coords de la viewport de la liste
            #         vp_pos = self.list.viewport().mapFromGlobal(_card.mapToGlobal(ev.pos()))
            #         forwarded = QMouseEvent(
            #             ev.type(), vp_pos, ev.globalPosition().toPoint(),
            #             ev.button(), ev.buttons(), ev.modifiers()
            #         )
            #         # On envoie l’event à la viewport de la QListWidget
            #         QCoreApplication.sendEvent(self.list.viewport(), forwarded)
            #         return  # surtout pas ev.accept(), on laisse Qt poursuivre le flux

            #     # Pour les autres boutons (droit, milieu...), comportement par défaut du widget
            #     QWidget.mousePressEvent(_card, ev)

            def _bind_click(ev, _it=it, _card=card, self=self):
                if ev.button() == Qt.LeftButton:
                    # 1) Sélection immédiate (sans attendre le cycle Qt)
                    mods = ev.modifiers()
                    sm = self.list.selectionModel()
                    idx = self.list.indexFromItem(_it)

                    if mods & Qt.ControlModifier:
                        # Toggle immédiat
                        if _it.isSelected():
                            sm.select(idx, QItemSelectionModel.Deselect)
                        else:
                            sm.select(idx, QItemSelectionModel.Select)
                        self.list.setCurrentItem(_it, QItemSelectionModel.NoUpdate)

                    elif mods & Qt.ShiftModifier:
                        # Sélection en plage (on ajoute sans effacer)
                        cur = self.list.currentRow()
                        new = self.list.row(_it)
                        if cur < 0:
                            self.list.setCurrentItem(_it)
                            sm.select(idx, QItemSelectionModel.Select)
                        else:
                            start, end = sorted((cur, new))
                            for r in range(start, end + 1):
                                sm.select(
                                    self.list.indexFromItem(self.list.item(r)),
                                    QItemSelectionModel.Select,
                                )
                            self.list.setCurrentRow(new)
                    else:
                        # Clic simple : Clear + Select
                        self.list.clearSelection()
                        sm.select(idx, QItemSelectionModel.ClearAndSelect)
                        self.list.setCurrentItem(_it)

                    # 2) MAJ immédiate du style et du panneau de détails
                    self.update_card_selection_styles()
                    self.update_selected_details()

                    # 3) Forward à la viewport pour laisser Qt gérer un éventuel DnD
                    vp_pos = self.list.viewport().mapFromGlobal(
                        _card.mapToGlobal(ev.pos())
                    )
                    forwarded = QMouseEvent(
                        ev.type(),
                        vp_pos,
                        ev.globalPosition().toPoint(),
                        ev.button(),
                        ev.buttons(),
                        ev.modifiers(),
                    )
                    QCoreApplication.sendEvent(self.list.viewport(), forwarded)
                    return  # très important : on coupe ici

                # Boutons non gauche : comportement par défaut
                QWidget.mousePressEvent(_card, ev)

            card.mousePressEvent = _bind_click
            card.requestFocus.connect(self.on_card_focus)
            card.requestRelaunch.connect(self.on_card_relaunch)
            card.requestKill.connect(self.on_card_kill)
            card.requestDelete.connect(self.on_card_delete)

            self.list.addItem(it)
            self.list.setItemWidget(it, card)

        card: InstanceItemWidget = self.list.itemWidget(it)
        if card:
            card.set_title(inst.title)
            color = self.instance_color(inst)
            extra = self.fmt_last_update(inst.last_heartbeat, with_label=True)
            card.update_status(color, extra)
            self._update_card_buttons_state(title, inst, card)

        if title not in self.instance_order:
            self.instance_order.append(title)
            _prefs.setdefault("instances", {})["order"] = list(self.instance_order)
            save_prefs(_prefs)

        self.update_card_selection_styles()

    def _ordered_titles_snapshot(self) -> List[str]:
        """Compose l’ordre final : prefs d’abord, puis titres restants."""
        with _state_lock:
            existing = list(_instances.keys())
        ordered = [t for t in self.instance_order if t in existing]
        for t in existing:
            if t not in ordered:
                ordered.append(t)
        return ordered

    def _refresh_one_card(self, title: str):
        """Rafraîchit l'affichage d'une seule carte (voyant + texte)."""
        try:
            with _state_lock:
                inst = _instances.get(title)
            if inst:
                self._ensure_item(title, inst)
                self.update_card_selection_styles()
        except Exception:
            pass

    def refresh_list_full(self):
        self.list.clear()
        for title in self._ordered_titles_snapshot():
            with _state_lock:
                inst = _instances.get(title)
            if inst:
                self._ensure_item(title, inst)
                # mise à jour visuelle de la sélection
        for i in range(self.list.count()):
            it = self.list.item(i)
            w = self.list.itemWidget(it)
            if not w:
                continue
            w.set_selected(it.isSelected())
        self._adjust_item_widths()
        self.update_selected_details()
        self.update_sub_list()
        self.update_card_selection_styles()
        self.update_instances_count()
        self.update_global_dot()

    def refresh_cards_and_details(self):
        try:
            titles = []
            for i in range(self.list.count()):
                it = self.list.item(i)
                if not it:
                    continue
                t = it.data(Qt.UserRole)
                if t:
                    titles.append(t)

            instances_snapshot = []
            with _state_lock:
                for t in titles:
                    inst = _instances.get(t)
                    if inst:
                        instances_snapshot.append((t, inst))

            for title, inst in instances_snapshot:
                self._ensure_item(title, inst)

            self._adjust_item_widths()
            self.update_selected_details()
            self.update_sub_list()
            self.update_card_selection_styles()
            self.update_instances_count()
            self.update_global_dot()
            # Ancien système de relance auto désactivé - remplacé par _scan_pids_periodic (scan toutes les minutes)
            # self._schedule_auto_relaunch_scan(instances_snapshot)

        except Exception as e:
            print("Timer refresh error:", e)

    def selected_title(self) -> Optional[str]:
        it = self.list.currentItem()
        return it.data(Qt.UserRole) if it else None

    def selected_titles(self) -> list:
        # Récupère le titre stocké dans Qt.UserRole pour chaque item sélectionné
        return [it.data(Qt.UserRole) for it in self.list.selectedItems() if it]

    def on_select_instance(self, cur, prev):
        self.update_selected_details()
        self.update_sub_list()
        self.update_card_selection_styles()

    def update_card_selection_styles(self):
        cur = self.list.currentItem()
        for i in range(self.list.count()):
            it = self.list.item(i)
            card = self.list.itemWidget(it)
            if not card:
                continue
            # selected = (it is cur)
            selected = (
                it.isSelected()
            )  # <-- prend la vraie sélection, pas juste current
            card.setProperty("selected", selected)
            card.style().unpolish(card)
            card.style().polish(card)
            card.update()

    def update_selected_details(self):
        """Met à jour le panneau 'Détails de l’instance' selon l’instance sélectionnée."""
        # Protection contre les appels trop précoces (avant création des widgets)
        if not hasattr(self, "lbl_script"):
            return

        title = self.selected_title()
        self.selected_instance_title = title

        # Si aucune instance sélectionnée → on vide les champs
        if not title:
            if hasattr(self, "lbl_title"):
                self.lbl_title.setText("-")
            if hasattr(self, "edit_title"):
                self.edit_title.setText("")
            self.lbl_script.setText("-")
            self.lbl_pid.setText("-")
            self.lbl_status.setText("-")
            self.lbl_subs.setText("-")
            self.lbl_last_reset.setText("-")
            if hasattr(self, "btn_rename_instance"):
                self.btn_rename_instance.setEnabled(False)
            return

        # Récupération de l’instance
        with _state_lock:
            inst = _instances.get(title)
            if not inst:
                return

        # --- Mise à jour du titre ---
        if hasattr(self, "lbl_title"):
            self.lbl_title.setText(inst.title)
        if hasattr(self, "edit_title"):
            self.edit_title.blockSignals(True)
            self.edit_title.setText(inst.title)
            self.edit_title.setCursorPosition(len(inst.title))
            self.edit_title.blockSignals(False)

        # --- Script et PID ---
        script_name = (
            os.path.basename(inst.controller_path) if inst.controller_path else "-"
        )
        self.lbl_script.setText(script_name)
        self.lbl_pid.setText(str(inst.pid) if inst.pid else "-")

        # --- Last Reset ---
        try:
            last_reset = getattr(inst, "last_reset", 0.0)
            if not last_reset:
                self.lbl_last_reset.setText("-")
            else:
                self.lbl_last_reset.setText(
                    datetime.fromtimestamp(float(last_reset)).strftime("%H:%M")
                )
        except Exception:
            self.lbl_last_reset.setText("-")

        # --- Last Update ---
        self.lbl_status.setText(
            self.fmt_last_update(inst.last_heartbeat, with_label=False)
        )

        # --- État de l’instance ---
        try:
            running = self._is_instance_running(inst)
            self.btn_reload_d.setEnabled(not running)
            self.btn_reload_d.setToolTip(
                "L'instance tourne déjà (heartbeat/PID/HWND OK)"
                if running
                else "Relancer l'instance arrêtée"
            )
        except Exception:
            pass

        # --- Activation du bouton de renommage ---
        if hasattr(self, "btn_rename_instance"):
            self.btn_rename_instance.setEnabled(True)

    # def update_selected_details(self):
    #     title = self.selected_title()
    #     self.selected_instance_title = title  # mémorise le titre courant pour on_rename_instance()
    #     if not title:
    #         self.lbl_title.setText("-"); self.lbl_script.setText("-"); self.lbl_pid.setText("-")
    #         self.lbl_status.setText("-"); self.lbl_subs.setText("-")
    #         return
    #     with _state_lock:
    #         inst = _instances.get(title)
    #         if not inst: return
    #         self.lbl_title.setText(inst.title)
    #         script_name = os.path.basename(inst.controller_path) if inst.controller_path else "-"
    #         self.lbl_script.setText(script_name)
    #         self.lbl_pid.setText(str(inst.pid) if inst.pid else "-")

    #         # NOUVEAU : Last Reset
    #         try:
    #             self.lbl_last_reset.setText(
    #                 "-" if not getattr(inst, "last_reset", 0.0) else datetime.fromtimestamp(float(inst.last_reset)).strftime("%H:%M")
    #             )
    #         except Exception:
    #             self.lbl_last_reset.setText("-")

    #         # Last Update (comme avant)
    #         self.lbl_status.setText(self.fmt_last_update(inst.last_heartbeat, with_label=False))

    #         try:
    #             running = self._is_instance_running(inst)
    #             self.btn_reload_d.setEnabled(not running)
    #             self.btn_reload_d.setToolTip(
    #                 "L'instance tourne déjà (heartbeat/PID/HWND OK)" if running else "Relancer l'instance arrêtée"
    #             )
    #         except Exception:
    #             pass
    #         # self.lbl_subs.setText(", ".join(sorted((v.get("alias") or k) for k,v in inst.sub_map.items())) if inst.sub_map else "-")
    #         # if inst.sub_map:
    #         #     subs_list = []
    #         #     for k, v in inst.sub_map.items():
    #         #         alias_raw = (v.get("alias") or k)
    #         #         alias_clean = str(alias_raw).replace("_", ":")
    #         #         subs_list.append(alias_clean)
    #         #     self.lbl_subs.setText(", ".join(sorted(subs_list)))
    #         # else:
    #         #     self.lbl_subs.setText("-")

    def remove_subcontroller_widget(self, sid: str):
        """Supprime le widget sous-contrôleur pour self.selected_instance_title."""
        title = self.selected_instance_title
        if not title:
            return

        with _state_lock:
            inst = _instances.get(title)
            if not inst:
                return

            # Retirer du state interne
            if sid in inst.sub_map:
                inst.sub_map.pop(sid, None)

        # Retirer de la liste GUI
        for i in range(self.subs_list.count()):
            item = self.subs_list.item(i)
            w = self.subs_list.itemWidget(item)
            if hasattr(w, "sid") and w.sid == sid:
                self.subs_list.takeItem(i)
                break

        # Met à jour les voyants / états de l'instance
        try:
            bus.instance_updated.emit(title)
        except:
            pass

        # Refresh UI si nécessité
        self.update_selected_details()
        # self.refresh_instance_cards()
        self.refresh_cards_and_details()

    def update_sub_list(self):
        title = self.selected_title()
        self.subs_list.clear()
        if not title:
            return
        with _state_lock:
            inst = _instances.get(title)
            if not inst:
                return
            # sort by stable sid (string)
            items = sorted(inst.sub_map.items(), key=lambda kv: str(kv[0]).lower())
            for sid, info in items:
                it = QListWidgetItem()
                # larger item height so alias + id fit
                # it.setSizeHint(QSize(self.subs_list.viewport().width() - 10, 60))
                it.setSizeHint(QSize(self.subs_list.viewport().width() - 10, 80))
                try:
                    # info should be dict {"alias":..., "ts":...}
                    alias = str(sid)
                    ts = 0.0
                    if isinstance(info, dict):
                        # prefer stored alias (may change) but sid remains the key
                        alias = str(info.get("alias") or sid)
                        try:
                            ts = float(info.get("ts") or 0.0)
                        except Exception:
                            ts = 0.0
                    else:
                        try:
                            ts = float(info)
                        except Exception:
                            ts = 0.0

                    # create widget: alias shown bold, sid shown below (stable id)
                    w = SubctrlItemWidget(alias=alias, sid=sid, last_ts=ts)
                    # color the dot by recency
                    w.dot.set_color(self.sub_status_color(ts, inst.stopped))
                    # w.requestDelete.connect(self.remove_subcontroller_widget)
                    w.requestDelete.connect(
                        lambda sid, t=title: self.remove_subcontroller_widget(sid)
                    )

                    # # ensure the name label can wrap if long
                    # try:
                    #     w.lbl_name.setWordWrap(True)
                    # except Exception:
                    #     pass

                    self.subs_list.addItem(it)
                    self.subs_list.setItemWidget(it, w)
                except Exception:
                    # ignore faulty entries
                    continue

    def _maybe_auto_relaunch(self, title: str, inst: InstanceState):
        # ANCIEN SYSTÈME DÉSACTIVÉ - Remplacé par _scan_pids_periodic qui scanne toutes les minutes
        # Ce système était trop lent car il dépendait de la sévérité de l'instance (voyant rouge)
        # Le nouveau système vérifie directement les PID toutes les minutes, indépendamment du heartbeat
        pass

    def on_card_focus(self, title: str):
        if not title:
            return

        # --- première chose : sélectionner le widget correspondant dans la liste ---
        it = self._get_item_by_title(title)
        if it:
            try:
                self.list.setCurrentItem(it)
            except Exception:
                pass

        with _state_lock:
            inst = _instances.get(title)
            if not inst:
                return
            hwnd = inst.hwnd
            if (not hwnd) and inst.pid:
                try:
                    hwnd = get_main_hwnd(int(inst.pid))
                    inst.hwnd = hwnd
                except:
                    hwnd = None

        if inst and inst.hwnd:
            bring_to_front(inst.hwnd)

    def on_card_kill(self, title: str):
        if not title:
            return
        with _state_lock:
            inst = _instances.get(title)
        if inst and inst.pid:
            try:
                terminate_process_tree(int(inst.pid), timeout=3.0)
            except Exception:
                try:
                    psutil.Process(int(inst.pid)).terminate()
                except Exception:
                    pass
        # Utiliser _mark_instance_stopped pour garantir que tous les champs sont réinitialisés
        self._mark_instance_stopped(title)

        try:
            _post_json("/goodbye", {"title": title})
        except Exception:
            pass

        try:
            bus.instance_updated.emit(title)
        except Exception:
            pass
        QTimer.singleShot(
            0,
            lambda: (
                self.update_sub_list(),
                self.update_selected_details(),
                self.update_global_dot(),
            ),
        )

    def on_card_delete(self, title: str):
        if not title:
            return

        # tenter d'arrêter proprement (best-effort) via la méthode existante
        try:
            self.on_card_kill(title)
        except Exception:
            pass

        # supprimer de l'état global
        with _state_lock:
            if title in _instances:
                del _instances[title]
                # NEW: si on supprime la carte sélectionnée, on vide le panneau subs
                try:
                    if self.selected_title() == title:
                        self.subs_list.clear()
                except Exception:
                    pass

        # supprimer du widget list
        it = self._get_item_by_title(title)
        if it:
            row = self.list.row(it)
            self.list.takeItem(row)

        # mettre à jour les panneaux/details
        self.update_selected_details()
        self.update_sub_list()
        self.update_global_dot()
        self.update_instances_count()

    # def _spawn_runner_thread(self, exe, controller, images, ratio, title):
    #     t = threading.Thread(target=run_snowbot_flow, args=(exe, controller, title, images, ratio), daemon=True)
    #     t.start()
    def _spawn_runner_thread(self, exe, controller, images, ratio, title):
        # smid = str(uuid.uuid4())
        args = [f"--title={title}", f"--controller={controller}"]
        t = threading.Thread(
            target=run_snowbot_flow,
            args=(exe, controller, title, images, ratio, args),
            daemon=True,
        )
        t.start()

    def on_card_relaunch_force(self, title: str, controller_path: str):
        """Relance forcée d'une instance sans vérifier si elle est déjà running.
        Utilisé par le scan auto-relaunch après détection de crash."""
        # print(f"[DEBUG on_card_relaunch_force] {title}: DÉBUT avec controller={controller_path}")
        if not title:
            # print(f"[DEBUG on_card_relaunch_force] {title}: title vide, return")
            return

        inst, controller, exe, images, ratio = self._get_instance_launch_params(title)
        # print(f"[DEBUG on_card_relaunch_force] {title}: inst={inst}")
        if not inst:
            # print(f"[DEBUG on_card_relaunch_force] {title}: inst None, return")
            return

        # Utiliser le controller passé en paramètre (celui détecté lors du crash)
        if controller_path:
            controller = controller_path

        if not controller:
            print(f"[DEBUG on_card_relaunch_force] {title}: pas de controller, ABORT")
            return

        # Killer les processus dupliqués si nécessaire
        if self._enforce_unique_title_processes(title):
            # print(f"[DEBUG on_card_relaunch_force] {title}: processus dupliqués tués")
            inst, controller, exe, images, ratio = self._get_instance_launch_params(
                title
            )
            if not inst:
                # print(f"[DEBUG on_card_relaunch_force] {title}: inst None après enforce, return")
                return

        # PAS de check _is_instance_running - on force la relance
        # print(f"[DEBUG on_card_relaunch_force] {title}: Marquage awaiting_first_hb=True, stopped=False")

        # Marquer l'instance comme "en attente du premier heartbeat" (voyant jaune)
        with _state_lock:
            inst2 = _instances.get(title)
            if inst2:
                inst2.awaiting_first_hb = True
                inst2.stopped = False
                inst2.last_heartbeat = 0.0
                inst2.pid = None  # Reset PID
                inst2.hwnd = None  # Reset HWND
                _instances[title] = inst2
        try:
            bus.instance_updated.emit(title)
        except Exception:
            pass

        # Forcer la mise à jour immédiate de l'UI
        QTimer.singleShot(0, lambda: self.refresh_cards_and_details())

        # print(f"[DEBUG on_card_relaunch_force] {title}: Envoi /register")
        try:
            _post_json(
                "/register",
                {
                    "title": title,
                    "pid": 0,
                    "hwnd": 0,
                    "controller": controller,
                    "exe": exe,
                    "images": images,
                    "ratio": ratio,
                    "touch": True,
                },
            )
        except Exception:
            pass

        # print(f"[DEBUG on_card_relaunch_force] {title}: Appel _spawn_runner_thread")
        try:
            self._spawn_runner_thread(exe, controller, images, ratio, title)
            # print(f"[DEBUG on_card_relaunch_force] {title}: _spawn_runner_thread terminé, FIN")
        except Exception as e:
            # print(f"[DEBUG on_card_relaunch_force] {title}: ERREUR dans _spawn_runner_thread: {e}")
            import traceback

            traceback.print_exc()

    def on_card_relaunch(self, title: str):
        # print(f"[DEBUG on_card_relaunch] {title}: DÉBUT")
        if not title:
            # print(f"[DEBUG on_card_relaunch] {title}: title vide, return")
            return
        inst, controller, exe, images, ratio = self._get_instance_launch_params(title)
        # print(f"[DEBUG on_card_relaunch] {title}: inst={inst}, controller={controller}")
        if not inst:
            # print(f"[DEBUG on_card_relaunch] {title}: inst None, return")
            return

        if self._enforce_unique_title_processes(title):
            # print(f"[DEBUG on_card_relaunch] {title}: enforce_unique trouvé des doublons, re-fetch params")
            inst, controller, exe, images, ratio = self._get_instance_launch_params(
                title
            )
            if not inst:
                # print(f"[DEBUG on_card_relaunch] {title}: inst None après enforce, return")
                return

        is_running = self._is_instance_running(inst)
        # print(f"[DEBUG on_card_relaunch] {title}: _is_instance_running={is_running}, inst.stopped={inst.stopped}, inst.pid={inst.pid}, inst.hwnd={inst.hwnd}")
        if is_running:
            # print(f"[DEBUG on_card_relaunch] {title}: instance considérée running, ABORT relance")
            return
        # --- Si c'est une "instance vide" lancée manuellement, on relance sans demander de controller ---
        if getattr(inst, "manual_empty", False):
            with _state_lock:
                inst.stopped = False
                inst.awaiting_first_hb = True
                inst.last_heartbeat = time.time()
                _instances[title] = inst
            try:
                bus.instance_updated.emit(title)
            except Exception:
                pass

            def _run_and_monitor_empty():
                _log_empty(f"[{title}] Relance instance vide exe={exe!r}")
                try:
                    cmd = [exe, f"--title={title}", "--empty"]
                    exe_dir = os.path.dirname(os.path.abspath(exe)) if exe else None
                    p = subprocess.Popen(
                        cmd,
                        creationflags=CREATE_NEW_CONSOLE,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                        text=False,
                        cwd=exe_dir,
                    )
                except Exception as e:
                    _log_empty(f"[{title}] Relance Popen ERREUR: {e}")
                    with _state_lock:
                        inst.stopped = True
                        inst.awaiting_first_hb = False
                        inst.last_heartbeat = 0.0
                        _instances[title] = inst
                    try:
                        bus.instance_updated.emit(title)
                    except Exception:
                        pass
                    return

                start_ts = time.time()
                with _state_lock:
                    try:
                        inst.pid = int(p.pid)
                    except Exception:
                        inst.pid = inst.pid
                    _instances[title] = inst
                try:
                    bus.instance_updated.emit(title)
                except Exception:
                    pass

                try:
                    main_hwnd, main_pid = wait_for_large_window_for_process(
                        p,
                        start_ts,
                        total_timeout=120.0,
                        min_screen_ratio=0.5,
                        poll_interval=0.25,
                        log_progress=True,
                        small_title=title,
                    )
                    try:
                        _post_json(
                            "/register",
                            {
                                "title": title,
                                "pid": int(main_pid) if main_pid else 0,
                                "hwnd": int(main_hwnd),
                                "touch": True,
                            },
                        )
                    except Exception:
                        pass
                    _set_empty_instance_window_title(main_hwnd, title, log_prefix=title)
                    with _state_lock:
                        inst.awaiting_first_hb = False
                        _instances[title] = inst
                    try:
                        bus.instance_updated.emit(title)
                    except Exception:
                        pass
                except Exception as e:
                    _log_empty(f"[{title}] Relance wait_for_large_window: {e}")

                p.wait()
                with _state_lock:
                    inst.stopped = True
                    inst.awaiting_first_hb = False
                    inst.last_heartbeat = 0.0
                    try:
                        inst.pid = None
                    except Exception:
                        pass
                    _instances[title] = inst
                try:
                    bus.instance_updated.emit(title)
                except Exception:
                    pass

            threading.Thread(target=_run_and_monitor_empty, daemon=True).start()
            return

        # --- Comportement existant pour les instances "normales" ---
        # print(f"[DEBUG on_card_relaunch] {title}: controller={controller}")
        if not controller:
            # print(f"[DEBUG on_card_relaunch] {title}: pas de controller, demande à l'utilisateur")
            # Dernier recours : demander à l'utilisateur
            controller, _ = QFileDialog.getOpenFileName(
                self,
                "Sélectionner le contrôleur principal",
                "",
                "Exécutables / Scripts (*.exe *.bat *.cmd *.py *.lua);;Tous (*.*)",
            )
            if not controller:
                # print(f"[DEBUG on_card_relaunch] {title}: utilisateur a annulé, return")
                return
            with _state_lock:
                inst.controller_path = controller
                _instances[title] = inst

        # print(f"[DEBUG on_card_relaunch] {title}: Marquage awaiting_first_hb=True, stopped=False")
        # Marquer l'instance comme "en attente du premier heartbeat" (voyant jaune)
        with _state_lock:
            inst2 = _instances.get(title)
            if inst2:
                inst2.awaiting_first_hb = True
                inst2.stopped = False
                inst2.last_heartbeat = 0.0  # Réinitialiser pour forcer le voyant jaune
                _instances[title] = inst2
        try:
            bus.instance_updated.emit(title)
        except Exception:
            pass
        # Forcer la mise à jour immédiate de l'UI pour afficher le voyant jaune
        QTimer.singleShot(0, lambda: self.refresh_cards_and_details())

        print(f"[DEBUG on_card_relaunch] {title}: Envoi /register")
        try:
            _post_json(
                "/register",
                {
                    "title": title,
                    "pid": inst.pid or 0,
                    "hwnd": inst.hwnd or 0,
                    "controller": controller,
                    "exe": exe,
                    "images": images,
                    "ratio": ratio,
                    "touch": True,
                },
            )
        except Exception:
            pass
        print(f"[DEBUG on_card_relaunch] {title}: Appel _spawn_runner_thread")
        try:
            # Launch integrated runner thread
            self._spawn_runner_thread(exe, controller, images, ratio, title)
            print(
                f"[DEBUG on_card_relaunch] {title}: _spawn_runner_thread terminé, FIN de on_card_relaunch"
            )
        except Exception as e:
            print(
                f"[DEBUG on_card_relaunch] {title}: ERREUR dans _spawn_runner_thread: {e}"
            )
            import traceback

            traceback.print_exc()

    def _ask(self, title: str, text: str) -> bool:
        res = QMessageBox.question(
            self, title, text, QMessageBox.Yes | QMessageBox.No, QMessageBox.No
        )
        return res == QMessageBox.Yes

    def on_bulk_reload(self):
        with _state_lock:
            # titles = list(_instances.keys())
            titles = self.selected_titles()
        if not titles:
            return
        if not self._ask(
            "Relancer toutes les instances",
            f"Voulez-vous relancer {len(titles)} instance(s) ?",
        ):
            return

        # récupérer délai (en secondes)
        try:
            delay_s = int(self.spin_launch_delay.value())
        except Exception:
            delay_s = 0

        for idx, t in enumerate(titles):
            # planifie chaque relance espacée par delay_s
            delay_ms = int(idx * delay_s * 1000)
            QTimer.singleShot(delay_ms, lambda _t=t: self.on_card_relaunch(_t))

    def on_bulk_kill(self):
        with _state_lock:
            # titles = list(_instances.keys())
            titles = self.selected_titles()
        if not titles:
            return
        if not self._ask(
            "Terminer toutes les instances",
            f"Voulez-vous terminer {len(titles)} instance(s) ?",
        ):
            return
        for t in titles:
            self.on_card_kill(t)
        self.update_global_dot()

    def on_bulk_delete(self):
        with _state_lock:
            # titles = list(_instances.keys())
            titles = self.selected_titles()
        if not titles:
            return
        if not self._ask(
            "Supprimer toutes les instances",
            f"Voulez-vous TERMINER puis SUPPRIMER {len(titles)} instance(s) ?",
        ):
            return
        for t in titles:
            self.on_card_kill(t)
        time.sleep(0.2)
        for t in list(titles):
            self.on_card_delete(t)

        self._reset_instance_order()
        # (optionnel) forcer un refresh UI cohérent
        self.refresh_list_full()

    def on_panic_selected_instances(self):
        """
        Lance un flux 'PANIC TS' sur les instances actuellement sélectionnées :
        - utilise le PID/HWND existant de chaque instance
        - utilise le contrôleur global défini dans Settings.json (clé 'panic_controller')
        - sélectionne le 1er contrôleur dans la fenêtre et charge/lance ce script
        """
        # Si un cooldown est actif ou si aucun contrôleur PANIC global n'est configuré, on ne fait rien
        if self._panic_cooldown_active or not PANIC_CONTROLLER_PATH:
            return

        with _state_lock:
            titles = self.selected_titles()
            # Si aucune sélection, PANIC TS s'applique uniquement aux instances
            # dont le titre est de la forme "Serveur Numéro" (ex: "Imagiro 1")
            if not titles:
                import re

                pattern = re.compile(r"^[^\d]+?\s+\d+$")
                titles = [
                    t
                    for t in _instances.keys()
                    if isinstance(t, str) and pattern.match(t)
                ]
        if not titles:
            return

        # Démarrer le cooldown visuel + désactivation pendant 30 secondes
        self._start_panic_cooldown()

        for title in titles:
            with _state_lock:
                inst = _instances.get(title)
            if not inst or not inst.pid:
                continue

            t = threading.Thread(
                target=run_snowbot_flow_panic,
                args=(title,),
                daemon=True,
            )
            t.start()

    def _start_panic_cooldown(self):
        """Désactive le bouton PANIC TS pendant 30s avec un indicateur visuel."""
        try:
            self._panic_cooldown_active = True
            self.btn_panic.setEnabled(False)
            self.btn_panic.setText("PANIC TS (en cours...)")
            self.btn_panic.setStyleSheet(self._panic_style_cooldown)
        except Exception:
            pass

        # Réactiver au bout de 30 secondes
        QTimer.singleShot(30000, self._end_panic_cooldown)

    def _end_panic_cooldown(self):
        """Réactive le bouton PANIC TS et restaure son apparence."""
        try:
            self._panic_cooldown_active = False
            self.btn_panic.setEnabled(True)
            self.btn_panic.setText("PANIC TS")
            self.btn_panic.setStyleSheet(self._panic_style_normal)
        except Exception:
            pass

    def kill_and_clear_all(self):
        with _state_lock:
            titles = list(_instances.keys())
        if not titles:
            self.list.clear()
            self.update_selected_details()
            self.update_sub_list()
            self.update_global_dot()
            # self.update_instances_count()
            return
        for t in titles:
            try:
                self.on_card_kill(t)
            except Exception:
                pass
        time.sleep(0.3)
        for t in titles:
            try:
                self.on_card_delete(t)
            except Exception:
                pass
        self.update_global_dot()
        self.update_instances_count()

        self._reset_instance_order()
        self.refresh_list_full()

    def on_save_config(self):
        path, _ = QFileDialog.getSaveFileName(
            self,
            "Enregistrer configuration d'instances",
            "",
            "Configurations JSON (*.json);Tous (*.*)",
        )
        if not path:
            return
        cfg = []
        for title in self._ordered_titles_snapshot():
            with _state_lock:
                inst = _instances.get(title)
            if inst:
                cfg.append(
                    {
                        "title": inst.title,
                        "controller": inst.controller_path or "",
                    }
                )
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(cfg, f, ensure_ascii=False, indent=2)
        except Exception as e:
            QMessageBox.warning(
                self, "Erreur", f"Impossible d'enregistrer la config:\n{e}"
            )

    def on_load_config(
        self, path: Optional[str] = None, autostart: Optional[bool] = None
    ):
        if not path:
            path, _ = QFileDialog.getOpenFileName(
                self,
                "Charger configuration d'instances",
                "",
                "Configurations JSON (*.json);;Tous (*.*)",
            )
        if not path:
            return
        try:
            with open(path, "r", encoding="utf-8") as f:
                cfg = json.load(f)
        except Exception as e:
            QMessageBox.warning(self, "Erreur", f"Impossible de lire la config:\n{e}")
            return

        # Support nouveau format avec "close" et "open"
        if isinstance(cfg, dict):
            return self._load_config_close_open(cfg, autostart)

        # Format legacy : liste d'instances
        if not isinstance(cfg, list):
            QMessageBox.warning(
                self,
                "Erreur",
                "Format de config invalide (attendu: liste d'instances ou dict avec 'close'/'open').",
            )
            return

            # --- comportement d'écrasement (overwrite) ---
        try:
            overwrite = bool(self.chk_overwrite_instances.isChecked())
        except Exception:
            overwrite = bool(_prefs.get("instances", {}).get("overwrite_on_load", True))

        # Si overwrite demandé -> fermer et supprimer toutes les instances existantes
        if overwrite:
            # ne demander confirmation que s'il y a déjà des instances
            try:
                has_instances = False
                with _state_lock:
                    has_instances = bool(_instances)
            except Exception:
                has_instances = False

            if has_instances:
                # demande confirmation destructive uniquement si nécessaire
                if not self._ask(
                    "Écraser les instances",
                    "La configuration va fermer et supprimer toutes les instances existantes. Continuer ?",
                ):
                    # utilisateur a refusé -> on annule l'écrasement
                    overwrite = False
                else:
                    try:
                        # ferme et supprime tout (kill + delete)
                        self.kill_and_clear_all()
                    except Exception as e:
                        print(
                            "Erreur lors de l'écrasement des instances existantes:", e
                        )
            else:
                # rien à écraser → pas de popup, on continue directement
                pass

        # lire comportement d'écrasement depuis l'UI / prefs
        try:
            overwrite = bool(self.chk_overwrite_instances.isChecked())
        except Exception:
            overwrite = bool(_prefs.get("instances", {}).get("overwrite_on_load", True))

        titles_loaded = []
        for item in cfg:
            try:
                title = str(item.get("title") or "").strip()
                controller = item.get("controller") or ""
                if not title or not controller:
                    continue
            except Exception:
                continue

            with _state_lock:
                existed = title in _instances

                # Si on ne veut pas écraser et que l'instance existe déjà -> ignorer
                if existed and (not overwrite):
                    # on skippe la mise à jour / création pour cette instance
                    continue

                inst = _instances.get(title)
                if not inst:
                    inst = InstanceState(title)
                # toujours mettre à jour le controller (utile si on veut remplacer)
                inst.title = title
                inst.controller_path = controller
                inst.exe_path = inst.exe_path or EXE
                inst.images_dir = inst.images_dir or RESOURCES
                inst.ratio = inst.ratio or 0.5

                # Si l'instance existait déjà (restaurée ou manuelle), on NE la marque PAS comme stopped,
                # pour éviter que l'autopilote la relance inutilement.
                if not existed:
                    inst.stopped = True
                    inst.awaiting_first_hb = False
                    inst.last_heartbeat = 0.0
                # si existed => on laisse son état (pid/hwnd/last_heartbeat/restored_recently) tel quel

                _instances[title] = inst
            titles_loaded.append(title)

        if autostart is None:
            try:
                autostart = bool(self.chk_instance_launch.isChecked())
            except Exception:
                # fallback prefs si l'UI n'est pas prête
                autostart = (
                    _prefs.get("instances", {}).get("mode", "load_only")
                    == "load_and_launch"
                )

        self.refresh_list_full()
        self.update_instances_count()

        if autostart and titles_loaded:
            # récupère délai configuré (secondes). Défaut 0 si erreur.
            try:
                delay_s = int(self.spin_launch_delay.value())
            except Exception:
                delay_s = 0

            for idx, t in enumerate(titles_loaded):
                delay_ms = int(idx * delay_s * 1000)

                # on crée une closure pour vérifier au moment du schedule si l'instance est déjà running
                def _maybe_relaunch(_t=t):
                    # Vérifie si l'instance est déjà en cours au moment de la relance
                    with _state_lock:
                        inst = _instances.get(_t)
                        already_running = False
                        if inst:
                            try:
                                if (
                                    is_pid_alive(inst.pid)
                                    or is_hwnd_valid(inst.hwnd)
                                    or (not inst.stopped and not inst.awaiting_first_hb)
                                ):
                                    already_running = True
                            except Exception:
                                already_running = False

                    if already_running:
                        try:
                            _post_json(
                                "/log",
                                {
                                    "title": _t,
                                    "message": "Autostart: instance déjà en cours, relance ignorée.",
                                    "level": "INFO",
                                },
                            )
                        except Exception:
                            pass
                        return

                    try:
                        self.on_card_relaunch(_t)
                    except Exception as e:
                        print("Erreur autostart relaunch:", e)
                        try:
                            _post_json(
                                "/log",
                                {
                                    "title": _t,
                                    "message": f"Autostart relaunch failed: {e}",
                                    "level": "ERROR",
                                },
                            )
                        except Exception:
                            pass

                QTimer.singleShot(delay_ms, _maybe_relaunch)

    def _load_config_close_open(self, cfg: dict, autostart: Optional[bool] = None):
        """
        Charge une config au format { "close": [...], "open": [...] }.
        - close: liste de titres d'instances à fermer (kill uniquement, pas de suppression)
        - open: liste d'instances à lancer (format: { "title": "...", "controller": "..." })
        Si overwrite=false, on ignore tout le reste (ne touche pas aux autres instances).
        """
        try:
            overwrite = bool(self.chk_overwrite_instances.isChecked())
        except Exception:
            overwrite = bool(_prefs.get("instances", {}).get("overwrite_on_load", True))

        close_list = cfg.get("close", [])
        open_list = cfg.get("open", [])

        if not isinstance(close_list, list):
            close_list = []
        if not isinstance(open_list, list):
            open_list = []

        # 1) Fermer les instances de "close"
        for title_to_close in close_list:
            try:
                title_str = str(title_to_close).strip()
                if not title_str:
                    continue
                with _state_lock:
                    if title_str in _instances:
                        self.on_card_kill(title_str)
            except Exception as e:
                app_log_warn(f"Erreur fermeture instance '{title_to_close}': {e}")

        # 2) Charger/lancer les instances de "open"
        titles_loaded = []
        for item in open_list:
            try:
                if isinstance(item, str):
                    # Format simple : juste le titre (nécessite que l'instance existe déjà)
                    title = str(item).strip()
                    if not title:
                        continue
                    with _state_lock:
                        if title in _instances:
                            inst = _instances[title]
                            if not overwrite and self._is_instance_running(inst):
                                continue
                            titles_loaded.append(title)
                        else:
                            app_log_warn(
                                f"Instance '{title}' non trouvée pour ouverture (format string)"
                            )
                    continue

                # Format dict : { "title": "...", "controller": "..." }
                title = str(item.get("title") or "").strip()
                controller = item.get("controller") or ""
                if not title:
                    continue

                with _state_lock:
                    existed = title in _instances

                    # Si overwrite=false et instance existe déjà en cours -> ignorer
                    if existed and (not overwrite):
                        inst = _instances.get(title)
                        if inst and self._is_instance_running(inst):
                            continue

                    inst = _instances.get(title)
                    if not inst:
                        inst = InstanceState(title)

                    inst.title = title
                    if controller:
                        inst.controller_path = controller
                    inst.exe_path = inst.exe_path or EXE
                    inst.images_dir = inst.images_dir or RESOURCES
                    inst.ratio = inst.ratio or 0.5

                    if not existed:
                        inst.stopped = True
                        inst.awaiting_first_hb = False
                        inst.last_heartbeat = 0.0

                    _instances[title] = inst
                titles_loaded.append(title)
            except Exception as e:
                app_log_warn(f"Erreur chargement instance depuis 'open': {e}")
                continue

        self.refresh_list_full()
        self.update_instances_count()

        # 3) Lancer les instances si autostart
        if autostart is None:
            try:
                autostart = bool(self.chk_instance_launch.isChecked())
            except Exception:
                autostart = (
                    _prefs.get("instances", {}).get("mode", "load_only")
                    == "load_and_launch"
                )

        if autostart and titles_loaded:
            try:
                delay_s = int(self.spin_launch_delay.value())
            except Exception:
                delay_s = 0

            for idx, t in enumerate(titles_loaded):
                delay_ms = int(idx * delay_s * 1000)

                def _maybe_relaunch(_t=t):
                    with _state_lock:
                        inst = _instances.get(_t)
                        already_running = False
                        if inst:
                            try:
                                if (
                                    is_pid_alive(inst.pid)
                                    or is_hwnd_valid(inst.hwnd)
                                    or (not inst.stopped and not inst.awaiting_first_hb)
                                ):
                                    already_running = True
                            except Exception:
                                already_running = False

                    if already_running:
                        try:
                            _post_json(
                                "/log",
                                {
                                    "title": _t,
                                    "message": "Autostart: instance déjà en cours, relance ignorée.",
                                    "level": "INFO",
                                },
                            )
                        except Exception:
                            pass
                        return

                    try:
                        self.on_card_relaunch(_t)
                    except Exception as e:
                        app_log_error(f"Autostart relaunch failed for {_t}: {e}")

                QTimer.singleShot(delay_ms, _maybe_relaunch)

    def on_new_instance(self):
        path, _ = QFileDialog.getOpenFileName(
            self,
            "Sélectionner le contrôleur principal",
            "",
            "Exécutables / Scripts (*.exe *.bat *.cmd *.py *.lua);;Tous (*.*)",
        )
        if not path:
            return
        title, ok = QInputDialog.getText(
            self, "Nom de l'instance", "Nom de l'instance :"
        )
        if not ok or not title.strip():
            return
        title = title.strip()
        exe = EXE
        images = RESOURCES
        ratio = 0.5
        try:
            _post_json(
                "/register",
                {
                    "title": title,
                    "controller": path,
                    "exe": exe,
                    "images": images,
                    "ratio": ratio,
                    "touch": True,
                },
            )
        except Exception:
            pass
        try:
            self._spawn_runner_thread(exe, path, images, ratio, title)
            self.update_instances_count()
        except Exception as e:
            print("Erreur lors du lancement de l'instance via runner:", e)

    def on_launch_empty_instance(self):
        """
        Demande un nom, crée une InstanceState 'vide' (manual_empty=True), lance EXE en console
        et surveille le process dans un thread pour basculer le voyant en gris quand il meurt.
        """
        exe = EXE
        if not exe or not os.path.exists(exe):
            QMessageBox.warning(self, "Erreur", f"Exécutable introuvable:\n{exe}")
            return

        title, ok = QInputDialog.getText(
            self, "Nom de l'instance vide", "Nom de l'instance :"
        )
        if not ok or not title or not title.strip():
            return
        title = title.strip()

        # si titre déjà utilisé -> avertir (vérifier SANS tenir le lock pendant le popup,
        # sinon deadlock/crash : la modal lance la boucle d'événements et un autre slot peut prendre _state_lock)
        with _state_lock:
            already_exists = title in _instances
        if already_exists:
            QMessageBox.warning(
                self, "Erreur", f"Une instance existe déjà avec ce nom : {title}"
            )
            return

        # crée l'instance dans l'état global (jaune = en attente de la fenêtre)
        inst = InstanceState(title)
        inst.exe_path = exe
        inst.controller_path = None
        inst.images_dir = None
        inst.manual_empty = True
        now_ts = time.time()
        inst.last_heartbeat = now_ts
        inst.last_reset = now_ts
        inst.awaiting_first_hb = True
        inst.stopped = False

        with _state_lock:
            _instances[title] = inst

        try:
            bus.new_instance.emit(title)
            bus.instance_updated.emit(title)
        except Exception:
            pass
        # Forcer la mise à jour du voyant (bleu) après le prochain cycle Qt
        QTimer.singleShot(50, lambda t=title: self._refresh_one_card(t))

        # thread qui lance et surveille le process (--title + --empty pour restauration voyant bleu)
        def _run_and_monitor():
            _log_empty(f"[{title}] Début lancement instance vide exe={exe!r}")
            try:
                cmd = [exe, f"--title={title}", "--empty"]
                _log_empty(f"[{title}] Popen cmd={cmd}")
                exe_dir = os.path.dirname(os.path.abspath(exe)) if exe else None
                p = subprocess.Popen(
                    cmd,
                    creationflags=CREATE_NEW_CONSOLE,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    text=False,
                    cwd=exe_dir,
                )
            except Exception as e:
                _log_empty(f"[{title}] ERREUR Popen: {e}")
                with _state_lock:
                    inst.stopped = True
                    inst.awaiting_first_hb = False
                    inst.last_heartbeat = 0.0
                    _instances[title] = inst
                try:
                    bus.instance_updated.emit(title)
                except Exception:
                    pass
                return

            start_ts = time.time()
            _log_empty(
                f"[{title}] PID={p.pid} démarrage ok, attente grande fenêtre (voyant jaune)"
            )
            with _state_lock:
                try:
                    inst.pid = int(p.pid)
                    _instances[title] = inst
                except Exception:
                    pass
            try:
                bus.instance_updated.emit(title)
            except Exception:
                pass

            # Même logique que run_snowbot_flow : attendre la grande fenêtre (>= 50% écran), puis register + renommer
            try:
                main_hwnd, main_pid = wait_for_large_window_for_process(
                    p,
                    start_ts,
                    total_timeout=120.0,
                    min_screen_ratio=0.5,
                    poll_interval=0.25,
                    log_progress=True,
                    small_title=title,
                )
                _log_empty(
                    f"[{title}] Fenêtre détectée hwnd={main_hwnd} pid={main_pid}, register + renommage"
                )
                try:
                    _post_json(
                        "/register",
                        {
                            "title": title,
                            "pid": int(main_pid) if main_pid else 0,
                            "hwnd": int(main_hwnd),
                            "touch": True,
                        },
                    )
                    _log_empty(f"[{title}] /register OK")
                except Exception as e_reg:
                    _log_empty(f"[{title}] /register ÉCHEC: {e_reg}")
                _set_empty_instance_window_title(main_hwnd, title, log_prefix=title)
                with _state_lock:
                    inst.awaiting_first_hb = False
                    _instances[title] = inst
                try:
                    bus.instance_updated.emit(title)
                except Exception:
                    pass
                _log_empty(f"[{title}] Voyant passé au bleu, attente fin process...")
            except Exception as e_wait:
                _log_empty(f"[{title}] wait_for_large_window exception: {e_wait}")
                import traceback

                _log_empty(traceback.format_exc())

            # attente bloquante sur la fin du process (comme V3)
            p.wait()
            with _state_lock:
                inst.stopped = True
                inst.awaiting_first_hb = False
                inst.last_heartbeat = 0.0
                _instances[title] = inst
            try:
                bus.instance_updated.emit(title)
            except Exception:
                pass

        t = threading.Thread(target=_run_and_monitor, daemon=True)
        t.start()

    def on_bus_update(self, title: str):
        with _state_lock:
            inst = _instances.get(title)
            if inst:
                self._ensure_item(title, inst)
        if self.selected_title() == title:
            self.update_selected_details()
            self.update_sub_list()
        self.update_global_dot()
        self.update_instances_count()

    def _run_async(self, func, *args, **kwargs):
        def _job():
            try:
                func(*args, **kwargs)
            except Exception as e:
                app_log_error(f"Async task failed: {e}")

        try:
            self._bg_executor.submit(_job)
        except Exception:
            _job()

    def _schedule_auto_relaunch_scan(self, instances_snapshot):
        # ANCIEN SYSTÈME DÉSACTIVÉ - Remplacé par _scan_pids_periodic qui scanne toutes les minutes
        # Ce système était trop lent car il dépendait de la sévérité de l'instance (voyant rouge)
        # Le nouveau système vérifie directement les PID toutes les minutes, indépendamment du heartbeat
        pass

    def _scan_pids_periodic(self):
        """Scan périodique optimisé - exécute le travail lourd dans un thread background."""
        if not self.auto_relaunch_enabled:
            return
        # Lancer le scan dans un thread background pour ne pas bloquer l'UI
        self._run_async(self._scan_pids_worker)

    def _scan_pids_worker(self):
        """Worker thread pour le scan des PIDs - NE JAMAIS appeler directement depuis le thread UI."""
        try:
            scan_log(f"[SCAN_PIDS] ========== Début du scan ==========")

            # 1) Traiter d'abord les demandes de reset en attente
            self._process_pending_resets()

            # 2) Scanner les processus UNE SEULE FOIS pour toutes les instances
            try:
                all_processes = scan_snowbot_processes_by_cmdline(verbose=False)
                scan_log(
                    f"[SCAN_PIDS] {len(all_processes)} processus {APP_EXE_NAME} trouvés"
                )
            except Exception as e:
                scan_log(f"[SCAN_PIDS] Erreur scan processus: {e}")
                app_log_error(f"[SCAN_PIDS] Erreur scan processus: {e}")
                all_processes = {}

            # Construire un index par titre pour détection rapide des doublons
            processes_by_title: Dict[str, List[dict]] = {}
            for info in all_processes.values():
                proc_title = str(info.get("title") or "").strip()
                if proc_title:
                    if proc_title not in processes_by_title:
                        processes_by_title[proc_title] = []
                    processes_by_title[proc_title].append(info)

            # Afficher les doublons détectés
            duplicates_found = {
                t: procs for t, procs in processes_by_title.items() if len(procs) > 1
            }
            if duplicates_found:
                scan_log(
                    f"[SCAN_PIDS] ⚠️ Doublons détectés: {list(duplicates_found.keys())}"
                )
                for title, procs in duplicates_found.items():
                    pids = [p.get("pid") for p in procs]
                    scan_log(
                        f"[SCAN_PIDS]   - '{title}': {len(procs)} processus (PIDs: {pids})"
                    )

            # 3) Récupérer snapshot des instances (copie pour éviter les locks prolongés)
            instances_snapshot = []
            with _state_lock:
                for title, inst in _instances.items():
                    if inst.stopped:
                        continue
                    # Détecter les instances avec PID et HWND invalides (0 ou None)
                    # Ces instances sont considérées comme terminées même si stopped=False
                    pid = inst.pid
                    hwnd = inst.hwnd
                    if (not pid or pid == 0) and (not hwnd or hwnd == 0):
                        scan_log(
                            f"[SCAN_PIDS] '{title}': Instance avec PID={pid}, HWND={hwnd} invalides détectée (stopped={inst.stopped})"
                        )
                        # Marquer l'instance comme stoppée si elle ne l'est pas déjà
                        if not inst.stopped:
                            scan_log(
                                f"[SCAN_PIDS] '{title}': Marquage comme stoppée..."
                            )
                            inst.stopped = True
                            inst.pid = None
                            inst.hwnd = None
                            inst.awaiting_first_hb = False
                            inst.last_heartbeat = 0.0
                            try:
                                inst.sub_map.clear()
                            except Exception:
                                inst.sub_map = {}
                            _instances[title] = inst
                            scan_log(
                                f"[SCAN_PIDS] '{title}': ✓ Instance marquée comme stoppée"
                            )
                        continue
                    # Copier les valeurs nécessaires pour éviter les accès concurrents
                    instances_snapshot.append(
                        {
                            "title": title,
                            "pid": pid,
                            "hwnd": hwnd,
                            "controller_path": inst.controller_path,
                        }
                    )

            scan_log(
                f"[SCAN_PIDS] {len(instances_snapshot)} instances actives à vérifier"
            )

            if not instances_snapshot:
                scan_log(f"[SCAN_PIDS] Aucune instance active, fin du scan")
                return

            # 4) Collecter les actions à effectuer (sans bloquer)
            actions_to_perform = (
                []
            )  # Liste de (action_type, title, controller_path, reason)
            now = time.time()

            for inst_data in instances_snapshot:
                try:
                    title = inst_data["title"]

                    # Vérifier le cooldown
                    last = self._last_auto_relaunch_attempt.get(title, 0.0)
                    cooldown_remaining = AUTO_RELAUNCH_COOLDOWN_S - (now - last)
                    if cooldown_remaining > 0:
                        scan_log(
                            f"[SCAN_PIDS] '{title}': cooldown actif ({cooldown_remaining:.1f}s restants)"
                        )
                        continue

                    # Vérifier les doublons avec le cache
                    title_procs = processes_by_title.get(title, [])
                    if len(title_procs) > 1:
                        scan_log(
                            f"[SCAN_PIDS] '{title}': ⚠️ DOUBLONS détectés ({len(title_procs)} processus)"
                        )
                        # Doublons détectés - les kill tous
                        self._kill_duplicate_processes(title, title_procs)
                        scan_log(
                            f"[SCAN_PIDS] '{title}': Résolution du controller_path..."
                        )
                        controller_path = self._resolve_controller_path(
                            title, inst_data["controller_path"]
                        )
                        if controller_path:
                            scan_log(
                                f"[SCAN_PIDS] '{title}': ✓ Controller trouvé: {controller_path}"
                            )
                            actions_to_perform.append(
                                ("relaunch", title, controller_path, "doublons")
                            )
                        else:
                            scan_log(
                                f"[SCAN_PIDS] '{title}': ✗ ERREUR - Controller introuvable ! Instance restera fermée."
                            )
                            app_log_error(
                                f"[SCAN_PIDS] '{title}': Doublons nettoyés mais controller_path introuvable"
                            )
                        continue

                    # Vérifier si crash (PID/HWND morts)
                    pid_ok = (
                        is_pid_alive(inst_data["pid"]) if inst_data["pid"] else False
                    )
                    hwnd_ok = (
                        is_hwnd_valid(inst_data["hwnd"]) if inst_data["hwnd"] else False
                    )

                    if not pid_ok and not hwnd_ok:
                        scan_log(
                            f"[SCAN_PIDS] '{title}': ⚠️ CRASH détecté (PID={inst_data['pid']} mort, HWND={inst_data['hwnd']} invalide)"
                        )
                        controller_path = self._resolve_controller_path(
                            title, inst_data["controller_path"]
                        )
                        if controller_path:
                            scan_log(
                                f"[SCAN_PIDS] '{title}': ✓ Controller trouvé: {controller_path}"
                            )
                            actions_to_perform.append(
                                ("relaunch", title, controller_path, "crash")
                            )
                        else:
                            scan_log(
                                f"[SCAN_PIDS] '{title}': ✗ ERREUR - Controller introuvable ! Instance restera fermée."
                            )
                            app_log_error(
                                f"[SCAN_PIDS] '{title}': Crash détecté mais controller_path introuvable"
                            )

                except Exception as e:
                    scan_log(
                        f"[SCAN_PIDS] '{inst_data.get('title', '?')}': ✗ Exception: {e}"
                    )
                    app_log_error(
                        f"[SCAN_PIDS] Erreur pour {inst_data.get('title', '?')}: {e}"
                    )
                    import traceback

                    traceback.print_exc()

            # 5) Exécuter les actions dans le thread UI via QTimer
            scan_log(f"[SCAN_PIDS] {len(actions_to_perform)} action(s) à exécuter")
            for action_type, title, controller_path, reason in actions_to_perform:
                if action_type == "relaunch":
                    scan_log(
                        f"[SCAN_PIDS] '{title}': Planification relance (raison: {reason})"
                    )
                    self._last_auto_relaunch_attempt[title] = now
                    self._mark_instance_stopped(title)
                    # Stocker temporairement le controller_path pour la relance
                    with _state_lock:
                        inst = _instances.get(title)
                        if inst:
                            inst.controller_path = controller_path
                    # Utiliser le signal Qt (thread-safe) pour relancer depuis le thread principal
                    scan_log(
                        f"[SCAN_PIDS] '{title}': Émission signal de relance (raison: {reason})"
                    )
                    self.relaunch_requested.emit(title)

            scan_log(f"[SCAN_PIDS] ========== Fin du scan ==========")

        except Exception as e:
            scan_log(f"[SCAN_PIDS] ✗ ERREUR GLOBALE: {e}")
            app_log_error(f"[SCAN_PIDS] Erreur globale: {e}")
            import traceback

            traceback.print_exc()

    def _process_pending_resets(self):
        """Traite les demandes de reset en attente."""
        if not hasattr(self, "_pending_resets"):
            return

        resets_to_process = []
        with _state_lock:
            if hasattr(self, "_pending_resets") and self._pending_resets:
                resets_to_process = list(self._pending_resets)
                self._pending_resets.clear()

        for title in resets_to_process:
            try:
                # Kill le processus
                self._kill_instance_sync(title)
                # Émettre le signal pour relancer dans le thread UI
                # (les signaux Qt sont thread-safe)
                self.relaunch_requested.emit(title)
            except Exception as e:
                app_log_error(f"[RESET] Erreur reset {title}: {e}")

    def _kill_duplicate_processes(self, title: str, proc_list: List[dict]):
        """Kill tous les processus dupliqués pour un titre donné."""
        scan_log(
            f"[KILL_DUPLICATES] '{title}': Tentative de kill de {len(proc_list)} processus..."
        )
        killed_pids = []
        failed_pids = []

        for info in proc_list:
            pid = info.get("pid")
            if not pid:
                scan_log(f"[KILL_DUPLICATES] '{title}': Process sans PID ignoré")
                continue
            try:
                scan_log(f"[KILL_DUPLICATES] '{title}': Kill PID {pid}...")
                terminate_process_tree(int(pid), timeout=3.0)
                killed_pids.append(pid)
                scan_log(f"[KILL_DUPLICATES] '{title}': ✓ PID {pid} tué")
            except Exception as e:
                scan_log(f"[KILL_DUPLICATES] '{title}': ✗ Échec kill PID {pid}: {e}")
                failed_pids.append(pid)

        if killed_pids:
            scan_log(
                f"[KILL_DUPLICATES] '{title}': ✓ {len(killed_pids)} processus tués: {killed_pids}"
            )
            app_log_warn(
                f"[SCAN_PIDS] Doublons nettoyés pour '{title}' (PIDs={killed_pids})"
            )

        if failed_pids:
            scan_log(
                f"[KILL_DUPLICATES] '{title}': ⚠️ {len(failed_pids)} échecs: {failed_pids}"
            )

        # Mettre à jour l'état
        scan_log(f"[KILL_DUPLICATES] '{title}': Marquage comme stoppée...")
        self._mark_instance_stopped(title)

    def _resolve_controller_path(
        self, title: str, current_path: Optional[str]
    ) -> Optional[str]:
        """Résout le controller_path, charge depuis autoload si nécessaire."""
        scan_log(f"[RESOLVE_CTRL] '{title}': current_path={current_path}")

        if current_path:
            if os.path.exists(current_path):
                scan_log(f"[RESOLVE_CTRL] '{title}': ✓ Path actuel valide")
                return current_path
            else:
                scan_log(
                    f"[RESOLVE_CTRL] '{title}': ⚠️ Path actuel n'existe pas: {current_path}"
                )
        else:
            scan_log(f"[RESOLVE_CTRL] '{title}': ⚠️ Aucun path actuel")

        # Essayer autoload
        scan_log(f"[RESOLVE_CTRL] '{title}': Tentative chargement depuis autoload...")
        try:
            controller_path = self._load_controller_from_autoload(title)
            if controller_path:
                if os.path.exists(controller_path):
                    scan_log(
                        f"[RESOLVE_CTRL] '{title}': ✓ Trouvé dans autoload: {controller_path}"
                    )
                    with _state_lock:
                        inst = _instances.get(title)
                        if inst:
                            inst.controller_path = controller_path
                            _instances[title] = inst
                    return controller_path
                else:
                    scan_log(
                        f"[RESOLVE_CTRL] '{title}': ⚠️ Path autoload invalide: {controller_path}"
                    )
            else:
                scan_log(f"[RESOLVE_CTRL] '{title}': ⚠️ Titre non trouvé dans autoload")
        except Exception as e:
            scan_log(f"[RESOLVE_CTRL] '{title}': ✗ Exception autoload: {e}")

        scan_log(f"[RESOLVE_CTRL] '{title}': ✗ ÉCHEC - Aucun controller valide trouvé")
        return None

    def _mark_instance_stopped(self, title: str):
        """Marque une instance comme stoppée de manière thread-safe."""
        scan_log(f"[MARK_STOPPED] '{title}': Marquage comme stoppée...")
        with _state_lock:
            inst = _instances.get(title)
            if inst:
                inst.pid = None
                inst.hwnd = None
                inst.stopped = True
                inst.awaiting_first_hb = False
                inst.last_heartbeat = 0.0
                try:
                    inst.sub_map.clear()
                except Exception:
                    inst.sub_map = {}
                _instances[title] = inst
                scan_log(f"[MARK_STOPPED] '{title}': ✓ Instance marquée comme stoppée")
            else:
                scan_log(
                    f"[MARK_STOPPED] '{title}': ⚠️ Instance non trouvée dans _instances"
                )

        # Émettre le signal dans le thread UI
        QTimer.singleShot(0, lambda t=title: self._emit_instance_updated(t))

    def _emit_instance_updated(self, title: str):
        """Émet le signal instance_updated de manière sûre."""
        try:
            bus.instance_updated.emit(title)
        except Exception:
            pass

    def _safe_relaunch(self, title: str, controller_path: str):
        """Relance une instance de manière sûre depuis le thread UI."""
        scan_log(f"[SAFE_RELAUNCH] '{title}': Début de la relance...")
        scan_log(f"[SAFE_RELAUNCH] '{title}': Controller: {controller_path}")

        if not controller_path:
            scan_log(
                f"[SAFE_RELAUNCH] '{title}': ✗ ERREUR - controller_path est None/vide !"
            )
            app_log_error(
                f"[SAFE_RELAUNCH] Impossible de relancer '{title}': controller_path vide"
            )
            return

        if not os.path.exists(controller_path):
            scan_log(
                f"[SAFE_RELAUNCH] '{title}': ✗ ERREUR - controller_path n'existe pas: {controller_path}"
            )
            app_log_error(
                f"[SAFE_RELAUNCH] Impossible de relancer '{title}': fichier introuvable"
            )
            return

        try:
            scan_log(f"[SAFE_RELAUNCH] '{title}': Appel on_card_relaunch_force...")
            self.on_card_relaunch_force(title, controller_path)
            scan_log(f"[SAFE_RELAUNCH] '{title}': ✓ Relance réussie")
        except Exception as e:
            scan_log(f"[SAFE_RELAUNCH] '{title}': ✗ EXCEPTION: {e}")
            app_log_error(f"[SAFE_RELAUNCH] Relance échouée pour {title}: {e}")
            import traceback

            traceback.print_exc()

    def _kill_instance_sync(self, title: str):
        """Kill une instance de manière synchrone (pour le worker thread)."""
        pid = None
        with _state_lock:
            inst = _instances.get(title)
            if inst:
                pid = inst.pid

        if pid:
            try:
                terminate_process_tree(int(pid), timeout=3.0)
            except Exception as e:
                app_log_warn(f"[RESET] Terminate failed for {title}: {e}")

        self._mark_instance_stopped(title)

    def _trigger_relaunch_from_reset(self, title: str):
        """Déclenche une relance après un reset."""
        try:
            self.on_card_relaunch(title)
        except Exception as e:
            app_log_error(f"[RESET] Relaunch failed for {title}: {e}")

    def _enforce_unique_title_processes(self, title: str) -> bool:
        """Supprime les processus AnkaBot dupliqués partageant le même --title."""
        if not title:
            return False
        try:
            matches = find_processes_by_title(title)
        except Exception as e:
            app_log_error(f"find_processes_by_title failed: {e}")
            return False
        if len(matches) <= 1:
            return False

        killed_pids: List[int] = []
        for info in matches:
            pid = info.get("pid")
            if not pid:
                continue
            try:
                pid_int = int(pid)
            except Exception:
                continue
            killed_pids.append(pid_int)
            terminate_process_tree(pid_int, timeout=5.0)

        if killed_pids:
            app_log_warn(
                f"Instances dupliquées détectées pour '{title}' (PIDs={killed_pids}), nettoyage forcé."
            )
            try:
                _post_json(
                    "/log",
                    {
                        "title": title,
                        "message": f"Nettoyage des instances dupliquées (PIDs={killed_pids}).",
                        "level": "WARN",
                    },
                )
            except Exception:
                pass

        with _state_lock:
            inst = _instances.get(title)
            if inst:
                inst.pid = None
                inst.hwnd = None
                inst.stopped = True
                inst.awaiting_first_hb = False
                inst.last_heartbeat = 0.0
                try:
                    inst.sub_map.clear()
                except Exception:
                    inst.sub_map = {}
                _instances[title] = inst

        try:
            bus.instance_updated.emit(title)
        except Exception:
            pass
        QTimer.singleShot(
            0,
            lambda: (
                self.update_sub_list(),
                self.update_selected_details(),
                self.update_global_dot(),
            ),
        )
        return True

    def _load_controller_from_autoload(self, title: str) -> Optional[str]:
        """Charge le contrôleur depuis autoload_instances_file si disponible."""
        try:
            autoload_file = get_autoload_instances_file(_prefs)
            if not autoload_file or not os.path.exists(autoload_file):
                return None

            with open(autoload_file, "r", encoding="utf-8") as f:
                autos = json.load(f)

            if isinstance(autos, list):
                for inst_data in autos:
                    if inst_data.get("title") == title:
                        ctrl = inst_data.get("controller")
                        if ctrl and os.path.exists(ctrl):
                            return ctrl
        except Exception as e:
            app_log_warn(
                f"Erreur lors du chargement du contrôleur depuis autoload pour {title}: {e}"
            )
        return None

    def _get_instance_launch_params(
        self, title: str
    ) -> Tuple[
        Optional["InstanceState"], Optional[str], Optional[str], Optional[str], float
    ]:
        with _state_lock:
            inst = _instances.get(title)
            if not inst:
                return None, None, None, None, 0.0
            controller = inst.controller_path
            # Si le contrôleur est perdu, essayer de le charger depuis autoload_instances_file
            if not controller:
                controller = self._load_controller_from_autoload(title)
                # Si trouvé, le sauvegarder dans l'instance
                if controller:
                    inst.controller_path = controller
                    _instances[title] = inst
            exe = inst.exe_path or EXE
            images = inst.images_dir or RESOURCES
            ratio = inst.ratio or 0.5
        return inst, controller, exe, images, ratio

    def _kill_instance_background(self, title: str):
        if not title:
            return
        pid = None
        with _state_lock:
            inst = _instances.get(title)
            if inst:
                pid = inst.pid
        if pid:
            try:
                proc = psutil.Process(int(pid))
                if proc.is_running():
                    proc.terminate()
                    try:
                        proc.wait(timeout=3)
                    except psutil.TimeoutExpired:
                        proc.kill()
            except Exception as e:
                app_log_warn(f"Terminate failed for {title}: {e}")
        # Utiliser _mark_instance_stopped pour garantir que tous les champs sont réinitialisés
        self._mark_instance_stopped(title)
        try:
            _post_json("/goodbye", {"title": title})
        except Exception:
            pass
        try:
            bus.instance_updated.emit(title)
        except Exception:
            pass

        def _refresh_ui():
            self.update_sub_list()
            self.update_selected_details()
            self.update_global_dot()

        QTimer.singleShot(0, _refresh_ui)

    def _on_relaunch_requested(self, title: str):
        """Slot appelé dans le thread principal via le signal, qui planifie on_card_relaunch de manière asynchrone."""
        # Utiliser QTimer.singleShot pour planifier l'exécution dans le thread principal
        # de manière asynchrone (non-bloquante car dans la queue d'événements Qt)
        # Cela permet d'exécuter on_card_relaunch dans le thread principal (nécessaire pour QFileDialog)
        # mais sans bloquer car c'est planifié dans la queue d'événements
        QTimer.singleShot(0, lambda t=title: self.on_card_relaunch(t))

    def on_bus_reset_instance(self, title: str):
        """Stoppe puis relance l'instance demandée via la queue de resets."""
        if not title:
            return
        # Ajouter à la queue de resets qui sera traitée dans le prochain scan
        with _state_lock:
            if not hasattr(self, "_pending_resets"):
                self._pending_resets = []
            if title not in self._pending_resets:
                self._pending_resets.append(title)
        # Déclencher un traitement immédiat dans un thread background
        self._run_async(self._process_pending_resets)

    def closeEvent(self, event):
        global _discord_bot

        # Arrêter le bot Discord (rapide, sans attendre)
        try:
            _discord_bot.stop()
        except Exception:
            pass  # Ignorer les erreurs pour fermer rapidement

        # Arrêter les threads en arrière-plan (sans attendre)
        try:
            if hasattr(self, "_bg_executor"):
                self._bg_executor.shutdown(wait=False)
        except Exception:
            pass

        # Fermeture immédiate
        super().closeEvent(event)

    def on_bus_goodbye_kill(self, title: str):
        """
        Fermeture + kill demandée par l'instance elle-même.
        On réutilise le kill standard UI (on_card_kill) pour rester cohérent.
        """
        try:
            self.on_card_kill(title)
        except Exception as e:
            print(f"[GOODBYE_KILL] kill failed for {title}: {e}")

    def on_bus_remove(self, title: str):
        pass

    def on_toggle_auto_relaunch(self, _state):
        self.auto_relaunch_enabled = self.chk_auto_relaunch.isChecked()
        _prefs["autoRelaunch"] = bool(self.auto_relaunch_enabled)
        save_prefs(_prefs)
        # Activer/désactiver le timer de scan PID selon la checkbox
        if self.auto_relaunch_enabled:
            self.timer_pid_scan.start(30000)  # 30 secondes (plus réactif)
        else:
            self.timer_pid_scan.stop()

    def on_toggle_discord_alert(self, _state):
        """Met à jour la préférence 'discord.enabled' lorsque l'utilisateur change la checkbox."""
        self.discord_alert_enabled = self.chk_discord_alert.isChecked()
        _prefs.setdefault("discord", {})["enabled"] = bool(self.discord_alert_enabled)
        save_prefs(_prefs)

    def on_toggle_overwrite_instances(self, _state):
        """Met à jour la préférence 'overwrite_on_load' lorsque l'utilisateur change la checkbox."""
        val = bool(self.chk_overwrite_instances.isChecked())
        _prefs.setdefault("instances", {})["overwrite_on_load"] = val
        save_prefs(_prefs)

    def on_toggle_autopilot(self, _state):
        self.autopilot_enabled = self.chk_autopilot.isChecked()
        _prefs.setdefault("autopilot", {})["enabled"] = bool(self.autopilot_enabled)
        save_prefs(_prefs)
        self.update_autopilot_state_label()

    def on_toggle_autopilot_overwrite(self, _state):
        val = bool(self.chk_autopilot_overwrite.isChecked())
        _prefs.setdefault("autopilot", {})["overwrite"] = val
        save_prefs(_prefs)

    def _save_collapse_pref(self, key: str, expanded: bool):
        """Sauvegarde la préférence d'état collapse/expand."""
        try:
            _prefs.setdefault("ui", {})[key] = bool(expanded)
            save_prefs(_prefs)
        except Exception:
            pass

    def _load_custom_buttons(self):
        """Charge les boutons programmables depuis les préférences."""
        try:
            buttons = _prefs.get("custom_buttons", [])
            if not isinstance(buttons, list):
                buttons = []
            app_log_error(
                f"Chargement de {len(buttons)} bouton(s) depuis les préférences"
            )

            for btn_data in buttons:
                if not isinstance(btn_data, dict):
                    continue

                title = btn_data.get("title", "")
                code = btn_data.get("code", "")
                button_id = btn_data.get("id")

                # Vérifier que le code existe (au moins une action doit être définie)
                if not code or not code.strip():
                    app_log_error(
                        f"Bouton ignoré (pas de code): title='{title}', id='{button_id}'"
                    )
                    continue

                # Générer un ID si manquant
                if not button_id:
                    import uuid

                    button_id = str(uuid.uuid4())

                # Permettre les boutons avec titre vide
                try:
                    self._add_custom_button_ui(str(title), str(code), str(button_id))
                    app_log_error(f"Bouton chargé: title='{title}', id='{button_id}'")
                except Exception as e:
                    app_log_error(f"Erreur lors du chargement du bouton '{title}': {e}")
                    import traceback

                    app_log_error(traceback.format_exc())
        except Exception as e:
            app_log_error(f"Erreur lors du chargement des boutons: {e}")
            import traceback

            app_log_error(traceback.format_exc())

    def _save_custom_buttons(self):
        """Sauvegarde les boutons programmables dans les préférences."""
        try:
            buttons = []
            # Parcourir tous les items du layout (le bouton "Ajouter" n'est pas dans ce layout)
            for i in range(self.custom_buttons_layout.count()):
                item = self.custom_buttons_layout.itemAt(i)
                if item and item.widget():
                    widget = item.widget()
                    # Vérifier si c'est un bouton personnalisé avec les attributs requis
                    if (
                        isinstance(widget, QPushButton)
                        and hasattr(widget, "_button_id")
                        and hasattr(widget, "_button_title")
                        and hasattr(widget, "_button_code")
                    ):
                        # Vérifier que le code n'est pas vide
                        code = widget._button_code
                        if code and code.strip():
                            buttons.append(
                                {
                                    "id": str(widget._button_id),
                                    "title": str(widget._button_title),
                                    "code": str(code),
                                }
                            )
                        else:
                            app_log_error(
                                f"Bouton ignoré lors de la sauvegarde (code vide): '{widget._button_title}'"
                            )

            # Sauvegarder même si la liste est vide (pour effacer les anciens boutons)
            _prefs["custom_buttons"] = buttons
            save_prefs(_prefs)
            app_log_error(f"Boutons sauvegardés: {len(buttons)} bouton(s)")
        except Exception as e:
            app_log_error(f"Erreur lors de la sauvegarde des boutons: {e}")
            import traceback

            app_log_error(traceback.format_exc())

    def _add_custom_button_ui(self, title: str, code: str, button_id: str = None):
        """Ajoute un bouton programmable à l'UI."""
        import uuid

        if button_id is None:
            button_id = str(uuid.uuid4())

        # Permettre titre vide (bouton sans texte)
        btn = QPushButton(title if title else " ")
        btn.setCursor(Qt.PointingHandCursor)
        btn._button_id = button_id
        btn._button_title = title
        btn._button_code = code
        btn._is_executing = False

        # Gérer le clic normal et CTRL+clic
        def on_button_clicked(event):
            if event.modifiers() & Qt.ControlModifier:
                # CTRL+clic : modifier
                self.on_edit_custom_button(button_id)
            else:
                # Clic normal : exécuter
                if not btn._is_executing:
                    self.on_execute_custom_button(code, btn)

        btn.mousePressEvent = on_button_clicked

        # Insérer avant le bouton "Ajouter"
        insert_index = self.custom_buttons_layout.count() - 1
        self.custom_buttons_layout.insertWidget(insert_index, btn)

    def on_add_custom_button(self):
        """Ouvre le dialogue pour ajouter un nouveau bouton."""
        dialog = CustomButtonDialog(self)
        if dialog.exec() == QDialog.Accepted:
            title = dialog.get_title()
            code = dialog.get_code()
            # Permettre titre vide, mais au moins une action doit être définie
            if code:
                self._add_custom_button_ui(title, code)
                self._save_custom_buttons()

    def on_edit_custom_button(self, button_id: str):
        """Ouvre le dialogue pour modifier un bouton existant."""
        # Trouver le bouton
        btn = None
        for i in range(self.custom_buttons_layout.count()):
            item = self.custom_buttons_layout.itemAt(i)
            if item and item.widget():
                w = item.widget()
                if hasattr(w, "_button_id") and w._button_id == button_id:
                    btn = w
                    break

        if not btn:
            return

        # Parser le code pour extraire close et open
        close_keywords = ""
        open_keywords = ""
        try:
            lines = btn._button_code.strip().split("\n")
            for line in lines:
                line = line.strip()
                if ":" in line:
                    action, targets = line.split(":", 1)
                    action = action.strip().lower()
                    targets = targets.strip()
                    if action == "close":
                        close_keywords = targets
                    elif action == "open":
                        open_keywords = targets
        except Exception:
            pass

        dialog = CustomButtonDialog(
            self,
            btn._button_title,
            close_keywords,
            open_keywords,
            is_edit=True,
            button_id=button_id,
        )
        if dialog.exec() == QDialog.Accepted:
            title = dialog.get_title()
            code = dialog.get_code()
            # Permettre titre vide, mais au moins une action doit être définie
            if code:
                # Mettre à jour le bouton
                btn.setText(title if title else " ")
                btn._button_title = title
                btn._button_code = code
                self._save_custom_buttons()
        elif hasattr(dialog, "delete_requested") and dialog.delete_requested:
            # Supprimer le bouton
            self._remove_custom_button(button_id)

    def _remove_custom_button(self, button_id: str):
        """Supprime un bouton programmable."""
        # Trouver et supprimer le bouton
        for i in range(self.custom_buttons_layout.count()):
            item = self.custom_buttons_layout.itemAt(i)
            if item and item.widget():
                w = item.widget()
                if hasattr(w, "_button_id") and w._button_id == button_id:
                    # Arrêter le timer avant de supprimer
                    if hasattr(w, "_pulse_timer") and w._pulse_timer:
                        try:
                            w._pulse_timer.stop()
                            w._pulse_timer.deleteLater()
                        except Exception:
                            pass
                        w._pulse_timer = None

                    # Marquer comme non-exécutant
                    if hasattr(w, "_is_executing"):
                        w._is_executing = False

                    self.custom_buttons_layout.removeWidget(w)
                    w.deleteLater()
                    self._save_custom_buttons()
                    break

    def on_execute_custom_button(self, code: str, btn: QPushButton = None):
        """Exécute le code d'un bouton programmable avec animation."""
        if btn:
            btn._is_executing = True
            # Sauvegarder l'état original
            btn._original_text = btn.text()
            btn._original_style = btn.styleSheet() if btn.styleSheet() else ""

            # Animation : changer la couleur du bouton avec un effet de pulsation
            # IMPORTANT: Créer le timer dans le thread principal Qt avec self comme parent
            btn.setText("⏳ " + (btn._button_title if btn._button_title else ""))
            btn._pulse_state = 0  # 0 = clair, 1 = foncé

            # Créer le timer avec self comme parent pour qu'il soit dans le thread principal
            pulse_timer = QTimer(self)
            pulse_timer.setSingleShot(False)  # Timer répétitif

            # Utiliser une fonction qui vérifie l'état avant d'appeler _pulse_button
            def pulse_callback():
                try:
                    if btn and hasattr(btn, "_is_executing") and btn._is_executing:
                        self._pulse_button(btn)
                    else:
                        # Arrêter le timer si le bouton n'est plus en exécution
                        if pulse_timer:
                            pulse_timer.stop()
                except (RuntimeError, AttributeError):
                    if pulse_timer:
                        pulse_timer.stop()

            pulse_timer.timeout.connect(pulse_callback)
            btn._pulse_timer = pulse_timer
            pulse_timer.start(500)  # Change toutes les 500ms
            self._pulse_button(btn)  # Premier état

        # Exécuter directement dans le thread principal (les opérations sont déjà asynchrones)
        actions_performed = False
        try:
            lines = code.strip().split("\n")
            for line in lines:
                line = line.strip()
                if not line:
                    continue

                # Parser la ligne : "close : Metiers,Halouf" ou "open : Vidage"
                if ":" in line:
                    action, targets = line.split(":", 1)
                    action = action.strip().lower()
                    targets = targets.strip()

                    if action == "close":
                        # Fermer les instances dont le titre contient un des mots-clés
                        keywords = [t.strip() for t in targets.split(",") if t.strip()]
                        if self._close_instances_by_keywords(keywords):
                            actions_performed = True
                    elif action == "open":
                        # Lancer les instances dont le titre contient le mot-clé
                        keywords = [t.strip() for t in targets.split(",") if t.strip()]
                        if self._open_instances_by_keywords(keywords):
                            actions_performed = True
        except Exception as e:
            app_log_error(f"Erreur lors de l'exécution du bouton: {e}")
            QMessageBox.warning(self, "Erreur", f"Erreur lors de l'exécution:\n{e}")
        finally:
            if btn:
                # Restaurer l'état normal après un court délai
                # Le délai permet aux opérations asynchrones de démarrer
                delay = 300 if actions_performed else 150
                QTimer.singleShot(delay, lambda: self._restore_button_state(btn))

    def _pulse_button(self, btn: QPushButton):
        """Change la couleur du bouton pour créer un effet de pulsation."""
        # Vérifier que le bouton existe encore et n'a pas été supprimé
        try:
            if (
                not btn
                or not hasattr(btn, "_pulse_state")
                or not hasattr(btn, "_is_executing")
            ):
                return

            # Vérifier que le bouton n'a pas été supprimé
            if not btn._is_executing:
                return

            if btn._pulse_state == 0:
                btn.setStyleSheet("QPushButton { background-color: #3b82f6; }")
                btn._pulse_state = 1
            else:
                btn.setStyleSheet("QPushButton { background-color: #60a5fa; }")
                btn._pulse_state = 0
            btn.repaint()
        except RuntimeError:
            # Le bouton a été supprimé, arrêter le timer
            if hasattr(btn, "_pulse_timer") and btn._pulse_timer:
                try:
                    btn._pulse_timer.stop()
                    btn._pulse_timer.deleteLater()
                except Exception:
                    pass
                btn._pulse_timer = None
        except Exception:
            pass

    def _restore_button_state(self, btn: QPushButton):
        """Restaure l'état normal du bouton après l'exécution."""
        if not btn:
            return

        # Vérifier que nous sommes dans le thread principal
        if not hasattr(btn, "_is_executing"):
            return

        btn._is_executing = False

        # Arrêter le timer de pulsation si il existe
        if hasattr(btn, "_pulse_timer") and btn._pulse_timer:
            try:
                btn._pulse_timer.stop()
                btn._pulse_timer.deleteLater()
            except Exception:
                pass
            finally:
                btn._pulse_timer = None

        # Nettoyer les attributs de pulsation
        if hasattr(btn, "_pulse_state"):
            delattr(btn, "_pulse_state")

        # Restaurer le style et le texte
        if hasattr(btn, "_original_style") and btn._original_style:
            # Si il y avait un style original, le restaurer
            btn.setStyleSheet(btn._original_style)
        else:
            # Sinon, réinitialiser complètement le style pour reprendre le style global
            btn.setStyleSheet("")
            # Forcer la mise à jour du style en désactivant puis réactivant
            btn.style().unpolish(btn)
            btn.style().polish(btn)

        if hasattr(btn, "_button_title"):
            btn.setText(btn._button_title if btn._button_title else " ")
        elif hasattr(btn, "_original_text"):
            btn.setText(btn._original_text)

        # Forcer la mise à jour visuelle
        btn.update()
        btn.repaint()

    def _close_instances_by_keywords(self, keywords: list) -> bool:
        """Ferme les instances dont le titre contient un des mots-clés.
        Retourne True si au moins une instance a été fermée, False sinon."""
        if not keywords:
            return False

        with _state_lock:
            instances_snapshot = list(_instances.keys())

        closed_count = 0
        for title in instances_snapshot:
            for keyword in keywords:
                if keyword.lower() in title.lower():
                    self.on_card_kill(title)
                    closed_count += 1
                    break

        if closed_count > 0:
            QTimer.singleShot(
                100,
                lambda: (
                    self.update_sub_list(),
                    self.update_selected_details(),
                    self.update_global_dot(),
                    self.update_instances_count(),
                ),
            )
            return True
        return False

    def _open_instances_by_keywords(self, keywords: list) -> bool:
        """Lance les instances dont le titre contient un des mots-clés avec un délai entre chaque lancement.
        Retourne True si au moins une instance a été lancée, False sinon."""
        if not keywords:
            return False

        with _state_lock:
            instances_snapshot = list(_instances.keys())

        # Récupérer le délai configuré
        try:
            delay_s = int(self.spin_launch_delay.value())
        except Exception:
            delay_s = 0

        # Collecter toutes les instances à lancer
        instances_to_launch = []
        for title in instances_snapshot:
            for keyword in keywords:
                if keyword.lower() in title.lower():
                    # Vérifier si l'instance est déjà en cours d'exécution
                    inst = _instances.get(title)
                    if inst and inst.pid:
                        try:
                            # Vérifier si le processus existe encore
                            psutil.Process(int(inst.pid))
                            # Le processus existe, on ne relance pas
                            continue
                        except (psutil.NoSuchProcess, psutil.AccessDenied, ValueError):
                            # Le processus n'existe plus, on peut relancer
                            pass

                    instances_to_launch.append(title)
                    break

        if not instances_to_launch:
            return False

        # Lancer les instances avec un délai entre chaque (utiliser QTimer pour ne pas bloquer l'UI)
        for idx, title in enumerate(instances_to_launch):
            delay_ms = idx * delay_s * 1000  # Convertir en millisecondes
            QTimer.singleShot(
                delay_ms,
                lambda t=title: self.on_card_relaunch(t),
            )

        # Mettre à jour l'UI après le dernier lancement
        total_delay_ms = len(instances_to_launch) * delay_s * 1000
        QTimer.singleShot(
            total_delay_ms + 100,
            lambda: (
                self.update_sub_list(),
                self.update_selected_details(),
                self.update_global_dot(),
                self.update_instances_count(),
            ),
        )
        return True

    def on_change_launch_delay(self, value: int):
        # sauvegarde immédiate dans les prefs
        _prefs.setdefault("instances", {})["launch_delay"] = int(value)
        save_prefs(_prefs)

    def on_change_reddot(self, value: int):
        """Met à jour le délai du reddot (heartbeat timeout) et sauvegarde dans les prefs."""
        global HEARTBEAT_RED_S
        # Mettre à jour la variable globale
        HEARTBEAT_RED_S = int(value)
        # Sauvegarder dans les préférences
        _prefs["reddot"] = int(value)
        save_prefs(_prefs)
        app_log_info(f"Délai reddot mis à jour : {value}s")

    def on_toggle_instance_launch_mode(self, _state):
        """Instances: décoché=load_only, coché=load_and_launch"""
        mode = (
            "load_and_launch" if self.chk_instance_launch.isChecked() else "load_only"
        )
        _prefs.setdefault("instances", {})["mode"] = mode
        save_prefs(_prefs)
        self.update_autopilot_state_label()

    def on_toggle_autopilot_mode(self, _state):
        """Autopilote: décoché=load_only, coché=load_and_launch"""
        self.autopilot_mode = (
            "load_and_launch" if self.chk_autopilot_launch.isChecked() else "load_only"
        )
        _prefs.setdefault("autopilot", {})["mode"] = self.autopilot_mode
        save_prefs(_prefs)
        self.update_autopilot_state_label()

    def on_change_instance_mode(self, mode: str):
        _prefs.setdefault("instances", {})["mode"] = str(mode)
        save_prefs(_prefs)
        # Met à jour l'affichage éventuellement si besoin
        self.update_autopilot_state_label()

    def on_change_autopilot_mode(self, mode: str):
        self.autopilot_mode = mode
        _prefs.setdefault("autopilot", {})["mode"] = mode
        save_prefs(_prefs)
        self.update_autopilot_state_label()

    def on_instances_reordered(self, titles_in_order: List[str]):
        """Persist l’ordre après drag & drop."""
        # Filtrer None / doublons au cas où
        clean = [t for t in titles_in_order if t]
        # Ajoute ceux non listés (si Qt n’a renvoyé que la page visible, par ex.)
        with _state_lock:
            for t in _instances.keys():
                if t not in clean:
                    clean.append(t)
        self.instance_order = clean
        _prefs.setdefault("instances", {})["order"] = clean
        save_prefs(_prefs)

    def on_autopilot_load_plan(self):
        path, _ = QFileDialog.getOpenFileName(
            self, "Charger planning autopilote", "", "JSON (*.json);;Tous (*.*)"
        )
        if not path:
            return
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            schedules = data.get("schedules")
            if not isinstance(schedules, list):
                raise ValueError("Format inattendu: 'schedules' (liste) manquant.")
            self.autopilot_schedules = schedules
            _prefs.setdefault("autopilot", {})["schedules"] = schedules
            save_prefs(_prefs)
            QMessageBox.information(
                self, "Autopilote", f"{len(schedules)} créneau(x) chargé(s)."
            )
        except Exception as e:
            QMessageBox.warning(
                self, "Erreur", f"Impossible de charger le planning:\n{e}"
            )
        self.update_autopilot_state_label()

    def on_autopilot_save_plan(self):
        path, _ = QFileDialog.getSaveFileName(
            self, "Enregistrer planning autopilote", "", "JSON (*.json);;Tous (*.*)"
        )
        if not path:
            return
        data = {"schedules": self.autopilot_schedules}
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            QMessageBox.information(self, "Autopilote", "Planning sauvegardé.")
        except Exception as e:
            QMessageBox.warning(self, "Erreur", f"Impossible d'enregistrer:\n{e}")

    def update_autopilot_state_label(self, active: Optional[dict] = None):
        if not self.autopilot_enabled:
            self._autopilot_status_text = "Autopilote: désactivé"
        else:
            if active is None:
                tmp_prefs = dict(_prefs)
                tmp_prefs.setdefault("autopilot", {})
                tmp_prefs["autopilot"]["schedules"] = self.autopilot_schedules
                active = match_autopilot_schedule(tmp_prefs)
            if active:
                nm = active.get("name", "(sans nom)")
                cfg = active.get("config", "(aucun)")
                self._autopilot_status_text = f"Actif: {nm} → {os.path.basename(cfg)}"
            else:
                self._autopilot_status_text = "Aucun créneau actif"
        self.lbl_autopilot_status.setText(self._autopilot_status_text)

    def evaluate_autopilot_now(self):
        """
        Forcer l'évaluation — on appelle la périodique avec force=True pour forcer l'action
        même si la clé calculée est identique à la précédente.
        """
        try:
            # force = True permet de bypasser le check `if key == self._last_autopilot_active_key: return`
            self.evaluate_autopilot_periodic(force=True)
        except Exception as e:
            print("Erreur evaluate_autopilot_now:", e)

    def evaluate_autopilot_periodic(self, force: bool = False):
        """
        Évaluation périodique de l'autopilote.
        - Si force=True : on force l'évaluation même si la clé n'a pas changé.
        - Si aucun créneau actif -> on ferme TOUT de suite (kill_and_clear_all).
        - Si un créneau devient actif -> on charge la config et autostart (si demandé),
        mais on évite de relancer des instances qui sont déjà en cours.
        """
        if not self.autopilot_enabled:
            self._last_autopilot_active_key = None
            self.update_autopilot_state_label(active=None)
            return

        run_prefs = dict(_prefs)
        run_prefs.setdefault("autopilot", {})
        run_prefs["autopilot"]["schedules"] = self.autopilot_schedules
        active = match_autopilot_schedule(run_prefs)
        self.update_autopilot_state_label(active=active)

        key = None
        if active:
            key = f"{active.get('name','')}-{active.get('days','')}-{active.get('from','')}-{active.get('to','')}-{active.get('config','')}"

        # si pas force et pas de changement -> rien à faire
        if (not force) and (key == self._last_autopilot_active_key):
            return

        prev = self._last_autopilot_active_key
        self._last_autopilot_active_key = key

        # Si aucun créneau actif -> on ferme toutes les instances immédiatement
        # Lire comportement overwrite autopilot
        try:
            ap_overwrite = bool(self.chk_autopilot_overwrite.isChecked())
        except Exception:
            ap_overwrite = bool(_prefs.get("autopilot", {}).get("overwrite", False))

        # Si aucun créneau actif -> on ferme seulement si overwrite_autopilot est ON
        if not active:
            if ap_overwrite:
                try:
                    self.kill_and_clear_all()
                except Exception as e:
                    print("Erreur lors de kill_and_clear_all (no active schedule):", e)
            return

        # Si on arrive ici, un créneau est actif.
        # Si on venait d'un autre créneau (prev != key) et prev non None -> on nettoie d'abord.
        # Si on change de créneau → ne nettoyer que si overwrite_autopilot est ON
        if prev is not None and prev != key:
            if ap_overwrite:
                try:
                    self.kill_and_clear_all()
                except Exception as e:
                    print(
                        "Erreur lors de kill_and_clear_all (changement de créneau):", e
                    )

        # Charge la config du planning (chemin résolu : relatif sous SnowMaster/autopilot/ ou absolu)
        cfg_path = resolve_autopilot_config_path(_prefs, active.get("config") or "")
        if not cfg_path or not os.path.exists(cfg_path):
            QMessageBox.warning(self, "Autopilote", f"Config introuvable:\n{cfg_path}")
            return

        autostart = self.autopilot_mode == "load_and_launch"
        # on charge la config : on_load_config gérera l'autostart (avec protection)
        self.on_load_config(path=cfg_path, autostart=autostart)


# ---------------------- HTTP client helper (used by GUI & runner) -------------------
def _post_json(path: str, payload: dict, timeout=2.0):
    url = f"http://{API_HOST}:{API_PORT}{path}"
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url, data=data, headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def send_discord_hook_success():
    """
    Envoie un webhook Discord simple :
    Embed : 🟢 Tout est revenu au VERT !
    """
    # Récupération basique du webhook (à adapter selon ton stockage)
    try:
        discord_cfg = _prefs.get("discord", {})
    except Exception:
        discord_cfg = {}

    webhook_url = str(discord_cfg.get("webhooksuccess") or "").strip()
    enabled = bool(discord_cfg.get("enabled", False))

    if not enabled or not webhook_url:
        print("[DISCORD SUCCESS] Webhook non configuré / désactivé.")
        return

    # Ajouter l'heure précise
    from datetime import datetime

    now_time = datetime.now().strftime("%H:%M:%S")

    # Embed Discord minimal
    payload = {
        "embeds": [
            {
                "description": "🟢 **Tout est revenu au VERT !** Le problème a été résolu.",
                "color": 0x10B981,
                "footer": {"text": f"⏰ {now_time}"},
            }
        ]
    }

    try:
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            webhook_url,
            data=data,
            headers={
                "Content-Type": "application/json",
                "User-Agent": DISCORD_WEBHOOK_USER_AGENT,
            },
        )
        with urllib.request.urlopen(req, timeout=10.0) as resp:
            print(f"[DISCORD SUCCESS] ✓ Envoyé (HTTP {resp.getcode()})")
    except Exception as e:
        print(f"[DISCORD SUCCESS] ✗ Erreur envoi : {e}")


def send_discord_hook():
    """
    Envoie un webhook Discord compact quand le voyant global passe au rouge.
    Format: une seule instance avec ses sous-contrôleurs en rouge.
    """
    print("[DISCORD HOOK] Fonction appelée...")

    try:
        discord_cfg = _prefs.get("discord", {})
    except Exception:
        discord_cfg = {}

    enabled = bool(discord_cfg.get("enabled", False))
    webhook_url = str(discord_cfg.get("webhookreddot") or "").strip()

    if not enabled:
        print("[DISCORD HOOK] Discord désactivé dans les préférences")
        return
    if not webhook_url:
        print("[DISCORD HOOK] Webhook URL vide")
        return

    # Trouver la première instance en rouge avec ses détails
    now = time.time()
    red_instance_title = None
    red_subs = []  # Liste des sous-contrôleurs en rouge
    green_subs = []  # Liste des sous-contrôleurs OK
    main_is_red = False
    total_red_count = 0

    try:
        with _state_lock:
            # D'abord compter le total d'instances en rouge
            for title, inst in _instances.items():
                if inst.stopped or getattr(inst, "manual_empty", False):
                    continue
                age_main = (now - inst.last_heartbeat) if inst.last_heartbeat else 1e9
                is_red = age_main >= HEARTBEAT_RED_S
                if not is_red:
                    for _, info in inst.sub_map.items():
                        try:
                            ts = (
                                float(info.get("ts"))
                                if isinstance(info, dict) and info.get("ts") is not None
                                else float(info)
                            )
                        except Exception:
                            ts = 0.0
                        if (now - ts if ts else 1e9) >= HEARTBEAT_RED_S:
                            is_red = True
                            break
                if is_red:
                    total_red_count += 1

            # Ensuite trouver les détails de la première instance en rouge
            for title, inst in _instances.items():
                if inst.stopped or getattr(inst, "manual_empty", False):
                    continue

                age_main = (now - inst.last_heartbeat) if inst.last_heartbeat else 1e9
                instance_is_red = False

                # Vérifier le heartbeat principal (Master)
                if age_main >= HEARTBEAT_RED_S:
                    instance_is_red = True
                    main_is_red = True

                # Vérifier chaque sous-contrôleur
                for sub_key, info in inst.sub_map.items():
                    try:
                        ts = (
                            float(info.get("ts"))
                            if isinstance(info, dict) and info.get("ts") is not None
                            else float(info)
                        )
                        alias = (
                            info.get("alias", sub_key)
                            if isinstance(info, dict)
                            else sub_key
                        )
                    except Exception:
                        ts = 0.0
                        alias = sub_key

                    age = (now - ts) if ts else 1e9
                    if age >= HEARTBEAT_RED_S:
                        instance_is_red = True
                        red_subs.append(alias)
                    else:
                        green_subs.append(alias)

                if instance_is_red:
                    red_instance_title = title
                    break  # On prend la première instance en rouge
                else:
                    # Reset pour la prochaine instance
                    red_subs = []
                    green_subs = []
                    main_is_red = False

    except Exception as e:
        print(f"[DISCORD HOOK] Erreur lors de l'analyse: {e}")
        return

    if not red_instance_title:
        print("[DISCORD HOOK] Aucune instance en rouge trouvée")
        return

    # Construire le message compact avec retours à la ligne
    parts = [f"**{red_instance_title}**"]

    # Ajouter Master si en rouge
    if main_is_red:
        red_subs.insert(0, "Master")

    # Sous-contrôleurs en rouge
    if red_subs:
        parts.append(f"🔴 {', '.join(red_subs)}")

    # Sous-contrôleurs OK (optionnel, seulement s'il y en a peu)
    if green_subs and len(green_subs) <= 5:
        parts.append(f"🟢 {', '.join(green_subs)}")
    elif green_subs:
        parts.append(f"🟢 {len(green_subs)} OK")

    description = "\n".join(parts)

    # Ajouter le compteur si plusieurs instances en rouge
    if total_red_count > 1:
        description += f"\n+{total_red_count - 1} autre(s)"

    print(f"[DISCORD HOOK] Message: {description}")

    # Ajouter l'heure précise
    from datetime import datetime

    now_time = datetime.now().strftime("%H:%M:%S")

    # Payload compact
    payload = {
        "embeds": [
            {
                "title": "🔴 REDDOT",
                "description": description,
                "color": 16711680,  # Rouge
                "footer": {"text": f"⏰ {now_time}"},
            }
        ]
    }

    try:
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            webhook_url,
            data=data,
            headers={
                "Content-Type": "application/json",
                "User-Agent": DISCORD_WEBHOOK_USER_AGENT,
            },
        )
        with urllib.request.urlopen(req, timeout=10.0) as resp:
            status = resp.getcode()
            resp.read()
            print(f"[DISCORD HOOK] ✓ Envoyé (HTTP {status})")
    except urllib.error.HTTPError as e:
        error_body = ""
        try:
            error_body = e.read().decode("utf-8")
        except Exception:
            pass
        print(f"[DISCORD HOOK] ✗ HTTP {e.code}: {error_body}")
        app_log_warn(f"[DISCORD HOOK] HTTP {e.code}: {error_body}")
    except Exception as e:
        print(f"[DISCORD HOOK] ✗ Erreur: {e}")
        app_log_warn(f"[DISCORD HOOK] envoi échoué: {e}")


def post_json_with_retry(path: str, payload: dict, timeout=2.0, retries=5, delay=0.5):
    url = f"http://{API_HOST}:{API_PORT}{path}"
    data = json.dumps(payload).encode("utf-8")
    for i in range(retries):
        try:
            req = urllib.request.Request(
                url, data=data, headers={"Content-Type": "application/json"}
            )
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception:
            time.sleep(delay)
    # last try without swallowing exception
    req = urllib.request.Request(
        url, data=data, headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


# ---------------------- Runner (integrated) -------------------
CREATE_NEW_CONSOLE = 0x00000010


def acquire_mouse_lock(owner, ttl=120.0, poll=0.5):
    """Loop until we acquire the global mouse lock via HTTP endpoint."""
    while True:
        try:
            resp = post_json_with_retry(
                "/mouse_lock/acquire",
                {"owner": owner, "ttl": ttl},
                timeout=3.0,
                retries=3,
                delay=0.3,
            )
            if resp.get("ok"):
                return True
        except Exception:
            pass
        time.sleep(poll)


def release_mouse_lock(owner):
    try:
        post_json_with_retry(
            "/mouse_lock/release", {"owner": owner}, timeout=2.0, retries=2, delay=0.2
        )
    except Exception:
        pass


def is_top_level_candidate(hwnd):
    try:
        if not win32gui.IsWindow(hwnd):
            return False
        if not win32gui.IsWindowVisible(hwnd):
            return False
        exstyle = win32gui.GetWindowLong(hwnd, win32con.GWL_EXSTYLE)
        if exstyle & win32con.WS_EX_TOOLWINDOW:
            return False
        if win32gui.GetWindow(hwnd, win32con.GW_OWNER):
            return False
        title = win32gui.GetWindowText(hwnd)
        if not title.strip():
            return False
        return True
    except Exception:
        return False


def get_window_area(hwnd):
    try:
        l, t, r, b = win32gui.GetWindowRect(hwnd)
        return max(0, r - l) * max(0, b - t)
    except Exception:
        return 0


def find_hwnds_for_pid(pid):
    hwnds = []

    def enum_handler(hwnd, _):
        try:
            if not is_top_level_candidate(hwnd):
                return
            _, wnd_pid = win32process.GetWindowThreadProcessId(hwnd)
            if wnd_pid == pid:
                hwnds.append(hwnd)
        except Exception:
            pass

    win32gui.EnumWindows(enum_handler, None)
    return hwnds


def find_hwnds_for_pid_allow_empty_title(pid, allow_owned=True):
    """
    Comme find_hwnds_for_pid mais accepte les fenêtres avec titre vide.
    allow_owned=True : inclut les fenêtres avec owner (certaines apps ont une fenêtre principale "owned").
    """
    hwnds = []

    def enum_handler(hwnd, _):
        try:
            if not win32gui.IsWindow(hwnd) or not win32gui.IsWindowVisible(hwnd):
                return
            exstyle = win32gui.GetWindowLong(hwnd, win32con.GWL_EXSTYLE)
            if exstyle & win32con.WS_EX_TOOLWINDOW:
                return
            if not allow_owned and win32gui.GetWindow(hwnd, win32con.GW_OWNER):
                return
            _, wnd_pid = win32process.GetWindowThreadProcessId(hwnd)
            if wnd_pid == pid:
                hwnds.append(hwnd)
        except Exception:
            pass

    win32gui.EnumWindows(enum_handler, None)
    return hwnds


def find_hwnds_for_pid_minimal(pid):
    """
    Toute fenêtre visible appartenant au pid (aucun filtre exstyle/owner).
    Fallback quand find_hwnds_for_pid_allow_empty_title ne trouve rien.
    """
    hwnds = []

    def enum_handler(hwnd, _):
        try:
            if not win32gui.IsWindow(hwnd) or not win32gui.IsWindowVisible(hwnd):
                return
            _, wnd_pid = win32process.GetWindowThreadProcessId(hwnd)
            if wnd_pid == pid:
                hwnds.append(hwnd)
        except Exception:
            pass

    win32gui.EnumWindows(enum_handler, None)
    return hwnds


def get_recent_child_pids(root_pid, since_ts):
    pids = set()
    try:
        root = psutil.Process(root_pid)
        pids.add(root_pid)
        for child in root.children(recursive=True):
            try:
                if child.create_time() >= since_ts - 1:
                    pids.add(child.pid)
            except Exception:
                pids.add(child.pid)
    except psutil.NoSuchProcess:
        pass
    return pids


def screen_area(use_virtual=False):
    if use_virtual:
        l = win32api.GetSystemMetrics(76)
        t = win32api.GetSystemMetrics(77)
        w = win32api.GetSystemMetrics(78)
        h = win32api.GetSystemMetrics(79)
        return max(1, w * h), (l, t, l + w, t + h)
    else:
        w = win32api.GetSystemMetrics(0)
        h = win32api.GetSystemMetrics(1)
        return max(1, w * h), (0, 0, w, h)


def wait_for_windows_with_early_register(
    exe_path,
    title_for_register,
    args=None,
    poll_interval=0.25,
    total_timeout=120.0,
    min_screen_ratio=0.5,
    use_virtual_screen=False,
    early_connexion_image=None,  # <— AJOUT
    early_connexion_confidence=0.3,  # <— AJOUT
):
    cmd = [exe_path] + (args or [])
    start_ts = time.time()
    exe_dir = os.path.dirname(os.path.abspath(exe_path)) if exe_path else None
    p = subprocess.Popen(
        cmd,
        creationflags=CREATE_NEW_CONSOLE,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        text=True,
        cwd=exe_dir,
    )
    # print(f"PID lancé: {p.pid} @ {datetime.fromtimestamp(start_ts).isoformat()}")
    app_log_info(
        "Launched PID=%s @ %s", p.pid, datetime.fromtimestamp(start_ts).isoformat()
    )

    scr_area, _ = screen_area(use_virtual_screen)
    threshold = scr_area * float(min_screen_ratio)

    early_registered = False
    while True:
        now = time.time()
        if now - start_ts > total_timeout:
            raise TimeoutError(
                f"Timeout fenêtre >= {int(min_screen_ratio*100)}% écran."
            )
        candidate_pids = get_recent_child_pids(p.pid, start_ts)

        # === EARLY CLICK sur la 1ʳᵉ petite fenêtre (AnkaBot login) ===
        # On le fait une seule fois par PID, et seulement si une image a été fournie.

        # EARLY REGISTER : enregistrer la petite fenêtre de chargement (pid/hwnd) pour pouvoir
        # la terminer prématurément via "Terminer". Sera écrasé dès que la grande fenêtre
        # est détectée (second /register dans run_snowbot_flow).
        if not early_registered:
            for cpid in list(candidate_pids):
                for h in find_hwnds_for_pid(cpid):
                    try:
                        # Renommer la petite fenêtre de lancement (avant la grande) en
                        # "Lancement de <title>" si elle est visible et de taille inférieure
                        # au seuil de la grande fenêtre.
                        try:
                            if win32gui.IsWindowVisible(h):
                                area = get_window_area(h)
                                if area < threshold:
                                    _set_launch_window_title(h, title_for_register)
                        except Exception:
                            # On ignore toute erreur de renommage pour ne rien casser.
                            pass

                        try:
                            post_json_with_retry(
                                "/register",
                                {
                                    "title": title_for_register,
                                    "pid": int(cpid),
                                    "hwnd": int(h),
                                    "touch": True,
                                },
                                timeout=2.0,
                                retries=2,
                                delay=0.25,
                            )
                        except Exception:
                            _post_json(
                                "/register",
                                {
                                    "title": title_for_register,
                                    "pid": int(cpid),
                                    "hwnd": int(h),
                                    "touch": True,
                                },
                            )
                        early_registered = True
                        break
                    except Exception:
                        pass
                if early_registered:
                    break

        # time.sleep(1)

        # === EARLY CLICK sur la 1ʳᵉ petite fenêtre (AnkaBot login) ===
        if early_registered and early_connexion_image:
            if "clicked_connexion_pids" not in locals():
                clicked_connexion_pids = set()

            # petit délai pour laisser la textbox + bouton se peindre
            time.sleep(0.35)

            pid_tag = None  # ex: "[PID: 72152]"
            # on récupère le tag PID depuis l'early register
            # (si tu as déjà 'cpid' dans la portée, on s'en sert directement)
            # sinon on extraiera depuis candidate_pids
            for cpid in list(candidate_pids):
                pid_tag = f"[PID: {cpid}]"
                if cpid in clicked_connexion_pids:
                    continue
                for h in find_hwnds_for_pid(cpid):
                    if not win32gui.IsWindowVisible(h):
                        continue
                    title_txt = win32gui.GetWindowText(h) or ""
                    area = get_window_area(h)

                    # 1) la fenêtre doit être la "petite" (avant la grande)
                    # 2) son titre doit contenir le PID (évite les faux positifs)
                    if area < threshold and pid_tag in title_txt:
                        try:
                            # Centrer la petite fenêtre sur le premier écran avant le clic
                            # (center_window_on_first_screen utilise SetWindowPos avec HWND_TOP qui met déjà au premier plan)
                            center_window_on_first_screen(h)
                            time.sleep(0.2)

                            ok = click_connexion_button(
                                h,
                                image_path=early_connexion_image,  # ton connexion.png si présent
                                confidence=early_connexion_confidence,  # p.ex. 0.55
                                timeout=1.2,
                                background=True,
                            )
                            if ok:
                                # print(
                                #     f"[EARLY CLICK] Connexion cliquée sur HWND={h} PID={cpid}"
                                # )
                                clicked_connexion_pids.add(cpid)
                                time.sleep(0.3)
                                break
                        except Exception as e:
                            # print(f"[WARN] early connexion click failed: {e}")
                            pass
                # si on a cliqué pour ce PID, on passe au suivant
                if cpid in clicked_connexion_pids:
                    break
                # Main window detection
                for cpid in list(candidate_pids):
                    for h in find_hwnds_for_pid(cpid):
                        area = get_window_area(h)
                        if area >= threshold:
                            title_txt = win32gui.GetWindowText(h)
                            # print(
                            #     f"[{datetime.fromtimestamp(now).isoformat()}] LARGE HWND={h} PID={cpid} Title='{title_txt}' Area={area}"
                            # )
                            return h, cpid

        time.sleep(poll_interval)


def _set_launch_window_title(hwnd, title):
    """
    Renomme une petite fenêtre de lancement en "Lancement de <title>".
    Utilisé pendant la phase de démarrage, avant la grande fenêtre principale.
    """
    try:
        if not win32gui.IsWindow(hwnd) or not win32gui.IsWindowVisible(hwnd):
            return
        win32gui.SetWindowText(hwnd, f"Lancement de {title}")
    except Exception as e:
        # On ignore les erreurs pour ne pas casser le lancement, mais on log si possible.
        try:
            app_log_info(
                "SetWindowText launch failed hwnd=%s title=%s err=%s", hwnd, title, e
            )
        except Exception:
            pass


def _log_empty(msg: str):
    """Log pour le flux instance vide (préfixe [EMPTY] pour filtrage)."""
    try:
        print(f"[EMPTY] {msg}")
        app_log_info(f"[EMPTY] {msg}")
    except Exception:
        print(f"[EMPTY] {msg}")


def _set_empty_instance_window_title(main_hwnd, title, log_prefix=None):
    """
    Renomme la fenêtre de l'instance vide en "{title} - Snowbot" de façon robuste :
    - vérifie que le hwnd est valide,
    - attend un court délai après /register (l'app peut réécrire le titre),
    - plusieurs tentatives espacées pour contrer un éventuel ré-écriture par l'app.
    """
    prefix = f"[{log_prefix}]" if log_prefix else ""
    desired = f"{title} - Snowbot"
    if not win32gui.IsWindow(main_hwnd):
        _log_empty(f"{prefix} SetWindowText ignoré: hwnd={main_hwnd} invalide")
        return
    # Délai après /register pour laisser l'app traiter la réponse avant de forcer le titre
    time.sleep(0.35)
    delays = [0, 0.35, 0.8]
    last_err = None
    for attempt, delay in enumerate(delays):
        if delay > 0:
            time.sleep(delay)
        try:
            win32gui.SetWindowText(main_hwnd, desired)
            if not win32gui.IsWindow(main_hwnd):
                break
            current = (win32gui.GetWindowText(main_hwnd) or "").strip()
            if current == desired:
                _log_empty(
                    f"{prefix} SetWindowText OK (tentative {attempt + 1}) titre={desired!r}"
                )
                return
        except Exception as e:
            last_err = e
            _log_empty(f"{prefix} SetWindowText tentative {attempt + 1} erreur: {e}")
    try:
        final = (
            (win32gui.GetWindowText(main_hwnd) or "").strip()
            if win32gui.IsWindow(main_hwnd)
            else "(hwnd invalide)"
        )
        _log_empty(
            f"{prefix} SetWindowText fin: titre actuel={final!r}"
            + (f" (dernière erreur: {last_err})" if last_err else "")
        )
    except Exception:
        _log_empty(f"{prefix} SetWindowText fin (lecture titre impossible)")


def wait_for_large_window_for_process(
    p,
    start_ts,
    total_timeout=120.0,
    min_screen_ratio=0.5,
    poll_interval=0.25,
    log_progress=True,
    small_title=None,
):
    """
    Attend l'apparition d'une "grande" fenêtre pour le process p (même critère que run_snowbot_flow).
    Utilisé pour les instances vides : pas d'early register, pas de clic connexion, juste attendre
    la fenêtre principale (>= min_screen_ratio de l'écran) pour la renommer.
    Retourne (main_hwnd, main_pid).
    Lance TimeoutError si pas de grande fenêtre avant total_timeout.
    """
    scr_area, _ = screen_area(use_virtual=False)
    threshold = scr_area * float(min_screen_ratio)
    if log_progress:
        _log_empty(
            f"wait_for_large_window pid={p.pid} scr_area={scr_area:.0f} threshold={threshold:.0f} ({min_screen_ratio*100:.0f}% écran) timeout={total_timeout}s"
        )
    poll_count = 0
    while True:
        now = time.time()
        if now - start_ts > total_timeout:
            if log_progress:
                _log_empty(f"TIMEOUT après {total_timeout}s (aucune grande fenêtre)")
            raise TimeoutError(
                f"Timeout fenêtre >= {int(min_screen_ratio * 100)}% écran."
            )
        candidate_pids = get_recent_child_pids(p.pid, start_ts)
        if log_progress and (poll_count == 0 or poll_count % 40 == 0):
            _log_empty(
                f"poll #{poll_count} candidate_pids={candidate_pids} elapsed={now - start_ts:.1f}s"
            )
        for cpid in list(candidate_pids):
            hwnds = find_hwnds_for_pid_allow_empty_title(cpid)
            if log_progress and poll_count > 0 and poll_count % 40 == 0 and hwnds:
                _log_empty(f"  cpid={cpid} hwnds={len(hwnds)}")
            for h in hwnds:
                if not win32gui.IsWindowVisible(h):
                    continue
                area = get_window_area(h)
                # Renommage des petites fenêtres de lancement pour les instances vides :
                # toute fenêtre visible avec une aire positive, inférieure au seuil de
                # la "grande" fenêtre, sera appelée "Lancement de <title>".
                if small_title and area > 0 and area < threshold:
                    try:
                        _set_launch_window_title(h, small_title)
                    except Exception:
                        # On ne casse jamais la boucle de détection à cause du renommage.
                        pass
                if log_progress and poll_count % 40 == 0 and area > 0:
                    try:
                        tit = win32gui.GetWindowText(h) or "(vide)"
                        _log_empty(f"  hwnd={h} area={area} title={tit[:50]!r}")
                    except Exception:
                        _log_empty(f"  hwnd={h} area={area}")
                if area >= threshold:
                    if log_progress:
                        try:
                            tit = win32gui.GetWindowText(h) or "(vide)"
                            _log_empty(
                                f"GRANDE FENÊTRE trouvée hwnd={h} cpid={cpid} area={area} title={tit[:50]!r}"
                            )
                        except Exception:
                            _log_empty(
                                f"GRANDE FENÊTRE trouvée hwnd={h} cpid={cpid} area={area}"
                            )
                    return h, cpid
        poll_count += 1
        time.sleep(poll_interval)


def run_snowbot_flow(
    exe_path,
    controller_path,
    title,
    image_dir=None,
    min_screen_ratio=0.5,
    args=None,
):
    if image_dir is None:
        image_dir = RESOURCES
    try:
        main_hwnd, main_pid = wait_for_windows_with_early_register(
            exe_path=exe_path,
            title_for_register=title,
            args=args,
            poll_interval=0.25,
            total_timeout=120.0,
            min_screen_ratio=min_screen_ratio,
            use_virtual_screen=False,
            early_connexion_image=os.path.join(image_dir, "connexion.png"),  # <— AJOUT
            early_connexion_confidence=0.3,
        )
    except Exception as e:
        # print("Erreur lancement/extraction fenêtre:", e)
        try:
            post_json_with_retry(
                "/log",
                {"title": title, "message": f"Erreur lancement: {e}", "level": "ERROR"},
                retries=2,
                delay=0.2,
            )
        except Exception:
            pass
        return

    # print(
    #     f"Fenêtre principale: HWND={main_hwnd}, Title='{win32gui.GetWindowText(main_hwnd)}', PID={main_pid}"
    # )

    try:
        try:
            post_json_with_retry(
                "/register",
                {
                    "title": title,
                    "pid": int(main_pid) if main_pid else 0,
                    "hwnd": int(main_hwnd),
                    "touch": True,
                },
                retries=2,
                delay=0.2,
            )
        except Exception:
            _post_json(
                "/register",
                {
                    "title": title,
                    "pid": int(main_pid) if main_pid else 0,
                    "hwnd": int(main_hwnd),
                    "touch": True,
                },
            )
    except Exception as e:
        # print("WARN register (main):", e)
        pass

    acquired = False
    try:
        acquire_mouse_lock(owner=title, ttl=180.0)
        acquired = True

        # Mettre la fenêtre au premier plan
        bring_to_front(main_hwnd)
        time.sleep(0.35)

        # Renommer la fenêtre avec le titre de l'instance
        try:
            win32gui.SetWindowText(main_hwnd, f"{title} - Snowbot")
        except Exception:
            pass  # Ignorer les erreurs de renommage

        ok = background_click_robust_by_relative(
            main_hwnd, rx=0.072, ry=0.946, dx=+14, dy=0
        )
        # print("BG robust click:", ok)

        # Laisse un petit délai, puis re-scanne la boîte :
        if ok:
            time.sleep(0.35)
            dlg = find_dialog_for_instance(
                main_hwnd, title_substr="Gestionnaire de comptes", timeout=5.0
            )
            if dlg:
                # print(f"Dialog trouvé: 0x{dlg:08X} '{win32gui.GetWindowText(dlg)}'")
                targets = [
                    "Charger un compte contrôleur",
                    "Charger un compte controleur",
                    "Charger un compte contrôleu",  # tronqué
                    "Charger un compte",
                    "charger un compte contrôleur",
                ]
                ok2 = click_child_by_text(dlg, targets, timeout=2.0)
                # print("Clique 'Charger un compte contrôleur':", ok2)

                # time.sleep(1)

                ok_close = close_gestionnaire_dialog(dlg)
                # print("Fermeture gestionnaire:", ok_close)
            else:
                pass
                # print("Boîte 'Gestionnaire de comptes' introuvable.")

            time.sleep(0.5)
            # On vise la 1ère ligne visible du panneau gauche (souvent "Contrôleur 1")
            ok_sel = activate_controller_by_index_background_strong(main_hwnd, index=0)
            # print("Activate contrôleur (strong):", ok_sel)

            time.sleep(0.5)

            # 1) Clic async sur l’icône “Charger un script” (ta fonction existante)
            ok_icon = click_script_loader_icon_async(main_hwnd, index_from_left=2)
            # print("Click icône 'Charger un script' (async):", ok_icon)

            time.sleep(0.5)

            ok = open_file_via_dialog_bg(main_hwnd, controller_path)
            # print("Open via dialog:", ok)

            time.sleep(0.5)

            ok_icon = click_script_loader_icon_async(main_hwnd, index_from_left=3)
            # print("Click icône 'LANCER un script' (async):", ok_icon)

    finally:
        if acquired:
            release_mouse_lock(owner=title)


def run_snowbot_flow_panic(title: str):
    """
    Variante PANIC basée sur une instance DÉJÀ lancée :
    - on part du PID/HWND existant de l'instance
    - PAS de nouveau lancement, PAS de /register ou autre POST JSON
    - utilise le contrôleur global configuré dans les préférences (panic_controller)
    - sélection directe du 1er contrôleur
    - chargement puis lancement du script contrôleur PANIC
    """

    # Contrôleur PANIC global : si non défini ou inexistant, on stoppe
    global PANIC_CONTROLLER_PATH
    controller_path = PANIC_CONTROLLER_PATH
    if not controller_path or not os.path.exists(controller_path):
        return

    # Récupérer l'état de l'instance + PID/HWND courant
    with _state_lock:
        inst = _instances.get(title)
        if not inst or not inst.pid:
            return
        pid = int(inst.pid)
        main_hwnd = inst.hwnd

    # Si on n'a pas encore de HWND enregistré, on le récupère via le PID
    if not main_hwnd:
        try:
            main_hwnd = get_main_hwnd(pid)
        except Exception:
            main_hwnd = None

        if not main_hwnd:
            return

        # Sauvegarder le HWND trouvé dans l'état global
        with _state_lock:
            inst2 = _instances.get(title)
            if inst2:
                inst2.hwnd = main_hwnd
                _instances[title] = inst2

    # acquired = False
    # try:
    # acquire_mouse_lock(owner=title, ttl=180.0)
    # acquired = True

    # bring_to_front(main_hwnd)
    time.sleep(0.35)

    # ok = background_click_robust_by_relative(
    #     main_hwnd, rx=0.072, ry=0.946, dx=+14, dy=0
    # )

    # if ok:
    time.sleep(0.5)
    ok_sel = activate_controller_by_index_background_strong(main_hwnd, index=0)

    time.sleep(0.5)

    if ok_sel:
        # 1) Clic async sur l’icône “Charger un script”
        click_script_loader_icon_async(main_hwnd, index_from_left=2)
        time.sleep(0.5)

        # 2) Charger le fichier du contrôleur
        open_file_via_dialog_bg(main_hwnd, controller_path)
        time.sleep(0.5)

        # 3) Clic async sur l’icône “LANCER un script”
        click_script_loader_icon_async(main_hwnd, index_from_left=3)
    # finally:
    #     if acquired:
    #         release_mouse_lock(owner=title)


# ======================== Process scan & restore (NEW) ===========================
def _normalize_cmdlist(cmdline):
    """Assure une liste de strings propre à parser."""
    if not cmdline:
        return []
    if isinstance(cmdline, str):
        # tente un split intelligent (garde les quotes)
        try:
            return shlex.split(cmdline)
        except Exception:
            return cmdline.split()
    if isinstance(cmdline, (list, tuple)):
        out = []
        for e in cmdline:
            if isinstance(e, str):
                # si l'élément contient des espaces et des quotes, tenter split local
                if " " in e and ('"' in e or "'" in e):
                    try:
                        out.extend(shlex.split(e))
                    except Exception:
                        out.append(e)
                else:
                    out.append(e)
            else:
                try:
                    out.append(str(e))
                except Exception:
                    pass
        return out
    try:
        return [str(cmdline)]
    except Exception:
        return []


def _parse_arg_from_cmdline(cmdline, name: str):
    """
    Récupère la valeur de --name (ou -name, /name, --name=, /name:) dans cmdline.
    Supporte:
      --title value  --title=value  -title value  /title:value  /title="a b"
    """
    if not cmdline:
        return None
    key_long = f"--{name}"
    key_short = f"-{name}"
    key_slash = f"/{name}"
    cmdlist = _normalize_cmdlist(cmdline)

    for i, token in enumerate(cmdlist):
        t = token.strip()
        tl = t.lower()

        # forms: --name=value  or -name=value or /name=value or /name:value
        if (
            tl.startswith(key_long + "=")
            or tl.startswith(key_short + "=")
            or tl.startswith(key_slash + "=")
            or tl.startswith(key_slash + ":")
        ):
            # split on first '=' or ':'
            if "=" in t:
                return t.split("=", 1)[1].strip().strip('"').strip("'")
            if ":" in t:
                return t.split(":", 1)[1].strip().strip('"').strip("'")

        # form: --name (next token is value)
        if tl == key_long or tl == key_short or tl == key_slash:
            if i + 1 < len(cmdlist):
                return cmdlist[i + 1].strip().strip('"').strip("'")

        # sometimes token like --title:"My Title" or --title:"My Title"
        if (
            tl.startswith(key_long + ":")
            or tl.startswith(key_short + ":")
            or tl.startswith(key_slash + ":")
        ):
            return t.split(":", 1)[1].strip().strip('"').strip("'")

        # token might contain --title:"My Title" glued (without space)
        if key_long in tl and ("=" in t or ":" in t):
            if "=" in t:
                return t.split("=", 1)[1].strip().strip('"').strip("'")
            if ":" in t:
                return t.split(":", 1)[1].strip().strip('"').strip("'")

    return None


def _has_flag_in_cmdline(cmdline, name: str) -> bool:
    """
    Retourne True si un flag (sans valeur) apparaît dans la ligne de commande.
    Ex. --empty, -empty, /empty pour name="empty".
    """
    if not cmdline:
        return False
    key_long = f"--{name}"
    key_short = f"-{name}"
    key_slash = f"/{name}"
    cmdlist = _normalize_cmdlist(cmdline)
    for token in cmdlist:
        t = token.strip().lower()
        if t == key_long or t == key_short or t == key_slash:
            return True
    return False


def _all_subprocess_cmdlines(root_pid):
    """Collecte cmdlines de root + tous les enfants récursifs (liste de (pid, cmdline))."""
    out = []
    try:
        root = psutil.Process(root_pid)
        try:
            out.append((root.pid, root.cmdline() or []))
        except Exception:
            # fallback: parfois cmdline non lisible pour le pid root
            try:
                info = root.as_dict(attrs=["cmdline"])
                out.append((root.pid, info.get("cmdline") or []))
            except Exception:
                out.append((root.pid, []))
        for child in root.children(recursive=True):
            try:
                out.append((child.pid, child.cmdline() or []))
            except Exception:
                try:
                    info = child.as_dict(attrs=["cmdline"])
                    out.append((child.pid, info.get("cmdline") or []))
                except Exception:
                    out.append((child.pid, []))
    except psutil.NoSuchProcess:
        pass
    except Exception:
        pass
    return out


def _debug_print_proc(info: dict):
    pid = info.get("pid")
    name = info.get("name") or ""
    exe = info.get("exe") or ""
    title = info.get("title")
    controller = info.get("controller")
    empty = info.get("empty", False)
    cmdline = info.get("cmdline")
    try:
        cmdstr = " ".join(_normalize_cmdlist(cmdline))
    except Exception:
        cmdstr = str(cmdline)
    print(
        f"[PROC] PID={pid} Name='{name}' Exe='{exe}' Title={title} Controller={controller} empty={empty} Cmdline='{cmdstr}'"
    )


def scan_snowbot_processes_by_cmdline(verbose: bool = True) -> Dict[int, dict]:
    """
    Heuristique robuste :
      - match si nom/exe correspond à EXE configuré (préféré)
      - fallback heuristique si EXE n'est pas configuré (dépend de appVariant)
      - récupère cmdlines root + enfants pour parser --title/--controller
    """
    out: Dict[int, dict] = {}
    me_pid = os.getpid()
    me_user = None
    try:
        me_user = psutil.Process().username()
    except Exception:
        me_user = None

    target_basename = _get_configured_client_basename()
    # Snowbot : ne pas considérer SnowMaster.exe comme client (c'est notre GUI)
    if (
        APP_VARIANT == "snowbot"
        and target_basename
        and (
            "snowmaster" in (target_basename or "")
            or target_basename == "snowmaster.exe"
        )
    ):
        target_basename = None

    access_denied_count = 0

    for p in psutil.process_iter(["pid", "name", "exe", "username"]):
        try:
            pid = p.info.get("pid")
            if not pid or pid == me_pid:
                continue

            name = (p.info.get("name") or "") or ""
            exe = (p.info.get("exe") or "") or ""
            lname = name.lower() if name else ""
            lexe = exe.lower() if exe else ""

            # heuristiques de sélection initiale (permissif mais ciblé AnkaBot)
            matched = False
            if target_basename:
                try:
                    if (
                        lname == target_basename
                        or os.path.basename(exe).lower() == target_basename
                    ):
                        matched = True
                except Exception:
                    pass

            if not matched:
                base = os.path.basename(exe).lower() if exe else ""
                # Auxiliaires connus (certains clients spawn des handlers)
                if base in (
                    "application_v2.0.exe",
                    "defaulthandler.exe",
                    "applicationhandler.exe",
                ):
                    matched = True

                # Fallback uniquement si EXE non configuré
                if not matched and not target_basename:
                    if APP_VARIANT == "ankamaster":
                        # 1) vrai binaire d'AnkaBot
                        if base == "ankabot.exe" or "ankabot.exe" in lname:
                            matched = True
                        # 2) éviter Ankama Launcher
                        elif ("anka" in lname or "anka" in (lexe or "")) and (
                            "launcher" not in lname
                        ):
                            matched = True
                    else:
                        # Snowbot
                        if base == "snowbot.exe" or "snowbot.exe" in lname:
                            matched = True
                        elif "snowbot" in lname or "snowbot" in (lexe or ""):
                            matched = True

            if not matched:
                continue

            # Snowbot : ne jamais inclure SnowMaster (notre GUI) comme instance
            if APP_VARIANT == "snowbot" and (
                "snowmaster" in lname
                or "snowmaster" in lexe
                or base == "snowmaster.exe"
            ):
                continue

            # optional: limiter au même user (évite system/service)
            if me_user and p.info.get("username") and p.info.get("username") != me_user:
                # skip other users
                continue

            # récupère cmdlines du proc et de ses enfants, puis parse pour title/controller
            title = None
            controller = None
            cmd_candidates = []
            try:
                cmd_candidates = _all_subprocess_cmdlines(pid)
            except Exception:
                cmd_candidates = []
            # always also attempt p.cmdline() directly
            try:
                raw = p.cmdline()
                if raw:
                    cmd_candidates.insert(0, (pid, raw))
            except Exception:
                pass

            # parse through collected cmdlines
            is_empty = False
            for cpid, cl in cmd_candidates:
                if not cl:
                    continue
                t = _parse_arg_from_cmdline(cl, "title")
                c = _parse_arg_from_cmdline(cl, "controller")
                if t and not title:
                    title = t
                if c and not controller:
                    controller = c
                if _has_flag_in_cmdline(cl, "empty"):
                    is_empty = True
                # stop early if both found
                if title and controller:
                    break

            # best-effort hwnd
            try:
                hwnd = get_main_hwnd(pid)
            except Exception:
                hwnd = None

            rec = {
                "pid": pid,
                "exe": exe,
                "name": name,
                "cmdline": [
                    cl for (_p, cl) in cmd_candidates
                ],  # list of cmdline lists for debug
                "title": title,
                "controller": controller,
                "empty": is_empty,
                "hwnd": hwnd,
                "username": p.info.get("username"),
            }
            out[pid] = rec

        except psutil.AccessDenied:
            access_denied_count += 1
            continue
        except psutil.NoSuchProcess:
            continue
        except Exception:
            continue

    if access_denied_count:
        print(
            f"[WARN] {access_denied_count} processus ignorés (AccessDenied). Lance en admin si besoin."
        )

    if verbose:
        if not out:
            print("[scan] Aucun processus candidat trouvé par les heuristiques.")
        else:
            print(f"[scan] {len(out)} processus candidats (debug list):")
            for info in out.values():
                _debug_print_proc(info)

    return out


def find_processes_by_title(title: str) -> List[dict]:
    """Retourne la liste des processus AnkaBot dont l'argument --title correspond."""
    if not title:
        return []
    try:
        processes = scan_snowbot_processes_by_cmdline(verbose=False)
    except Exception as e:
        app_log_error(f"scan_snowbot_processes_by_cmdline failed: {e}")
        return []
    title_norm = title.strip().lower()
    if not title_norm:
        return []
    matches: List[dict] = []
    for info in processes.values():
        proc_title = str(info.get("title") or "").strip().lower()
        if proc_title and proc_title == title_norm:
            matches.append(info)
    return matches


def terminate_process_tree(pid: int, timeout: float = 5.0):
    """Tente de terminer un processus et tous ses enfants."""
    try:
        proc = psutil.Process(int(pid))
    except (
        psutil.NoSuchProcess,
        psutil.AccessDenied,
        psutil.ZombieProcess,
        ValueError,
    ):
        return

    try:
        children = proc.children(recursive=True)
    except Exception:
        children = []

    for child in children:
        try:
            child.terminate()
        except Exception:
            pass

    try:
        proc.terminate()
    except Exception:
        pass

    procs = children + [proc]
    try:
        gone, alive = psutil.wait_procs(procs, timeout=timeout)
    except Exception:
        alive = []
    for p in alive:
        try:
            p.kill()
        except Exception:
            pass


def restore_running_instances_from_cmdline():
    """
    Scanne les processus candidats et restaure les instances détectées.
    - Si le processus paraît être SnowMaster (même exe / même nom) et que ce n'est PAS
      le processus courant, on tente de le terminer (on ne le restaure pas).
    - Les instances restaurées n'ont PAS de last_heartbeat (set to 0.0) et awaiting_first_hb=True,
      ainsi l'UI affichera '-' pour Last Update jusqu'à réception d'un heartbeat réel.
    """
    found = scan_snowbot_processes_by_cmdline()
    if not found:
        print("[restore] Aucun processus cible trouvé.")
        return

    me_pid = os.getpid()
    # Chemin de l'exécutable courant (si disponible)
    try:
        me_exe = psutil.Process(me_pid).exe()
    except Exception:
        me_exe = None
    me_name = None
    try:
        me_name = psutil.Process(me_pid).name().lower()
    except Exception:
        me_name = None

    restored = 0
    killed_old_snow = 0

    with _state_lock:
        for pid, info in list(found.items()):
            try:
                # Ignore current process (sécurité)
                if not pid or pid == me_pid:
                    continue

                name = (info.get("name") or "") or ""
                exe = (info.get("exe") or "") or ""
                lname = name.lower() if name else ""
                lexe = exe.lower() if exe else ""
                # Filtre final : ne restaurer que le vrai client (basé sur la pref `exe` si dispo)
                base_exe = os.path.basename(exe).lower() if exe else ""
                configured_client = _get_configured_client_basename()
                if configured_client:
                    # parfois le champ exe est vide : on retombe sur le "name"
                    if not (
                        base_exe == configured_client or lname == configured_client
                    ):
                        continue
                else:
                    # fallback sécurité (quand `exe` n'est pas configuré)
                    if APP_VARIANT == "ankamaster":
                        if not (base_exe == "ankabot.exe" or lname == "ankabot.exe"):
                            continue
                    else:
                        if not (base_exe == "snowbot.exe" or lname == "snowbot.exe"):
                            continue

                # Détection "SnowMaster" heuristique : même exe que le GUI courant
                is_snowmaster_like = False
                try:
                    if (
                        me_exe
                        and exe
                        and os.path.normcase(os.path.abspath(exe))
                        == os.path.normcase(os.path.abspath(me_exe))
                    ):
                        is_snowmaster_like = True
                    elif me_name and (
                        any(tok in lname for tok in MASTER_GUI_NAME_TOKENS)
                        or any(tok in lexe for tok in MASTER_GUI_NAME_TOKENS)
                    ):
                        # fallback heuristique
                        is_snowmaster_like = True
                except Exception:
                    pass

                # Ne skip que notre propre GUI (AnkaMaster), pas AnkaBot
                is_gui_like = False
                try:
                    if (
                        me_exe
                        and exe
                        and os.path.normcase(os.path.abspath(exe))
                        == os.path.normcase(os.path.abspath(me_exe))
                    ):
                        is_gui_like = True
                    else:
                        base_me = (me_name or "").lower()
                        base_exe = os.path.basename(exe).lower() if exe else ""
                        # on ne considère GUI-like que si c'est bien notre GUI (python/AnkaMaster packagé),
                        # pas les binaires "AnkaBot.exe" et ses handlers
                        if any(
                            tok in (lname or "") for tok in MASTER_GUI_NAME_TOKENS
                        ) or any(tok in base_exe for tok in MASTER_GUI_NAME_TOKENS):
                            is_gui_like = True
                except Exception:
                    pass

                if is_gui_like:
                    # autre instance de la GUI -> on ne restaure pas
                    continue

                title = info.get("title")
                controller = info.get("controller")
                is_empty_instance = bool(info.get("empty", False))

                # si pas de title, créer un titre dérivé stable
                if not title:
                    if controller:
                        try:
                            cb = os.path.basename(controller) or "controller"
                            title = f"{cb}@{pid}"
                        except Exception:
                            title = f"unknown@{pid}"
                    else:
                        # fallback : name@pid
                        title = f"{info.get('name') or 'proc'}@{pid}"

                # évite override si l'instance existe déjà
                if title in _instances:
                    inst = _instances[title]
                    # compléter pid/hwnd si manquants
                    if (not inst.pid) and info.get("pid"):
                        try:
                            inst.pid = int(info.get("pid"))
                        except:
                            pass
                    if (not inst.hwnd) and info.get("hwnd"):
                        try:
                            inst.hwnd = int(info.get("hwnd"))
                        except:
                            pass

                    if is_empty_instance:
                        inst.manual_empty = True
                        inst.restored_recently = False
                    else:
                        inst.restored_recently = True
                    inst.last_heartbeat = 0.0
                    inst.awaiting_first_hb = False if is_empty_instance else True

                    # sauvegarde et FORCER une mise à jour UI pour refléter l'état restauré
                    _instances[title] = inst
                    try:
                        bus.instance_updated.emit(title)
                    except Exception:
                        pass
                    continue

                # Crée et enregistre l'instance restaurée
                inst = InstanceState(title)
                try:
                    inst.pid = int(info.get("pid") or 0)
                except Exception:
                    inst.pid = None
                try:
                    inst.hwnd = int(info.get("hwnd") or 0) if info.get("hwnd") else None
                except Exception:
                    inst.hwnd = None

                inst.controller_path = controller or None
                inst.exe_path = info.get("exe") or None
                inst.images_dir = None
                inst.last_heartbeat = 0.0
                inst.stopped = False
                if is_empty_instance:
                    inst.manual_empty = True
                    inst.awaiting_first_hb = False
                    inst.restored_recently = False
                else:
                    inst.manual_empty = False
                    inst.awaiting_first_hb = True
                    inst.restored_recently = True
                _instances[title] = inst

                try:
                    bus.new_instance.emit(title)
                    bus.instance_updated.emit(title)
                except Exception:
                    pass

                restored += 1

            except Exception as e:
                print("restore error for pid", pid, e)
                continue

    if killed_old_snow:
        print(f"[restore] {killed_old_snow} ancien(s) SnowMaster tenté(s) kill.")
    if restored:
        print(f"[restore] {restored} instance(s) restaurée(s).")
    else:
        if not killed_old_snow:
            print("[restore] Aucun processus restaure (filtre final).")


# ======================== CHECK ADMIN ============================
def is_user_admin() -> bool:
    try:
        return ctypes.windll.shell32.IsUserAnAdmin() != 0
    except Exception:
        return False


def _pythonw_path():
    py = sys.executable
    folder = os.path.dirname(py)
    candidate = os.path.join(folder, "pythonw.exe")
    return candidate if os.path.exists(candidate) else None


def ensure_admin_or_exit():
    if is_user_admin():
        return

    if getattr(sys, "frozen", False):
        executable = sys.executable
        params = " ".join(f'"{arg}"' for arg in sys.argv[1:])
    else:
        pw = _pythonw_path()
        if pw:
            executable = pw
        else:
            executable = sys.executable
        script = os.path.abspath(sys.argv[0])
        tail = " ".join(f'"{arg}"' for arg in sys.argv[1:])
        params = f'"{script}"' + (f" {tail}" if tail else "")

    try:
        ret = ctypes.windll.shell32.ShellExecuteW(
            None, "runas", executable, params, os.getcwd(), 1
        )
        if int(ret) <= 32:
            ctypes.windll.user32.MessageBoxW(
                None,
                "Les droits administrateur sont requis pour lancer l'application.\nLe processus va s'arrêter.",
                "Elevation requise",
                0x10,
            )
            sys.exit(1)
    except Exception as e:
        ctypes.windll.user32.MessageBoxW(
            None,
            f"Erreur lors de la demande d'élévation : {e}\nLe processus va s'arrêter.",
            "Erreur",
            0x10,
        )
        sys.exit(1)

    sys.exit(0)


# ======================== startup helpers ============================
def load_paths():
    global RESOURCES, EXE, HEARTBEAT_RED_S, PANIC_CONTROLLER_PATH
    try:
        if os.path.exists(PREFS_FILE):
            with open(PREFS_FILE, "r", encoding="utf-8") as f:
                prefs = json.load(f)
            # Dossier d'images : sous-répertoire "images" du dossier courant (SnowMaster).
            RESOURCES = IMAGES_DIR
            # Préférence d'exécutable :
            #   - nouveau champ : "client_exe" (chemin absolu complet de Nvidia.exe)
            #   - compatibilité : on accepte encore "exe" si présent.
            exe_value = (prefs.get("client_exe") or prefs.get("exe") or "").strip()
            EXE = resolve_path_from_bot_root(
                prefs,
                exe_value or "Nvidia.exe",
                "Nvidia.exe",
            )
            HEARTBEAT_RED_S = prefs.get("reddot", HEARTBEAT_RED_S)
            PANIC_CONTROLLER_PATH = (prefs.get("panic_controller") or "").strip()
    except Exception as e:
        print("WARN load_paths:", e)


def _ensure_playwright_ready():
    """
    Vérifie que Playwright + Chromium (headless) sont prêts sur cette machine.
    - Force un dossier de navigateurs dédié : SnowMaster/browsers/ (writable).
    - Migration : si l'ancien dossier pw-browsers existe et browsers non, on le renomme.
    - On n'installe que le strict minimum : chromium headless shell (--only-shell), pas le full browser ni ffmpeg.
    - ffmpeg n'est pas utilisé par cette app (pas d'enregistrement vidéo).
    Retourne: (ok: bool, err: str|None)
    """
    browsers_dir = os.path.join(ANKADIR, "browsers")
    pw_browsers_legacy = os.path.join(ANKADIR, "pw-browsers")

    try:
        # 1) Migration : ancien dossier pw-browsers -> browsers (une seule fois)
        if os.path.isdir(pw_browsers_legacy) and not os.path.isdir(browsers_dir):
            try:
                os.rename(pw_browsers_legacy, browsers_dir)
            except Exception:
                pass
        # 2) Dossier stable pour les navigateurs (évite permissions, même chemin en .exe)
        os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", browsers_dir)
        os.makedirs(browsers_dir, exist_ok=True)
    except Exception:
        pass

    # 3) Essayer de lancer Chromium tout de suite (si déjà installé, ça suffit)
    try:
        from playwright.sync_api import sync_playwright

        with sync_playwright() as pw:
            b = pw.chromium.launch(
                headless=True
            )  # headless uniquement -> headless shell suffit
            b.close()
        return True, None
    except Exception as e_launch:
        # 4) Pas prêt -> installer uniquement le headless shell (strict minimum, pas ffmpeg)
        #    --only-shell : évite de télécharger le full Chromium (headless suffit pour notre usage)
        os.environ["PLAYWRIGHT_BROWSERS_PATH"] = (
            browsers_dir  # force notre dossier pour l'install
        )
        install_args = ["playwright", "install", "chromium", "--only-shell"]
        try:
            from playwright.__main__ import main as pw_cli_main

            old_argv = list(sys.argv)
            sys.argv = install_args
            try:
                pw_cli_main()
            finally:
                sys.argv = old_argv
        except Exception as e_cli:
            if getattr(sys, "frozen", False):
                return (
                    False,
                    f"{e_launch}\nInstall failed (frozen): {e_cli}\nEn .exe, installez Playwright/Chromium manuellement ou incluez-les au packaging.",
                )
            try:
                subprocess.run(
                    [
                        sys.executable,
                        "-m",
                        "playwright",
                        "install",
                        "chromium",
                        "--only-shell",
                    ],
                    check=True,
                    env={**os.environ, "PLAYWRIGHT_BROWSERS_PATH": browsers_dir},
                )
            except Exception as e_sub:
                return (
                    False,
                    f"{e_launch}\nInstall failed: {e_cli}\nSubprocess failed: {e_sub}",
                )

        # 5) Re-tenter un launch après install
        try:
            from playwright.sync_api import sync_playwright

            with sync_playwright() as pw:
                b = pw.chromium.launch(headless=True)
                b.close()
            return True, None
        except Exception as e_after:
            return False, f"Install seemed to run but launch still fails: {e_after}"


# ======================== FENÊTRE DE CHARGEMENT ============================
class LoadingSplash(QWidget):
    """
    Petite fenêtre de chargement affichée au démarrage pour éviter le fond blanc
    et donner un retour visuel (barre de progression) pendant la préparation de l'interface.
    Même direction artistique que l'app : fond sombre, bleu accent, barre de chargement.
    """

    finished = Signal()

    def __init__(self, app_name: str, parent=None):
        super().__init__(parent)
        self.setWindowFlags(
            Qt.FramelessWindowHint | Qt.WindowStaysOnTopHint | Qt.Window
        )
        # Fond de la fenêtre entièrement transparent : nous dessinons un conteneur
        # arrondi à l'intérieur pour avoir un vrai effet de carte moderne.
        self.setAttribute(Qt.WA_TranslucentBackground, True)
        self.setFixedSize(440, 210)
        self._progress = 0
        self._timer = None
        self._finishing = False
        self._finish_timer = None
        self._app_name = app_name

        # Conteneur principal arrondi (carte) à l'intérieur de la fenêtre transparente
        outer_layout = QVBoxLayout(self)
        outer_layout.setContentsMargins(0, 0, 0, 0)
        outer_layout.setSpacing(0)

        self._container = QFrame(self)
        self._container.setObjectName("SplashContainer")
        outer_layout.addWidget(self._container)

        # Ombre portée douce pour mieux détacher la carte du fond.
        shadow = QGraphicsDropShadowEffect(self._container)
        shadow.setBlurRadius(42)
        shadow.setOffset(0, 0)
        shadow.setColor(QColor(15, 23, 42, 230))
        self._container.setGraphicsEffect(shadow)

        container_layout = QVBoxLayout(self._container)
        container_layout.setSpacing(18)
        container_layout.setContentsMargins(26, 20, 26, 20)

        # En-tête façon fenêtre moderne
        header = QHBoxLayout()
        header.setContentsMargins(2, 0, 2, 4)

        self._title = QLabel(app_name)
        self._title.setObjectName("SplashTitle")
        self._title.setStyleSheet(
            "font-size: 20px; font-weight: 800; color: #60a5fa;"
            "background: transparent; border: none;"
        )
        self._title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        header.addStretch(1)
        header.addWidget(self._title)
        header.addStretch(1)

        container_layout.addLayout(header)

        # Sous-titre centré
        self._subtitle = QLabel("Préparation de l'interface...")
        self._subtitle.setStyleSheet(
            "font-size: 13px; color: #d1d5db; background: transparent; border: none;"
        )
        self._subtitle.setAlignment(Qt.AlignmentFlag.AlignCenter)
        container_layout.addWidget(self._subtitle)

        # Barre de progression (accent vert, look moderne)
        self._progress_bar = QProgressBar()
        self._progress_bar.setRange(0, 100)
        self._progress_bar.setValue(0)
        self._progress_bar.setTextVisible(True)
        self._progress_bar.setFormat("%p%")
        self._progress_bar.setStyleSheet(
            """
            QProgressBar {
                background-color: #020617;
                border: 1px solid #16a34a;
                border-radius: 6px;
                height: 16px;
                text-align: center;
            }
            QProgressBar::chunk {
                background: qlineargradient(x1:0, y1:0, x2:1, y2:0,
                    stop:0 #22c55e, stop:1 #16a34a);
                border-radius: 4px;
            }
            """
        )
        container_layout.addWidget(self._progress_bar)

        # Style du conteneur : fond légèrement plus clair, bords très arrondis
        self._container.setStyleSheet(
            """
            QFrame#SplashContainer {
                background-color: #111827;
                border-radius: 24px;
                border: 1px solid rgba(148,163,184,0.75);
            }
            """
        )
        self.setWindowTitle(app_name)

    def start_animation(self):
        """Lance la simulation de progression (0 -> 95%) jusqu'à ce que finish() soit appelé."""
        self._progress = 0
        self._progress_bar.setValue(0)
        self._finishing = False
        if self._finish_timer is not None:
            self._finish_timer.stop()
            self._finish_timer = None
        if self._timer is not None:
            self._timer.stop()
        self._timer = QTimer(self)
        self._timer.timeout.connect(self._tick)
        # Tick plus fréquent avec pas plus petit pour un mouvement plus fluide
        self._timer.start(45)

    def _tick(self):
        if self._progress >= 95:
            self._timer.stop()
            self._timer = None
            return
        # Petits pas pour donner une impression de chargement continu
        self._progress = min(95, self._progress + 2)
        self._progress_bar.setValue(self._progress)

    def finish(self):
        """
        Fait progresser la barre jusqu'à 100 % de façon fluide, puis
        émet le signal finished et ferme la fenêtre.
        """
        if self._finishing:
            return
        self._finishing = True

        # Arrêter l'animation lente si elle tourne encore.
        if self._timer is not None:
            self._timer.stop()
            self._timer = None

        # Si on est déjà à 100 %, on ferme directement.
        if self._progress >= 100:
            self._progress_bar.setValue(100)
            self._subtitle.setText("Prêt.")
            self.finished.emit()
            self.close()
            return

        # Sinon, on termine la progression avec une petite animation rapide.
        self._finish_timer = QTimer(self)
        self._finish_timer.timeout.connect(self._finish_tick)
        # Animation de fin un peu plus fine également
        self._finish_timer.start(28)

    def _finish_tick(self):
        step = 4
        self._progress = min(100, self._progress + step)
        self._progress_bar.setValue(self._progress)
        if self._progress >= 100:
            if self._finish_timer is not None:
                self._finish_timer.stop()
                self._finish_timer = None
            self._subtitle.setText("Prêt.")
            self.finished.emit()
            self.close()

    def center_on_screen(self):
        from PySide6.QtGui import QScreen

        screen = QApplication.primaryScreen()
        if screen is not None:
            gr = screen.availableGeometry()
            self.move(
                gr.x() + (gr.width() - self.width()) // 2,
                gr.y() + (gr.height() - self.height()) // 2,
            )


def _parse_version(version_str: str) -> tuple:
    """Transforme '1.4.2' en tuple comparable (1,4,2)."""
    parts = []
    for chunk in str(version_str).split("."):
        try:
            parts.append(int(chunk))
        except ValueError:
            parts.append(0)
    return tuple(parts)


def _is_remote_version_newer(remote: str, local: str) -> bool:
    return _parse_version(remote) > _parse_version(local)


def maybe_check_for_update_and_run_updater(parent_widget=None) -> bool:
    """
    Vérifie le manifest distant, propose une mise à jour si une version plus
    récente existe, lance l'updater puis quitte le process.

    Retourne:
        True  -> continuer le démarrage normal
        False -> une mise à jour a été lancée / erreur bloquante, ne pas continuer
    """
    if not UPDATE_MANIFEST_URL:
        return True

    try:
        with urllib.request.urlopen(UPDATE_MANIFEST_URL, timeout=4.0) as resp:
            data = resp.read().decode("utf-8", errors="replace")
        manifest = json.loads(data)
        remote_version = str(manifest.get("version", "")).strip()
        download_url = manifest.get("download_url", "")
        if not remote_version or not download_url:
            return True
    except Exception:
        # En cas d'erreur réseau ou de parsing, on démarre simplement l'app.
        return True

    if not _is_remote_version_newer(remote_version, APP_VERSION):
        return True

    # Demander à l'utilisateur s'il veut mettre à jour maintenant.
    msg = (
        f"Une nouvelle version de {APP_DISPLAY_NAME} est disponible.\n\n"
        f"Version actuelle : {APP_VERSION}\n"
        f"Nouvelle version : {remote_version}\n\n"
        "Voulez-vous télécharger et installer cette mise à jour maintenant ?"
    )
    box = QMessageBox(parent_widget)
    box.setIcon(QMessageBox.Question)
    box.setWindowTitle(f"Mise à jour {APP_DISPLAY_NAME}")
    box.setText(msg)
    box.setStandardButtons(QMessageBox.Yes | QMessageBox.No)
    box.setDefaultButton(QMessageBox.Yes)
    choice = box.exec()
    if choice != QMessageBox.Yes:
        return True

    exe_dir = os.path.dirname(sys.executable)
    updater_path = os.path.join(exe_dir, UPDATER_EXE_NAME)
    if not os.path.exists(updater_path):
        QMessageBox.warning(
            parent_widget,
            "Mise à jour",
            f"Programme de mise à jour introuvable :\n{updater_path}",
        )
        return True

    try:
        args = [
            updater_path,
            "--target-exe",
            sys.executable,
            "--download-url",
            download_url,
            "--version",
            remote_version,
        ]
        subprocess.Popen(args, cwd=exe_dir)
    except Exception as e:
        QMessageBox.warning(
            parent_widget,
            "Mise à jour",
            f"Impossible de lancer la mise à jour : {e}",
        )
        return True

    # On quitte immédiatement pour laisser l'updater remplacer l'exe.
    sys.exit(0)


# ======================== MAIN ============================
def main():
    load_paths()
    # start flask server in a background thread
    threading.Thread(target=run_server, daemon=True).start()

    # restore any already-running snowbot processes launched with --title
    try:
        restore_running_instances_from_cmdline()
    except Exception as e:
        print("WARN restore_running_instances_from_cmdline:", e)

    # UI
    app = QApplication(sys.argv)
    # app.setStyle("Fusion")
    ico_path = os.path.join(IMAGES_DIR, "master.ico")  # ou "assets/icon.ico"
    if os.path.exists(ico_path):
        app.setWindowIcon(QIcon(ico_path))

    apply_dark_blue_style(app)

    # Vérifier une éventuelle nouvelle version avant de poursuivre le démarrage.
    # Si une mise à jour est lancée, cette fonction ne revient pas (sys.exit).
    if not maybe_check_for_update_and_run_updater(None):
        return

    # Fenêtre de chargement pour éviter le fond blanc et simuler la préparation
    splash = LoadingSplash(APP_DISPLAY_NAME)
    splash.center_on_screen()
    splash.show()
    splash.start_animation()
    app.processEvents()

    def _prepare_and_show_main():
        _load_holdings_from_disk()
        ok_pw, err_pw = _ensure_playwright_ready()
        if not ok_pw:
            try:
                print("Playwright init error:", err_pw)
            except Exception:
                pass
        w = SnowMasterGUI()
        splash.finished.connect(lambda: _on_splash_closed(w))
        # Une fois la préparation terminée, on termine proprement le splash :
        # la barre progresse rapidement jusqu'à 100 %, puis le signal finished
        # sera émis et la fenêtre principale affichée.
        splash.finish()

    def _on_splash_closed(main_win):
        # Affichage plus doux : fade-in sur l'interface principale, sans flash blanc.
        main_win.setAttribute(Qt.WA_StyledBackground, True)
        main_win.setWindowOpacity(0.0)
        main_win.show()

        anim = QPropertyAnimation(main_win, b"windowOpacity", main_win)
        anim.setDuration(650)
        anim.setStartValue(0.0)
        anim.setEndValue(1.0)
        anim.setEasingCurve(QEasingCurve.OutCubic)
        # Garder une référence sur l'animation pour éviter le GC prématuré.
        main_win._fade_anim = anim
        anim.start()

        QTimer.singleShot(0, lambda: bus.revenue_updated.emit())
        QTimer.singleShot(0, lambda: fetch_reference_prices_async())

    # Démarrer la préparation après un court délai pour laisser la barre démarrer
    QTimer.singleShot(220, _prepare_and_show_main)

    sys.exit(app.exec())


if __name__ == "__main__":
    # ensure_admin_or_exit()
    main()
