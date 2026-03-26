# -*- coding: utf-8 -*-
"""
Assistant graphique de mise à jour pour BotMaster.
- Lancé par BotMaster : affiche les builds, bouton pour télécharger / installer, barre de progression.
- Lancé seul : vérifie le manifest (fichier BotMaster_update.json ou arguments) et indique si l'app est à jour.

Packaging : pyinstaller --noconfirm --onefile --windowed --name BotMasterUpdater BotMasterUpdater.py
"""
from __future__ import annotations

import argparse
import json
import os
import queue
import sys
import threading
import time
import urllib.error
import urllib.request


def _windows_detach_flags():
    import subprocess

    flags = subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.DETACHED_PROCESS
    if hasattr(subprocess, "CREATE_BREAKAWAY_FROM_JOB"):
        flags |= subprocess.CREATE_BREAKAWAY_FROM_JOB
    return flags


def _updater_root_dir() -> str:
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))


def _default_target_exe() -> str:
    return os.path.join(_updater_root_dir(), "BotMaster.exe")


def _load_sidecar_config() -> dict:
    p = os.path.join(_updater_root_dir(), "BotMaster_update.json")
    if not os.path.isfile(p):
        return {}
    try:
        with open(p, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _read_local_build_id(target_exe: str) -> str:
    d = os.path.dirname(os.path.abspath(target_exe))
    p = os.path.join(d, "build_id.txt")
    if not os.path.isfile(p):
        return ""
    try:
        with open(p, encoding="utf-8") as f:
            return (f.readline() or "").strip()
    except Exception:
        return ""


def _manifest_url_from_repo(repo: str) -> str:
    r = (repo or "").strip().strip("/")
    if not r:
        return ""
    return f"https://github.com/{r}/releases/latest/download/update-manifest.json"


def _fetch_manifest(manifest_url: str) -> dict:
    req = urllib.request.Request(
        manifest_url,
        headers={"User-Agent": "BotMasterUpdater/1.0"},
    )
    with urllib.request.urlopen(req, timeout=18.0) as resp:
        data = resp.read().decode("utf-8-sig", errors="replace")
    return json.loads(data)


def _download_to_file(
    url: str,
    dest_path: str,
    progress_q: queue.Queue,
) -> None:
    req = urllib.request.Request(url, headers={"User-Agent": "BotMasterUpdater/1.0"})
    with urllib.request.urlopen(req, timeout=900) as resp:
        total = int(resp.headers.get("Content-Length") or 0)
        n = 0
        block = 256 * 1024
        with open(dest_path, "wb") as f:
            while True:
                chunk = resp.read(block)
                if not chunk:
                    break
                f.write(chunk)
                n += len(chunk)
                progress_q.put(("progress", n, total))
    progress_q.put(("done", None, None))


def _replace_and_relaunch(target: str, tmp_dl: str) -> None:
    target = os.path.abspath(target)
    exe_dir = os.path.dirname(target)
    bak = target + ".old"

    for _ in range(120):
        try:
            if os.path.isfile(bak):
                os.remove(bak)
            break
        except OSError:
            time.sleep(0.25)

    if os.path.isfile(target):
        for _ in range(120):
            try:
                os.replace(target, bak)
                break
            except OSError:
                time.sleep(0.25)
        else:
            raise RuntimeError("Impossible de remplacer l'exécutable (fichier verrouillé ?).")

    for _ in range(120):
        try:
            os.replace(tmp_dl, target)
            break
        except OSError:
            time.sleep(0.25)
    else:
        raise RuntimeError("Impossible d'installer le nouveau fichier.")

    if sys.platform == "win32":
        import subprocess

        subprocess.Popen(
            [target],
            cwd=exe_dir,
            creationflags=_windows_detach_flags(),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            close_fds=True,
        )
    else:
        import subprocess

        subprocess.Popen([target], cwd=exe_dir, close_fds=True)


class UpdaterApp:
    def __init__(
        self,
        target_exe: str,
        app_name: str,
        manifest_url: str,
        download_url: str,
        local_build_id: str,
        remote_build_id: str,
    ):
        import tkinter as tk
        from tkinter import ttk

        self._tk = tk
        self._ttk = ttk
        self.target_exe = os.path.abspath(target_exe)
        self.app_name = app_name or "BotMaster"
        self.manifest_url = (manifest_url or "").strip()
        self.download_url = (download_url or "").strip()
        self.local_build_id = (local_build_id or "").strip()
        self.remote_build_id = (remote_build_id or "").strip()

        self.root = tk.Tk()
        self.root.title(f"Mise à jour — {self.app_name}")
        self.root.resizable(False, False)
        self.root.minsize(420, 260)

        self._progress_q: queue.Queue = queue.Queue()

        frm = ttk.Frame(self.root, padding=16)
        frm.pack(fill=tk.BOTH, expand=True)

        self.lbl_title = ttk.Label(frm, text="", font=("", 11, "bold"))
        self.lbl_title.pack(anchor=tk.W)

        self.txt_body = tk.Text(
            frm,
            height=8,
            width=52,
            wrap=tk.WORD,
            state=tk.DISABLED,
            font=("Segoe UI", 10),
            relief=tk.FLAT,
            bg="#f5f5f5",
        )
        self.txt_body.pack(fill=tk.BOTH, expand=True, pady=(10, 8))

        self.progress = ttk.Progressbar(frm, mode="determinate", maximum=100)
        self.progress.pack(fill=tk.X, pady=(0, 8))
        self.progress.pack_forget()

        self.lbl_progress = ttk.Label(frm, text="")
        self.lbl_progress.pack(anchor=tk.W)

        btn_row = ttk.Frame(frm)
        btn_row.pack(fill=tk.X, pady=(8, 0))

        self.btn_install = ttk.Button(btn_row, text="Télécharger et installer", command=self._on_install)
        self.btn_install.pack(side=tk.RIGHT, padx=(8, 0))

        self.btn_close = ttk.Button(btn_row, text="Fermer", command=self.root.destroy)
        self.btn_close.pack(side=tk.RIGHT)

        self._apply_state_from_args_or_fetch()

    def _set_body(self, text: str) -> None:
        self.txt_body.configure(state=tk.NORMAL)
        self.txt_body.delete("1.0", tk.END)
        self.txt_body.insert(tk.END, text)
        self.txt_body.configure(state=tk.DISABLED)

    def _resolve_manifest_url(self) -> str:
        if self.manifest_url:
            return self.manifest_url
        cfg = _load_sidecar_config()
        mu = (cfg.get("manifest_url") or "").strip()
        if mu:
            return mu
        repo = (cfg.get("github_repo") or "").strip()
        return _manifest_url_from_repo(repo)

    def _apply_state_from_args_or_fetch(self) -> None:
        if self.download_url and self.remote_build_id:
            self._show_update_available_ui()
            return

        murl = self._resolve_manifest_url()
        if not murl:
            self.lbl_title.configure(text="Configuration incomplète")
            self._set_body(
                "Impossible de vérifier les mises à jour.\n\n"
                "Créez un fichier BotMaster_update.json à côté de cet utilitaire, par exemple :\n"
                '  { "github_repo": "votre-compte/votre-depot" }\n\n'
                "Ou passez --manifest-url ou --github-repo en ligne de commande."
            )
            self.btn_install.state(["disabled"])
            return

        self.manifest_url = murl
        try:
            man = _fetch_manifest(murl)
            remote = str(man.get("build_id") or man.get("version") or "").strip()
            durl = str(man.get("download_url") or "").strip()
        except Exception as e:
            self.lbl_title.configure(text="Vérification impossible")
            self._set_body(
                "Le serveur de mises à jour n'a pas pu être joint.\n\n"
                f"Détail : {e}"
            )
            self.btn_install.state(["disabled"])
            return

        if not remote or not durl:
            self.lbl_title.configure(text="Manifest invalide")
            self._set_body("Le fichier de mise à jour distant est incomplet.")
            self.btn_install.state(["disabled"])
            return

        self.remote_build_id = remote
        self.download_url = durl
        if not self.local_build_id:
            self.local_build_id = _read_local_build_id(self.target_exe)

        if self.local_build_id == self.remote_build_id:
            self.lbl_title.configure(text="Logiciel à jour")
            self._set_body(
                f"{self.app_name} est à jour.\n\n"
                f"Build installé : {self.local_build_id or '(inconnu)'}\n"
                f"Dernier build publié : {self.remote_build_id}"
            )
            self.btn_install.state(["disabled"])
            return

        self._show_update_available_ui()

    def _show_update_available_ui(self) -> None:
        self.lbl_title.configure(text="Mise à jour disponible")
        self._set_body(
            f"Une version plus récente de {self.app_name} est disponible.\n\n"
            f"Build installé : {self.local_build_id or '(inconnu)'}\n"
            f"Nouveau build : {self.remote_build_id}\n\n"
            "Cliquez sur « Télécharger et installer » pour mettre à jour. "
            "L'application principale doit être fermée pendant l'installation."
        )
        self.btn_install.state(["!disabled"])

    def _on_install(self) -> None:
        if not self.download_url:
            return
        self.btn_install.state(["disabled"])
        self.btn_close.state(["disabled"])
        self.progress.pack(fill=tk.X, pady=(0, 8))
        self.progress["value"] = 0
        self.lbl_progress.configure(text="Préparation…")
        self.root.update_idletasks()

        threading.Thread(target=self._install_thread_main, daemon=True).start()
        self._poll_progress()

    def _install_thread_main(self) -> None:
        try:
            time.sleep(1.0)
            base = os.path.basename(self.target_exe)
            tmp_dl = os.path.join(os.path.dirname(self.target_exe), f".{base}.part")
            _download_to_file(self.download_url, tmp_dl, self._progress_q)
            self._progress_q.put(("replace", tmp_dl, None))
        except Exception as e:
            self._progress_q.put(("error", str(e), None))

    def _poll_progress(self) -> None:
        reschedule = True
        try:
            while True:
                try:
                    kind, a, b = self._progress_q.get_nowait()
                except queue.Empty:
                    break
                if kind == "progress":
                    n, total = a, b
                    if total and total > 0:
                        try:
                            self.progress.stop()
                        except Exception:
                            pass
                        self.progress.configure(mode="determinate", maximum=100)
                        pct = min(100, int(100 * n / total))
                        self.progress["value"] = pct
                        self.lbl_progress.configure(
                            text=f"Téléchargement : {pct} % ({n // (1024 * 1024)} Mo)"
                        )
                    else:
                        self.progress.configure(mode="indeterminate")
                        self.progress.start(12)
                        self.lbl_progress.configure(text="Téléchargement en cours…")
                elif kind == "done":
                    pass
                elif kind == "replace":
                    tmp_dl = a
                    try:
                        try:
                            self.progress.stop()
                        except Exception:
                            pass
                        self.progress.configure(mode="determinate", maximum=100)
                        self.progress["value"] = 100
                        self.lbl_progress.configure(text="Installation…")
                        self.root.update_idletasks()
                        _replace_and_relaunch(self.target_exe, tmp_dl)
                        reschedule = False
                        self.root.destroy()
                        return
                    except Exception as e:
                        try:
                            self.progress.stop()
                        except Exception:
                            pass
                        self._set_body(
                            "La mise à jour n'a pas pu être finalisée.\n\n" + str(e)
                        )
                        self.lbl_progress.configure(text="")
                        self.btn_install.state(["!disabled"])
                        self.btn_close.state(["!disabled"])
                        reschedule = False
                        return
                elif kind == "error":
                    try:
                        self.progress.stop()
                    except Exception:
                        pass
                    try:
                        self.progress.pack_forget()
                    except Exception:
                        pass
                    self.lbl_progress.configure(text="")
                    self._set_body("La mise à jour a échoué.\n\n" + str(a))
                    self.btn_install.state(["!disabled"])
                    self.btn_close.state(["!disabled"])
                    reschedule = False
                    return
        finally:
            try:
                if reschedule and self.root.winfo_exists():
                    self.root.after(80, self._poll_progress)
            except Exception:
                pass


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Assistant de mise à jour BotMaster")
    p.add_argument("--target-exe", default="", help="Chemin de BotMaster.exe")
    p.add_argument("--download-url", default="", help="URL du nouvel exe (si déjà connu)")
    p.add_argument("--manifest-url", default="", help="URL du update-manifest.json")
    p.add_argument("--github-repo", default="", help='Format "owner/repo" pour déduire le manifest')
    p.add_argument("--local-build-id", default="", help="Build actuellement installé")
    p.add_argument("--remote-build-id", default="", help="Build publié à installer")
    p.add_argument("--app-display-name", default="BotMaster", help="Nom affiché")
    return p.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    target = (args.target_exe or "").strip() or _default_target_exe()
    manifest_url = (args.manifest_url or "").strip()
    if not manifest_url and (args.github_repo or "").strip():
        manifest_url = _manifest_url_from_repo(args.github_repo)

    app = UpdaterApp(
        target_exe=target,
        app_name=(args.app_display_name or "BotMaster").strip(),
        manifest_url=manifest_url,
        download_url=(args.download_url or "").strip(),
        local_build_id=(args.local_build_id or "").strip(),
        remote_build_id=(args.remote_build_id or "").strip(),
    )
    app.root.mainloop()


if __name__ == "__main__":
    main()
