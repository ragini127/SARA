import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext, simpledialog
import socket
import threading
import json
import time
import datetime
import uuid
import platform
import subprocess
import re
import queue
import os
import webbrowser


# ═══════════════════════════════════════════════════════════
#  CONSTANTS
# ═══════════════════════════════════════════════════════════
BROADCAST_PORT   = 45678
BROADCAST_ADDR   = "255.255.255.255"
BUFFER_SIZE      = 4096
HEARTBEAT_SEC    = 5
PEER_TIMEOUT_SEC = 25
CONTACTS_FILE    = "sara_contacts.json"
RESCUE_ROLE      = "RESCUE_TEAM"
SURVIVOR_ROLE    = "SURVIVOR"

MSG_CHAT         = "CHAT"
MSG_SOS          = "SOS"
MSG_LOCATION     = "LOCATION"
MSG_HEARTBEAT    = "HEARTBEAT"
MSG_RESCUE_PING  = "RESCUE_PING"     # Rescue team announces itself
MSG_DISPATCH     = "DISPATCH"        # Direct location → rescue team


# ═══════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════
def get_local_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("192.168.1.1", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


def get_gps_coordinates():
    system = platform.system()
    if system == "Windows":
        try:
            ps = """
Add-Type -AssemblyName System.Device
$w = New-Object System.Device.Location.GeoCoordinateWatcher
$w.Start(); Start-Sleep -Seconds 4
$p = $w.Position.Location
Write-Output "$($p.Latitude),$($p.Longitude)"
$w.Stop()
"""
            r = subprocess.run(["powershell", "-Command", ps],
                               capture_output=True, text=True, timeout=12)
            parts = r.stdout.strip().split(",")
            if len(parts) == 2:
                lat, lon = float(parts[0]), float(parts[1])
                if lat != 0 or lon != 0:
                    return lat, lon
        except Exception:
            pass
    if system == "Linux":
        try:
            r = subprocess.run(["gpspipe", "-w", "-n", "5"],
                               capture_output=True, text=True, timeout=8)
            for line in r.stdout.splitlines():
                if '"lat"' in line and '"lon"' in line:
                    lat = float(re.search(r'"lat":\s*([-\d.]+)', line).group(1))
                    lon = float(re.search(r'"lon":\s*([-\d.]+)', line).group(1))
                    return lat, lon
        except Exception:
            pass
    return None, None


def load_contacts():
    try:
        if os.path.exists(CONTACTS_FILE):
            with open(CONTACTS_FILE, "r") as f:
                return json.load(f)
    except Exception:
        pass
    return []


def save_contacts(contacts):
    try:
        with open(CONTACTS_FILE, "w") as f:
            json.dump(contacts, f, indent=2)
    except Exception:
        pass


def open_sms(phone: str, message: str):
    """Open native SMS app with pre-filled message (offline capable via local dialer)."""
    system = platform.system()
    encoded = message.replace(" ", "%20").replace("\n", "%0A")
    if system == "Windows":
        try:
            subprocess.Popen(["start", f"ms-chat:?ContactId={phone}"], shell=True)
            return True, "Opened Windows Messaging app"
        except Exception:
            pass
    if system == "Darwin":
        try:
            subprocess.Popen(["open", f"sms:{phone}"])
            return True, "Opened Messages app"
        except Exception:
            pass
    if system == "Linux":
        for app in [["gnome-contacts"], ["xdg-open", f"tel:{phone}"]]:
            try:
                subprocess.Popen(app)
                return True, f"Opened dialer for {phone}"
            except Exception:
                continue
    # Fallback: copy to clipboard and show instructions
    return False, f"Could not open SMS app automatically.\nSend manually to: {phone}"


# ═══════════════════════════════════════════════════════════
#  NETWORK ENGINE
# ═══════════════════════════════════════════════════════════
class NetworkEngine:
    def __init__(self, device_id, username, role, on_message_cb, on_peer_update_cb):
        self.device_id        = device_id
        self.username         = username
        self.role             = role
        self.on_message       = on_message_cb
        self.on_peer_update   = on_peer_update_cb
        self.local_ip         = get_local_ip()
        self.peers            = {}
        self.running          = False
        self._send_sock       = None
        self._recv_sock       = None

    def start(self):
        self.running = True
        self._setup_sockets()
        threading.Thread(target=self._listen_loop,    daemon=True).start()
        threading.Thread(target=self._heartbeat_loop, daemon=True).start()
        threading.Thread(target=self._peer_cleanup,   daemon=True).start()

    def stop(self):
        self.running = False
        try: self._recv_sock.close()
        except Exception: pass

    def broadcast(self, msg_type, payload: dict):
        packet = json.dumps({
            "type":      msg_type,
            "device_id": self.device_id,
            "username":  self.username,
            "role":      self.role,
            "timestamp": datetime.datetime.now().isoformat(),
            **payload
        }).encode()
        try:
            self._send_sock.sendto(packet, (BROADCAST_ADDR, BROADCAST_PORT))
        except Exception as e:
            print(f"[NET] Send error: {e}")

    def send_to_ip(self, ip: str, msg_type, payload: dict):
        """Unicast a message to a specific device IP."""
        packet = json.dumps({
            "type":      msg_type,
            "device_id": self.device_id,
            "username":  self.username,
            "role":      self.role,
            "timestamp": datetime.datetime.now().isoformat(),
            **payload
        }).encode()
        try:
            self._send_sock.sendto(packet, (ip, BROADCAST_PORT))
        except Exception as e:
            print(f"[NET] Unicast error: {e}")

    def _setup_sockets(self):
        self._send_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._send_sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self._send_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        self._recv_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._recv_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._recv_sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        try:
            self._recv_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        except AttributeError:
            pass
        self._recv_sock.bind(("", BROADCAST_PORT))
        self._recv_sock.settimeout(2)

    def _listen_loop(self):
        while self.running:
            try:
                data, addr = self._recv_sock.recvfrom(BUFFER_SIZE)
                msg = json.loads(data.decode())
                src_id = msg.get("device_id")
                if src_id == self.device_id:
                    continue
                self.peers[src_id] = {
                    "username":  msg.get("username", "Unknown"),
                    "role":      msg.get("role", SURVIVOR_ROLE),
                    "ip":        addr[0],
                    "last_seen": time.time(),
                    "lat":       msg.get("lat"),
                    "lon":       msg.get("lon"),
                }
                self.on_peer_update(self.peers)
                if msg["type"] != MSG_HEARTBEAT:
                    self.on_message(msg)
            except socket.timeout:
                continue
            except Exception as e:
                if self.running:
                    print(f"[NET] Recv: {e}")

    def _heartbeat_loop(self):
        while self.running:
            self.broadcast(MSG_HEARTBEAT, {})
            time.sleep(HEARTBEAT_SEC)

    def _peer_cleanup(self):
        while self.running:
            now = time.time()
            stale = [k for k, v in self.peers.items()
                     if now - v["last_seen"] > PEER_TIMEOUT_SEC]
            for k in stale:
                del self.peers[k]
            if stale:
                self.on_peer_update(self.peers)
            time.sleep(5)


# ═══════════════════════════════════════════════════════════
#  EMERGENCY CONTACTS DIALOG
# ═══════════════════════════════════════════════════════════
class ContactsDialog(tk.Toplevel):
    BG    = "#0D1B2A"
    CARD  = "#1A2E42"
    PANEL = "#112233"
    TEXT  = "#ECF0F1"
    GREEN = "#2ECC71"
    RED   = "#FF4136"
    YELLOW= "#F39C12"
    SUB   = "#7F8C8D"

    def __init__(self, parent, contacts: list, on_save_cb):
        super().__init__(parent)
        self.title("📋  Emergency Contacts — S.A.R.A")
        self.geometry("480x520")
        self.configure(bg=self.BG)
        self.resizable(False, False)
        self.contacts = [dict(c) for c in contacts]
        self.on_save  = on_save_cb
        self._build()
        self._refresh_list()
        self.grab_set()

    def _build(self):
        tk.Label(self, text="📋  EMERGENCY CONTACTS",
                 font=("Segoe UI", 13, "bold"), bg=self.BG, fg=self.TEXT
                 ).pack(pady=(14, 4))
        tk.Label(self, text="These contacts receive your location via SMS when you send SOS",
                 font=("Segoe UI", 9), bg=self.BG, fg=self.SUB
                 ).pack(pady=(0, 10))

        list_frame = tk.Frame(self, bg=self.CARD, padx=10, pady=8)
        list_frame.pack(fill="both", expand=True, padx=14, pady=(0, 8))
        self._listbox = tk.Listbox(list_frame, font=("Courier New", 10),
                                   bg=self.PANEL, fg=self.GREEN,
                                   selectbackground="#154E7A",
                                   relief="flat", bd=0, height=10)
        self._listbox.pack(fill="both", expand=True)

        add_frame = tk.Frame(self, bg=self.BG)
        add_frame.pack(fill="x", padx=14, pady=4)

        tk.Label(add_frame, text="Name", font=("Segoe UI", 9),
                 bg=self.BG, fg=self.SUB).grid(row=0, column=0, sticky="w")
        tk.Label(add_frame, text="Phone (with country code)",
                 font=("Segoe UI", 9), bg=self.BG, fg=self.SUB
                 ).grid(row=0, column=1, sticky="w", padx=(10, 0))

        self._e_name  = tk.Entry(add_frame, font=("Segoe UI", 10),
                                  bg=self.PANEL, fg=self.TEXT,
                                  insertbackground=self.TEXT, relief="flat", bd=4)
        self._e_phone = tk.Entry(add_frame, font=("Segoe UI", 10),
                                  bg=self.PANEL, fg=self.TEXT,
                                  insertbackground=self.TEXT, relief="flat", bd=4)
        self._e_name.grid(row=1, column=0, sticky="ew", pady=4)
        self._e_phone.grid(row=1, column=1, sticky="ew", padx=(10, 0), pady=4)
        add_frame.columnconfigure(0, weight=1)
        add_frame.columnconfigure(1, weight=1)

        tk.Label(add_frame, text="Relationship (e.g. Father, Police, Hospital)",
                 font=("Segoe UI", 9), bg=self.BG, fg=self.SUB
                 ).grid(row=2, column=0, columnspan=2, sticky="w")
        self._e_rel = tk.Entry(add_frame, font=("Segoe UI", 10),
                                bg=self.PANEL, fg=self.TEXT,
                                insertbackground=self.TEXT, relief="flat", bd=4)
        self._e_rel.grid(row=3, column=0, columnspan=2, sticky="ew", pady=4)

        btn_row = tk.Frame(self, bg=self.BG)
        btn_row.pack(fill="x", padx=14, pady=(0, 14))
        for text, cmd, color in [
            ("➕ Add Contact",    self._add,    "#1A6B3C"),
            ("🗑 Remove Selected", self._remove, "#6B1A1A"),
            ("💾 Save & Close",   self._save,   "#154E7A"),
        ]:
            tk.Button(btn_row, text=text, font=("Segoe UI", 10),
                      bg=color, fg="white", relief="flat", bd=0,
                      cursor="hand2", padx=8, pady=6,
                      command=cmd).pack(side="left", padx=(0, 6))

    def _refresh_list(self):
        self._listbox.delete(0, tk.END)
        for c in self.contacts:
            self._listbox.insert(tk.END,
                f"  {c['name']:<18} {c['phone']:<16}  [{c.get('relation','—')}]")

    def _add(self):
        name  = self._e_name.get().strip()
        phone = self._e_phone.get().strip()
        rel   = self._e_rel.get().strip() or "—"
        if not name or not phone:
            messagebox.showwarning("Missing Fields", "Name and phone are required.",
                                   parent=self)
            return
        self.contacts.append({"name": name, "phone": phone, "relation": rel})
        self._refresh_list()
        self._e_name.delete(0, tk.END)
        self._e_phone.delete(0, tk.END)
        self._e_rel.delete(0, tk.END)

    def _remove(self):
        sel = self._listbox.curselection()
        if not sel:
            return
        del self.contacts[sel[0]]
        self._refresh_list()

    def _save(self):
        self.on_save(self.contacts)
        self.destroy()


# ═══════════════════════════════════════════════════════════
#  MAIN APP — S.A.R.A
# ═══════════════════════════════════════════════════════════
class SARAApp:

    # ── Palette ─────────────────────────────
    BG      = "#0B1622"
    PANEL   = "#0F1F30"
    CARD    = "#162840"
    ACCENT  = "#E8290B"      # SARA red
    ACCENT2 = "#F39C12"      # amber warning
    GREEN   = "#27AE60"
    BLUE    = "#2980B9"
    RESCUE  = "#8E44AD"      # purple = rescue team
    TEXT    = "#ECF0F1"
    SUBTEXT = "#6D8CA8"
    BORDER  = "#1E3A52"

    FH  = ("Segoe UI", 13, "bold")
    FN  = ("Segoe UI", 11)
    FS  = ("Segoe UI", 9)
    FM  = ("Courier New", 10)
    FSOS= ("Segoe UI", 15, "bold")

    def __init__(self, root):
        self.root = root
        self.root.title("S.A.R.A — Search And Rescue Assistant")
        self.root.geometry("1000x720")
        self.root.configure(bg=self.BG)
        self.root.resizable(True, True)

        self.device_id = str(uuid.uuid4())[:8].upper()
        self.username  = tk.StringVar(value=f"User_{self.device_id[:4]}")
        self.role      = tk.StringVar(value=SURVIVOR_ROLE)
        self.lat       = tk.StringVar(value="")
        self.lon       = tk.StringVar(value="")
        self.local_ip  = get_local_ip()
        self.contacts  = load_contacts()
        self.net: NetworkEngine | None = None

        self._build_ui()
        self._try_auto_gps()
        self.root.after(600, self._start_network)
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

    # ══════════════════════════════════════════
    #  UI BUILD
    # ══════════════════════════════════════════
    def _build_ui(self):
        self._build_header()
        body = tk.Frame(self.root, bg=self.BG)
        body.pack(fill="both", expand=True, padx=8, pady=6)

        left = tk.Frame(body, bg=self.BG, width=310)
        left.pack(side="left", fill="y", padx=(0, 6))
        left.pack_propagate(False)

        right = tk.Frame(body, bg=self.BG)
        right.pack(side="left", fill="both", expand=True)

        self._build_left(left)
        self._build_right(right)

    # ── HEADER ──────────────────────────────
    def _build_header(self):
        hdr = tk.Frame(self.root, bg=self.ACCENT, height=56)
        hdr.pack(fill="x")
        hdr.pack_propagate(False)

        # Logo block
        logo = tk.Frame(hdr, bg="#B8200A", width=56, height=56)
        logo.pack(side="left")
        logo.pack_propagate(False)
        tk.Label(logo, text="🛰", font=("Segoe UI", 22),
                 bg="#B8200A", fg="white").place(relx=0.5, rely=0.5, anchor="center")

        tk.Label(hdr, text="S.A.R.A",
                 font=("Segoe UI", 20, "bold"), bg=self.ACCENT, fg="white"
                 ).pack(side="left", padx=(12, 4), pady=14)
        tk.Label(hdr, text="SEARCH AND RESCUE ASSISTANT  •  OFFLINE MODE",
                 font=("Segoe UI", 9), bg=self.ACCENT, fg="#FFB3A7"
                 ).pack(side="left", pady=14)

        # Role toggle
        role_frame = tk.Frame(hdr, bg=self.ACCENT)
        role_frame.pack(side="right", padx=16)
        tk.Label(role_frame, text="ROLE:", font=("Segoe UI", 9, "bold"),
                 bg=self.ACCENT, fg="#FFB3A7").pack(side="left", padx=(0, 4))
        for label, value, color in [
            ("👤 Survivor", SURVIVOR_ROLE, "#1A6B3C"),
            ("🚒 Rescue Team", RESCUE_ROLE, "#4A1A6B"),
        ]:
            tk.Radiobutton(role_frame, text=label, variable=self.role,
                           value=value, font=("Segoe UI", 9, "bold"),
                           bg=color, fg="white", selectcolor=color,
                           activebackground=color, activeforeground="white",
                           relief="flat", padx=8, pady=4,
                           indicatoron=False,
                           command=self._on_role_change
                           ).pack(side="left", padx=2)

        self._lbl_ip = tk.Label(hdr, text=f"📡 {self.local_ip}",
                                font=("Segoe UI", 8), bg=self.ACCENT, fg="#FFB3A7")
        self._lbl_ip.pack(side="right", padx=4)

    # ── LEFT PANEL ──────────────────────────
    def _build_left(self, parent):
        # Identity
        c = self._card(parent, "👤  IDENTITY")
        tk.Label(c, text="Your Name", font=self.FS, bg=self.CARD,
                 fg=self.SUBTEXT).pack(anchor="w")
        tk.Entry(c, textvariable=self.username, font=self.FN,
                 bg=self.PANEL, fg=self.TEXT, insertbackground=self.TEXT,
                 relief="flat", bd=4).pack(fill="x", pady=(2, 6))
        tk.Label(c, text=f"Device ID: {self.device_id}  |  IP: {self.local_ip}",
                 font=self.FS, bg=self.CARD, fg=self.SUBTEXT).pack(anchor="w")
        c.pack(fill="x", pady=(0, 5))

        # GPS
        g = self._card(parent, "📍  YOUR LOCATION")
        for lbl, var in [("Latitude", self.lat), ("Longitude", self.lon)]:
            tk.Label(g, text=lbl, font=self.FS, bg=self.CARD,
                     fg=self.SUBTEXT).pack(anchor="w")
            tk.Entry(g, textvariable=var, font=self.FN,
                     bg=self.PANEL, fg=self.TEXT, insertbackground=self.TEXT,
                     relief="flat", bd=4).pack(fill="x", pady=(2, 4))
        r = tk.Frame(g, bg=self.CARD)
        r.pack(fill="x", pady=(4, 0))
        self._btn(r, "📡 Auto GPS",        self._try_auto_gps,    "#1A5276").pack(
                  side="left", fill="x", expand=True, padx=(0, 2))
        self._btn(r, "📤 Share Location",  self._share_location,  "#1A6B3C").pack(
                  side="left", fill="x", expand=True, padx=(2, 0))
        self._lbl_gps = tk.Label(g, text="", font=self.FS, bg=self.CARD,
                                  fg=self.GREEN, wraplength=270)
        self._lbl_gps.pack(anchor="w", pady=(4, 0))
        g.pack(fill="x", pady=(0, 5))

        # SOS
        self._sos_btn = tk.Button(
            parent, text="🆘  SEND SOS ALERT",
            font=self.FSOS, bg=self.ACCENT, fg="white",
            activebackground="#B8200A", activeforeground="white",
            relief="flat", bd=0, cursor="hand2", height=2,
            command=self._send_sos)
        self._sos_btn.pack(fill="x", pady=(0, 5))

        # Rescue Team Dispatch
        d = self._card(parent, "🚒  DISPATCH TO RESCUE TEAM")
        self._rescue_var = tk.StringVar(value="")
        self._rescue_combo = ttk.Combobox(d, textvariable=self._rescue_var,
                                           font=self.FN, state="readonly")
        self._rescue_combo["values"] = ["No rescue teams detected"]
        self._rescue_combo.pack(fill="x", pady=(0, 6))
        self._btn(d, "📡 Send My Location to Rescue Team",
                  self._dispatch_to_rescue, "#4A1A6B").pack(fill="x")
        d.pack(fill="x", pady=(0, 5))

        # Emergency Contacts
        ec = self._card(parent, "📱  EMERGENCY CONTACTS")
        self._contacts_lbl = tk.Label(
            ec,
            text=self._contacts_summary(),
            font=self.FS, bg=self.CARD, fg=self.GREEN, wraplength=270, justify="left")
        self._contacts_lbl.pack(anchor="w", pady=(0, 6))
        row = tk.Frame(ec, bg=self.CARD)
        row.pack(fill="x")
        self._btn(row, "📋 Manage Contacts",  self._open_contacts,      "#1A5276"
                  ).pack(side="left", fill="x", expand=True, padx=(0, 2))
        self._btn(row, "📨 Alert All Contacts", self._alert_all_contacts, "#6B3A1A"
                  ).pack(side="left", fill="x", expand=True, padx=(2, 0))
        ec.pack(fill="x", pady=(0, 5))

    # ── RIGHT PANEL ─────────────────────────
    def _build_right(self, parent):
        # Tabs
        style = ttk.Style()
        style.theme_use("default")
        style.configure("SARA.TNotebook",       background=self.BG, borderwidth=0)
        style.configure("SARA.TNotebook.Tab",   background=self.PANEL,
                        foreground=self.SUBTEXT, padding=[12, 6],
                        font=("Segoe UI", 10, "bold"))
        style.map("SARA.TNotebook.Tab",
                  background=[("selected", self.CARD)],
                  foreground=[("selected", self.TEXT)])

        self._nb = ttk.Notebook(parent, style="SARA.TNotebook")
        self._nb.pack(fill="both", expand=True)

        # Tab 1 — Map
        tab_map = tk.Frame(self._nb, bg=self.CARD)
        self._nb.add(tab_map, text="🗺  RESCUE MAP")
        self._build_map_tab(tab_map)

        # Tab 2 — Broadcast Channel
        tab_chat = tk.Frame(self._nb, bg=self.CARD)
        self._nb.add(tab_chat, text="💬  BROADCAST CHANNEL")
        self._build_chat_tab(tab_chat)

        # Tab 3 — Rescue Team Log (visible to all, highlighted for rescue)
        tab_log = tk.Frame(self._nb, bg=self.CARD)
        self._nb.add(tab_log, text="🚒  RESCUE DISPATCH LOG")
        self._build_log_tab(tab_log)

        # Tab 4 — Peers
        tab_peers = tk.Frame(self._nb, bg=self.CARD)
        self._nb.add(tab_peers, text="🌐  NETWORK PEERS")
        self._build_peers_tab(tab_peers)

    def _build_map_tab(self, parent):
        info = tk.Frame(parent, bg=self.CARD)
        info.pack(fill="x", padx=10, pady=6)
        tk.Label(info, text="Live positions of all SARA devices on network — updates automatically",
                 font=self.FS, bg=self.CARD, fg=self.SUBTEXT).pack(side="left")
        self._btn(info, "🔄 Refresh Map", self._update_map, "#1A5276"
                  ).pack(side="right")

        self._map_canvas = tk.Canvas(parent, bg="#071118",
                                      highlightthickness=0)
        self._map_canvas.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        self._map_canvas.bind("<Configure>", lambda e: self._update_map())

        # Legend
        leg = tk.Frame(parent, bg=self.PANEL)
        leg.pack(fill="x", padx=10, pady=(0, 8))
        for dot, label, color in [
            ("●", "You",          self.GREEN),
            ("●", "Survivor",     "#3498DB"),
            ("●", "Rescue Team",  self.RESCUE),
            ("●", "SOS Active",   self.ACCENT),
        ]:
            tk.Label(leg, text=f"{dot} {label}", font=("Segoe UI", 9),
                     bg=self.PANEL, fg=color).pack(side="left", padx=10, pady=4)

    def _build_chat_tab(self, parent):
        self._chat_box = scrolledtext.ScrolledText(
            parent, font=self.FN, bg=self.PANEL, fg=self.TEXT,
            relief="flat", state="disabled", wrap="word", padx=8, pady=8)
        self._chat_box.tag_config("sos",      foreground=self.ACCENT,
                                              font=("Segoe UI", 11, "bold"))
        self._chat_box.tag_config("location", foreground="#3498DB")
        self._chat_box.tag_config("system",   foreground=self.ACCENT2)
        self._chat_box.tag_config("self",     foreground=self.GREEN)
        self._chat_box.tag_config("rescue",   foreground=self.RESCUE,
                                              font=("Segoe UI", 11, "bold"))
        self._chat_box.pack(fill="both", expand=True, padx=8, pady=(8, 0))

        inp = tk.Frame(parent, bg=self.CARD)
        inp.pack(fill="x", padx=8, pady=8)
        self._msg_entry = tk.Entry(inp, font=self.FN, bg=self.PANEL, fg=self.TEXT,
                                    insertbackground=self.TEXT, relief="flat", bd=6)
        self._msg_entry.pack(side="left", fill="x", expand=True, padx=(0, 6))
        self._msg_entry.bind("<Return>", lambda e: self._send_chat())
        self._btn(inp, "Broadcast 📢", self._send_chat, self.BLUE).pack(side="left")

    def _build_log_tab(self, parent):
        tk.Label(parent,
                 text="All location dispatches and SOS signals received by rescue teams",
                 font=self.FS, bg=self.CARD, fg=self.SUBTEXT, padx=10, pady=6
                 ).pack(anchor="w")
        self._log_box = scrolledtext.ScrolledText(
            parent, font=self.FM, bg=self.PANEL, fg=self.GREEN,
            relief="flat", state="disabled", wrap="word", padx=8, pady=8)
        self._log_box.tag_config("sos",      foreground=self.ACCENT,
                                             font=("Courier New", 10, "bold"))
        self._log_box.tag_config("dispatch", foreground=self.RESCUE,
                                             font=("Courier New", 10, "bold"))
        self._log_box.tag_config("system",   foreground=self.ACCENT2)
        self._log_box.pack(fill="both", expand=True, padx=8, pady=(0, 8))
        self._btn(parent, "🗑  Clear Log", self._clear_log, "#2C3E50"
                  ).pack(anchor="e", padx=10, pady=(0, 8))

    def _build_peers_tab(self, parent):
        tk.Label(parent,
                 text="All active SARA devices on this network (auto-refreshes every 5s)",
                 font=self.FS, bg=self.CARD, fg=self.SUBTEXT, padx=10, pady=6
                 ).pack(anchor="w")
        self._peers_box = scrolledtext.ScrolledText(
            parent, font=self.FM, bg=self.PANEL, fg=self.GREEN,
            relief="flat", state="disabled", wrap="none", padx=8, pady=8)
        self._peers_box.tag_config("rescue", foreground=self.RESCUE)
        self._peers_box.pack(fill="both", expand=True, padx=8, pady=(0, 8))

    # ══════════════════════════════════════════
    #  NETWORK ACTIONS
    # ══════════════════════════════════════════
    def _start_network(self):
        self.net = NetworkEngine(
            device_id         = self.device_id,
            username          = self.username.get(),
            role              = self.role.get(),
            on_message_cb     = self._on_message_received,
            on_peer_update_cb = self._on_peers_updated,
        )
        self.net.start()
        self._log_system(f"✅  S.A.R.A network started  |  IP: {self.local_ip}")
        self._log_system("📡  Broadcasting on local network — no internet required")

    def _on_role_change(self):
        if self.net:
            self.net.role = self.role.get()
        color = self.RESCUE if self.role.get() == RESCUE_ROLE else self.ACCENT
        self._sos_btn.configure(bg=color)
        self._log_system(f"🔄  Role changed to: {self.role.get()}")

    def _send_chat(self):
        msg = self._msg_entry.get().strip()
        if not msg or not self.net:
            return
        self._msg_entry.delete(0, tk.END)
        self.net.broadcast(MSG_CHAT, {
            "text": msg,
            "lat": self.lat.get() or "N/A",
            "lon": self.lon.get() or "N/A",
        })
        ts = datetime.datetime.now().strftime("%H:%M:%S")
        self._append_chat(f"[{ts}] YOU: {msg}\n", "self")

    def _send_sos(self):
        lat = self.lat.get().strip()
        lon = self.lon.get().strip()
        if not lat or not lon:
            if not messagebox.askyesno("No Location",
                    "No GPS coordinates set.\nSend SOS without location?"):
                return
            lat = lon = "UNKNOWN"

        name = self.username.get()
        ts   = datetime.datetime.now().strftime("%H:%M:%S")
        maps_link = f"https://maps.google.com/?q={lat},{lon}" if lat != "UNKNOWN" else "No location"

        # Broadcast on network
        if self.net:
            self.net.broadcast(MSG_SOS, {
                "text": f"🆘 SOS FROM {name}",
                "lat": lat, "lon": lon,
                "maps_link": maps_link,
            })
        self._append_chat(
            f"[{ts}] 🆘 SOS SENT  |  {name}  |  LAT:{lat} LON:{lon}\n", "sos")
        self._log_dispatch(
            f"[{ts}] 🆘 SOS  |  {name}  |  {lat}, {lon}  |  {maps_link}\n", "sos")
        self._flash_sos_btn()

        # Alert emergency contacts
        if self.contacts:
            self._alert_all_contacts(auto=True, lat=lat, lon=lon)

    def _share_location(self):
        lat = self.lat.get().strip()
        lon = self.lon.get().strip()
        if not lat or not lon:
            messagebox.showwarning("No Coordinates",
                                   "Enter or auto-detect coordinates first.")
            return
        if not self.net:
            return
        name = self.username.get()
        self.net.broadcast(MSG_LOCATION, {
            "text": f"📍 {name} is at LAT {lat}, LON {lon}",
            "lat": lat, "lon": lon,
        })
        ts = datetime.datetime.now().strftime("%H:%M:%S")
        self._append_chat(
            f"[{ts}] 📍 LOCATION SHARED → All devices  |  {lat}, {lon}\n", "location")

    def _dispatch_to_rescue(self):
        """Send location directly to selected rescue team device."""
        if not self.net:
            return
        lat = self.lat.get().strip()
        lon = self.lon.get().strip()
        if not lat or not lon:
            messagebox.showwarning("No Location",
                                   "Set your GPS coordinates first.")
            return

        sel = self._rescue_var.get()
        if not sel or sel.startswith("No rescue"):
            messagebox.showinfo("No Rescue Team",
                "No rescue team devices found on network.\n"
                "Make sure rescue team is connected to the same Wi-Fi hotspot.")
            return

        # Find the IP of selected rescue peer
        rescue_ip   = None
        rescue_name = sel.split("  ")[0].strip()
        for pid, info in self.net.peers.items():
            if info["username"] == rescue_name and info["role"] == RESCUE_ROLE:
                rescue_ip = info["ip"]
                break

        name      = self.username.get()
        maps_link = f"https://maps.google.com/?q={lat},{lon}"
        msg_text  = (f"🚨 DISPATCH from {name} | "
                     f"LAT: {lat}, LON: {lon} | Maps: {maps_link}")
        ts = datetime.datetime.now().strftime("%H:%M:%S")

        if rescue_ip:
            self.net.send_to_ip(rescue_ip, MSG_DISPATCH, {
                "text":      msg_text,
                "lat":       lat,
                "lon":       lon,
                "maps_link": maps_link,
            })
            self._append_chat(
                f"[{ts}] 🚒 LOCATION SENT TO RESCUE TEAM [{rescue_name}]"
                f"  |  {lat}, {lon}\n", "rescue")
            self._log_dispatch(
                f"[{ts}] DISPATCH → {rescue_name} [{rescue_ip}]"
                f"  |  {lat}, {lon}  |  {maps_link}\n", "dispatch")
            messagebox.showinfo("Dispatched ✅",
                f"Your location has been sent to rescue team:\n{rescue_name}\n\n"
                f"Coordinates: {lat}, {lon}")
        else:
            # Broadcast as fallback
            self.net.broadcast(MSG_DISPATCH, {
                "text": msg_text, "lat": lat, "lon": lon, "maps_link": maps_link,
            })
            self._append_chat(
                f"[{ts}] 🚒 LOCATION BROADCAST TO ALL RESCUE TEAMS"
                f"  |  {lat}, {lon}\n", "rescue")

    # ══════════════════════════════════════════
    #  EMERGENCY CONTACTS
    # ══════════════════════════════════════════
    def _contacts_summary(self):
        if not self.contacts:
            return "No contacts saved yet"
        lines = [f"• {c['name']} ({c.get('relation','—')}): {c['phone']}"
                 for c in self.contacts[:4]]
        if len(self.contacts) > 4:
            lines.append(f"  + {len(self.contacts)-4} more…")
        return "\n".join(lines)

    def _open_contacts(self):
        ContactsDialog(self.root, self.contacts, self._on_contacts_saved)

    def _on_contacts_saved(self, new_contacts):
        self.contacts = new_contacts
        save_contacts(self.contacts)
        self._contacts_lbl.config(text=self._contacts_summary())
        self._log_system(f"📋  Contacts saved: {len(self.contacts)} entries")

    def _alert_all_contacts(self, auto=False, lat=None, lon=None):
        if not self.contacts:
            messagebox.showinfo("No Contacts",
                "No emergency contacts saved.\nUse 'Manage Contacts' to add them.")
            return
        lat  = lat  or self.lat.get().strip()  or "UNKNOWN"
        lon  = lon  or self.lon.get().strip()  or "UNKNOWN"
        name = self.username.get()
        maps_link = (f"https://maps.google.com/?q={lat},{lon}"
                     if lat != "UNKNOWN" else "Location unavailable")
        ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sms_body = (
            f"🆘 EMERGENCY ALERT from {name}\n"
            f"Time: {ts}\n"
            f"Location: LAT {lat}, LON {lon}\n"
            f"Map: {maps_link}\n"
            f"Sent via S.A.R.A Rescue App"
        )

        results = []
        for c in self.contacts:
            ok, status = open_sms(c["phone"], sms_body)
            results.append(f"{'✅' if ok else '⚠️'} {c['name']} ({c['phone']}): {status}")
            time.sleep(0.3)

        # Show result summary
        summary = "\n".join(results)
        if not auto:
            messagebox.showinfo("SMS Alert Status", summary)

        log_ts = datetime.datetime.now().strftime("%H:%M:%S")
        self._log_dispatch(
            f"[{log_ts}] 📱 SMS ALERT → {len(self.contacts)} contacts"
            f"  |  {lat}, {lon}\n", "dispatch")
        self._append_chat(
            f"[{log_ts}] 📱 SMS ALERT SENT to {len(self.contacts)} emergency contacts\n",
            "rescue")

    # ══════════════════════════════════════════
    #  NETWORK CALLBACKS
    # ══════════════════════════════════════════
    def _on_message_received(self, msg: dict):
        self.root.after(0, self._handle_message, msg)

    def _handle_message(self, msg: dict):
        ts    = datetime.datetime.now().strftime("%H:%M:%S")
        src   = msg.get("username", "???")
        role  = msg.get("role", SURVIVOR_ROLE)
        lat   = msg.get("lat", "")
        lon   = msg.get("lon", "")
        mtype = msg.get("type")
        maps  = msg.get("maps_link", "")
        badge = "🚒" if role == RESCUE_ROLE else "👤"

        if mtype == MSG_SOS:
            self._append_chat(
                f"[{ts}] 🆘 SOS — {badge}{src}  |  {lat}, {lon}\n", "sos")
            self._log_dispatch(
                f"[{ts}] 🆘 SOS  |  {src}  |  {lat}, {lon}  |  {maps}\n", "sos")
            self._update_map()
            messagebox.showwarning("⚠️ SOS RECEIVED",
                f"SOS from {src}\nLocation: {lat}, {lon}\n"
                f"Map: {maps}\n\nDispatch rescue team immediately!")

        elif mtype == MSG_DISPATCH:
            self._append_chat(
                f"[{ts}] 🚨 DISPATCH from {badge}{src}  |  {lat}, {lon}\n", "rescue")
            self._log_dispatch(
                f"[{ts}] DISPATCH from {src}  |  {lat}, {lon}  |  {maps}\n", "dispatch")
            self._update_map()
            if self.role.get() == RESCUE_ROLE:
                messagebox.showinfo("📍 Dispatch Received",
                    f"Location from: {src}\n"
                    f"LAT: {lat}  LON: {lon}\n"
                    f"Maps: {maps}")

        elif mtype == MSG_LOCATION:
            self._append_chat(
                f"[{ts}] 📍 {badge}{src}  |  {lat}, {lon}\n", "location")
            self._update_map()

        elif mtype == MSG_CHAT:
            tag = "rescue" if role == RESCUE_ROLE else ""
            line = f"[{ts}] {badge} {src}: {msg.get('text','')}"
            if lat and lat != "N/A":
                line += f"  [📍{lat},{lon}]"
            self._append_chat(line + "\n", tag)

    def _on_peers_updated(self, peers: dict):
        self.root.after(0, self._refresh_peers, peers)

    def _refresh_peers(self, peers: dict):
        # Update rescue dropdown
        rescue_options = []
        for pid, info in peers.items():
            if info.get("role") == RESCUE_ROLE:
                rescue_options.append(
                    f"{info['username']}  [{info['ip']}]")
        self._rescue_combo["values"] = rescue_options or ["No rescue teams detected"]
        if rescue_options and not self._rescue_var.get().startswith(rescue_options[0][:5]):
            self._rescue_var.set(rescue_options[0])

        # Update peers tab
        self._peers_box.configure(state="normal")
        self._peers_box.delete("1.0", tk.END)
        if not peers:
            self._peers_box.insert(tk.END, "  No devices on network yet…\n")
        else:
            self._peers_box.insert(tk.END,
                f"  {'NAME':<18} {'ROLE':<14} {'IP':<16} {'LAT':<12} {'LON'}\n")
            self._peers_box.insert(tk.END, "  " + "─"*70 + "\n")
            for pid, info in peers.items():
                role_label = "🚒 RESCUE" if info.get("role") == RESCUE_ROLE else "👤 SURVIVOR"
                tag = "rescue" if info.get("role") == RESCUE_ROLE else ""
                lat = info.get("lat") or "?"
                lon = info.get("lon") or "?"
                line = (f"  🟢 {info['username'][:16]:<18}"
                        f"{role_label:<14}"
                        f"{info['ip']:<16}"
                        f"{str(lat)[:10]:<12}{lon}\n")
                self._peers_box.insert(tk.END, line, tag)
        self._peers_box.configure(state="disabled")
        self._update_map()

    # ══════════════════════════════════════════
    #  GPS
    # ══════════════════════════════════════════
    def _try_auto_gps(self):
        try:
            self._lbl_gps.config(text="⏳ Detecting GPS…", fg=self.ACCENT2)
        except Exception:
            pass
        threading.Thread(target=self._gps_thread, daemon=True).start()

    def _gps_thread(self):
        lat, lon = get_gps_coordinates()
        self.root.after(0, self._gps_done, lat, lon)

    def _gps_done(self, lat, lon):
        if lat is not None:
            self.lat.set(f"{lat:.6f}")
            self.lon.set(f"{lon:.6f}")
            self._lbl_gps.config(
                text=f"✅ GPS: {lat:.5f}, {lon:.5f}", fg=self.GREEN)
        else:
            self._lbl_gps.config(
                text="⚠️ GPS unavailable — enter manually", fg=self.ACCENT2)

    # ══════════════════════════════════════════
    #  MAP
    # ══════════════════════════════════════════
    def _update_map(self):
        c = self._map_canvas
        c.delete("all")
        w = max(c.winfo_width(), 500)
        h = max(c.winfo_height(), 300)

        # Grid background
        for x in range(0, w, 44):
            c.create_line(x, 0, x, h, fill="#0A1A2A", width=1)
        for y in range(0, h, 44):
            c.create_line(0, y, w, y, fill="#0A1A2A", width=1)

        all_pts = []
        try:
            my_lat = float(self.lat.get())
            my_lon = float(self.lon.get())
            all_pts.append(("YOU", self.username.get(),
                             my_lat, my_lon, self.GREEN, "self"))
        except (ValueError, tk.TclError):
            pass

        if self.net:
            for pid, info in self.net.peers.items():
                try:
                    plat = float(info["lat"])
                    plon = float(info["lon"])
                    role = info.get("role", SURVIVOR_ROLE)
                    color = self.RESCUE if role == RESCUE_ROLE else "#3498DB"
                    kind  = "rescue" if role == RESCUE_ROLE else "peer"
                    all_pts.append((pid, info["username"],
                                    plat, plon, color, kind))
                except (TypeError, ValueError):
                    pass

        if not all_pts:
            c.create_text(w//2, h//2,
                          text="Share your location to appear on map",
                          fill="#1A3A52", font=("Segoe UI", 12))
            return

        lats = [p[2] for p in all_pts]
        lons = [p[3] for p in all_pts]
        dlat = max(max(lats) - min(lats), 0.005)
        dlon = max(max(lons) - min(lons), 0.005)
        pad  = 45

        def to_xy(lat, lon):
            x = pad + (lon - min(lons)) / dlon * (w - 2*pad)
            y = h - pad - (lat - min(lats)) / dlat * (h - 2*pad)
            return x, y

        # Draw lines from self to each peer
        self_pts = [(p[2], p[3]) for p in all_pts if p[5] == "self"]
        if self_pts:
            sx, sy = to_xy(*self_pts[0])
            for p in all_pts:
                if p[5] != "self":
                    px, py = to_xy(p[2], p[3])
                    c.create_line(sx, sy, px, py,
                                  fill="#1A3A52", dash=(5, 4), width=1)

        # Draw points
        for pid, name, lat, lon, color, kind in all_pts:
            x, y = to_xy(lat, lon)
            r = 12 if kind == "self" else 9
            # Outer glow
            c.create_oval(x-r-3, y-r-3, x+r+3, y+r+3,
                          fill="", outline=color, width=1)
            c.create_oval(x-r, y-r, x+r, y+r,
                          fill=color, outline="white", width=1)
            icon = "★" if kind == "self" else ("⛑" if kind == "rescue" else "●")
            c.create_text(x, y, text=icon, fill="white",
                          font=("Segoe UI", 8, "bold"))
            c.create_text(x, y - r - 9, text=name[:12],
                          fill=color, font=("Segoe UI", 8, "bold"))
            c.create_text(x, y + r + 9,
                          text=f"{lat:.4f},{lon:.4f}",
                          fill=self.SUBTEXT, font=("Segoe UI", 7))

    # ══════════════════════════════════════════
    #  HELPERS
    # ══════════════════════════════════════════
    def _append_chat(self, text: str, tag: str = ""):
        self._chat_box.configure(state="normal")
        self._chat_box.insert(tk.END, text, tag)
        self._chat_box.see(tk.END)
        self._chat_box.configure(state="disabled")

    def _log_dispatch(self, text: str, tag: str = "system"):
        self._log_box.configure(state="normal")
        self._log_box.insert(tk.END, text, tag)
        self._log_box.see(tk.END)
        self._log_box.configure(state="disabled")

    def _log_system(self, text: str):
        ts = datetime.datetime.now().strftime("%H:%M:%S")
        self._append_chat(f"[{ts}] {text}\n", "system")
        self._log_dispatch(f"[{ts}] {text}\n", "system")

    def _clear_log(self):
        self._log_box.configure(state="normal")
        self._log_box.delete("1.0", tk.END)
        self._log_box.configure(state="disabled")

    def _flash_sos_btn(self):
        def _flash(n=0):
            if n > 9:
                self._sos_btn.configure(bg=self.ACCENT)
                return
            c = "white" if n % 2 == 0 else self.ACCENT
            self._sos_btn.configure(bg=c)
            self.root.after(200, _flash, n + 1)
        _flash()

    def _card(self, parent, title: str) -> tk.Frame:
        outer = tk.Frame(parent, bg=self.BG)
        tk.Label(outer, text=title, font=("Segoe UI", 10, "bold"),
                 bg=self.BG, fg=self.SUBTEXT).pack(anchor="w", pady=(4, 1))
        inner = tk.Frame(outer, bg=self.CARD, padx=10, pady=8)
        inner.pack(fill="both", expand=True)
        return inner

    def _btn(self, parent, text, cmd, color="#333") -> tk.Button:
        return tk.Button(parent, text=text, font=("Segoe UI", 10),
                         bg=color, fg="white", relief="flat", bd=0,
                         cursor="hand2", padx=8, pady=5,
                         activebackground=color, activeforeground="white",
                         command=cmd)

    def _on_close(self):
        if self.net:
            self.net.stop()
        self.root.destroy()


# ═══════════════════════════════════════════════════════════
if __name__ == "__main__":
    root = tk.Tk()
    SARAApp(root)
    root.mainloop()# SARA
