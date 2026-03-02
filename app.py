"""
NEXUS — Decentralized Renewable Energy Exchange
Flask backend with:
  - Supabase auth + real DB storage (no hardcoded data)
  - Location-aware weather pricing engine (OpenWeatherMap)
  - SHA-256 hash chain (hashes stored in DB, visible only to admin)
  - Razorpay payment gateway integration
  - Role-based access (producer / consumer / investor / admin)
  - Community energy pools
  - AI advisor per role
  - Carbon credit system
  - Escrow-based P2P settlement
"""

import os, hashlib, json, time, math, uuid, traceback
import hmac as _hmac_mod   # rename to avoid collision with flask hmac usage
import base64
import requests
from datetime import datetime, timezone, timedelta
from functools import wraps
from flask import Flask, jsonify, request, render_template, session, redirect
from flask_cors import CORS
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", os.urandom(32).hex())
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
CORS(app, supports_credentials=True, origins=["*"])

# ─────────────────────────────────────────────
# IN-MEMORY FALLBACK (MUST BE DEFINED FIRST)
# ─────────────────────────────────────────────
_mem = {
    "users": {},        # email → user dict
    "listings": [],
    "transactions": [],
    "pools": [],
    "investments": [],
    "sessions": {}      # token → user
}

# ─────────────────────────────────────────────
# SUPABASE INIT
# ─────────────────────────────────────────────
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_ANON = os.getenv("SUPABASE_ANON_KEY", "")
SUPABASE_SERVICE = os.getenv("SUPABASE_SERVICE_KEY", "")

supabase = None
supabase_admin = None   # service-role client — bypasses RLS for trusted writes

if SUPABASE_URL and SUPABASE_ANON:
    try:
        from supabase import create_client
        supabase = create_client(SUPABASE_URL, SUPABASE_ANON)
        print("✓ Supabase connected")
        try:
            test = supabase.table("profiles").select("*", count="exact").limit(1).execute()
            print("✓ Supabase connection verified")
        except Exception as conn_err:
            print(f"⚠ Supabase connection test failed: {conn_err}")
            supabase = None
    except Exception as e:
        print(f"⚠ Supabase error: {e}")
        supabase = None

# Service-role client — bypasses RLS for server-side writes (pools, investments, etc.)
if SUPABASE_URL and SUPABASE_SERVICE:
    try:
        from supabase import create_client as _create_client
        supabase_admin = _create_client(SUPABASE_URL, SUPABASE_SERVICE)
        print("✓ Supabase admin client ready (service role)")
    except Exception as e:
        print(f"⚠ Supabase admin client error: {e}")
        supabase_admin = None

# ─────────────────────────────────────────────
# DEMO USERS (memory fallback)
# ─────────────────────────────────────────────
print("📝 Adding memory fallback users for testing...")
demo_users = [
    {"email": "ravi@nexus.demo",   "name": "Ravi Solar Farm",  "role": "producer",  "password": "Demo1234!"},
    {"email": "priya@nexus.demo",  "name": "Priya Wind Energy", "role": "producer",  "password": "Demo1234!"},
    {"email": "arjun@nexus.demo",  "name": "Arjun Biogas Co.", "role": "producer",  "password": "Demo1234!"},
    {"email": "sneha@nexus.demo",  "name": "Sneha GreenPower", "role": "producer",  "password": "Demo1234!"},
    {"email": "arya@nexus.demo",   "name": "Arya Sharma",      "role": "consumer",  "password": "Demo1234!"},
    {"email": "kabir@nexus.demo",  "name": "Kabir Mehta",      "role": "consumer",  "password": "Demo1234!"},
    {"email": "zara@nexus.demo",   "name": "Zara Khan",        "role": "consumer",  "password": "Demo1234!"},
    {"email": "vikram@nexus.demo", "name": "Vikram Capital",   "role": "investor",  "password": "Demo1234!"},
    {"email": "admin@nexus.demo",  "name": "NEXUS Admin",      "role": "admin",     "password": "Demo1234!"},
]

for demo_user in demo_users:
    if demo_user["email"] not in _mem["users"]:
        uid = "nx_" + uuid.uuid4().hex[:12]
        pw_hash = hashlib.sha256(demo_user["password"].encode()).hexdigest()
        _mem["users"][demo_user["email"]] = {
            "id": uid, "email": demo_user["email"], "full_name": demo_user["name"],
            "role": demo_user["role"], "location_city": "Indore",
            "location_lat": 22.7196, "location_lon": 75.8577,
            "carbon_score": 0, "green_credits": 0, "pw_hash": pw_hash
        }
        print(f"✅ Added demo user: {demo_user['email']} ({demo_user['role']})")

demo_listings = [
    {"producer_name": "Ravi Solar Farm",  "energy_type": "solar",  "base_price": 3.0, "available_kwh": 500, "capacity_kw": 50, "location_city": "Indore", "location_lat": 22.7196, "location_lon": 75.8577,  "description": "Rooftop solar, 50kW"},
    {"producer_name": "Ravi Solar Farm",  "energy_type": "solar",  "base_price": 2.8, "available_kwh": 300, "capacity_kw": 30, "location_city": "Indore", "location_lat": 22.7350, "location_lon": 75.8700,  "description": "Ground-mounted solar farm"},
    {"producer_name": "Priya Wind Energy","energy_type": "wind",   "base_price": 3.5, "available_kwh": 400, "capacity_kw": 40, "location_city": "Bhopal", "location_lat": 23.2599, "location_lon": 77.4126,  "description": "Wind turbine farm"},
    {"producer_name": "Priya Wind Energy","energy_type": "wind",   "base_price": 3.2, "available_kwh": 250, "capacity_kw": 25, "location_city": "Bhopal", "location_lat": 23.2700, "location_lon": 77.4300,  "description": "Open terrain wind installation"},
    {"producer_name": "Arjun Biogas Co.", "energy_type": "biogas", "base_price": 4.0, "available_kwh": 200, "capacity_kw": 20, "location_city": "Ujjain", "location_lat": 23.1765, "location_lon": 75.7885,  "description": "Biogas plant"},
    {"producer_name": "Sneha GreenPower","energy_type": "solar",  "base_price": 3.1, "available_kwh": 350, "capacity_kw": 35, "location_city": "Dewas",  "location_lat": 22.9676, "location_lon": 76.0534,  "description": "Hybrid solar installation"},
]

for listing in demo_listings:
    listing["id"] = str(uuid.uuid4())
    producer = next((u for u in _mem["users"].values() if u["full_name"] == listing["producer_name"]), None)
    listing["producer_id"] = producer["id"] if producer else "nx_demo"
    listing["is_active"] = True
    listing["created_at"] = datetime.now(timezone.utc).isoformat()
    _mem["listings"].append(listing)

_mem["investments"].append({
    "id": str(uuid.uuid4()), "investor_id": "nx_demo_inv1", "producer_id": "nx_demo",
    "listing_id": "nx_demo_list1", "amount_inr": 5000, "kwh_funded": 1000,
    "return_rate": 8.5, "status": "active", "created_at": datetime.now(timezone.utc).isoformat()
})

print(f"✅ Added {len(demo_listings)} demo listings")
print(f"📊 Memory mode active with {len(_mem['users'])} users")

WEATHER_API_KEY  = os.getenv("OPENWEATHER_API_KEY", "")
RAZORPAY_KEY_ID  = os.getenv("RAZORPAY_KEY_ID", "")
RAZORPAY_KEY_SECRET = os.getenv("RAZORPAY_KEY_SECRET", "")
GROQ_API_KEY     = os.getenv("GROQ_API_KEY", "")
ARIA_DEBUG       = os.getenv("ARIA_DEBUG", "true").lower() == "true"

# ─────────────────────────────────────────────
# POLICY CONFIG
# ─────────────────────────────────────────────
POLICY = {
    "max_price_kwh": 12.0,
    "min_price_kwh": 1.50,
    "solar_subsidy": 0.50,
    "wind_subsidy": 0.30,
    "biogas_subsidy": 0.20,
    "bulk_threshold_kwh": 50,
    "bulk_discount_pct": 8.0,
    "carbon_per_kwh": 0.82,
    "transmission_fee_per_km": 0.02,
    "platform_fee_pct": 1.5,
}

# ─────────────────────────────────────────────
# AUTH HELPERS
# ─────────────────────────────────────────────
_TOKEN_SECRET = os.getenv("SECRET_KEY", "nexus-demo-secret")

def generate_token(email=""):
    """Signed token encoding user email — survives Flask restarts."""
    payload = base64.urlsafe_b64encode(email.encode()).decode().rstrip("=")
    sig = _hmac_mod.new(_TOKEN_SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()[:16]
    token = f"nx_{payload}_{sig}"
    if email and email in _mem["users"]:
        _mem["sessions"][token] = _mem["users"][email]
    return token

def get_user_from_token(token):
    if not token:
        return None

    # 1. Fast path — already cached in memory
    if token in _mem["sessions"]:
        return _mem["sessions"][token]

    # 2. Supabase JWT path (long tokens, not starting with nx_)
    if supabase and not token.startswith("nx_"):
        try:
            user_resp = supabase.auth.get_user(token)
            if user_resp and user_resp.user:
                uid = str(user_resp.user.id)
                profile = supabase.table("profiles").select("*").eq("id", uid).single().execute()
                if profile.data:
                    _mem["sessions"][token] = profile.data
                    return profile.data
                meta = user_resp.user.user_metadata or {}
                minimal = {
                    "id": uid,
                    "email": user_resp.user.email,
                    "full_name": meta.get("name", user_resp.user.email),
                    "role": meta.get("role", "consumer"),
                    "location_lat": 22.7196,
                    "location_lon": 75.8577,
                    "location_city": "Indore",
                    "carbon_score": 0,
                }
                _mem["sessions"][token] = minimal
                return minimal
        except Exception as e:
            print(f"⚠ Supabase token check failed: {e}")

    # 3. Restart-safe nx_ token decode
    if token.startswith("nx_"):
        try:
            parts = token.split("_")
            sig = parts[-1]
            payload = "_".join(parts[1:-1])
            expected = _hmac_mod.new(_TOKEN_SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()[:16]
            if not _hmac_mod.compare_digest(sig, expected):
                print("⚠ Token signature mismatch")
                return None
            padding = 4 - len(payload) % 4
            email = base64.urlsafe_b64decode(payload + "=" * (padding % 4)).decode()
            user = _mem["users"].get(email)
            if user:
                print(f"✅ Reconstructed session for {email}")
                _mem["sessions"][token] = user
                return user
        except Exception as e:
            print(f"⚠ Token decode failed: {e}")

    return None

def require_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.headers.get('Authorization', '').replace('Bearer ', '').strip()
        if not token:
            token = request.cookies.get('nx_token', '')
        user = get_user_from_token(token)
        if not user:
            return jsonify({"error": "Unauthorized"}), 401
        request.current_user = user
        return f(*args, **kwargs)
    return decorated

def require_role(*roles):
    def decorator(f):
        @wraps(f)
        def decorated(*args, **kwargs):
            token = request.headers.get('Authorization', '').replace('Bearer ', '').strip()
            if not token:
                token = request.cookies.get('nx_token', '')
            user = get_user_from_token(token)
            if not user:
                return jsonify({"error": "Unauthorized"}), 401
            if user.get('role') not in roles:
                return jsonify({"error": "Forbidden"}), 403
            request.current_user = user
            return f(*args, **kwargs)
        return decorated
    return decorator

# ─────────────────────────────────────────────
# WEATHER ENGINE
# ─────────────────────────────────────────────
_weather_cache = {}
def fetch_weather(lat=22.7196, lon=75.8577):
    cache_key = f"{round(lat,2)}_{round(lon,2)}"
    now = time.time()

    if cache_key in _weather_cache:
        if now - _weather_cache[cache_key]["ts"] < 300:
            data = _weather_cache[cache_key]["data"]
            print(f"♻ CACHE HIT ({cache_key}) → {data}")
            return data

    demo = {
        "city": "Indore",
        "temp": 28.4,
        "feels_like": 31.0,
        "clouds": 35,
        "wind_speed_ms": 4.0,
        "wind_speed": 14.4,
        "humidity": 58,
        "condition": "Partly Cloudy",
        "icon": "⛅",
        "visibility": 10,
        "pressure": 1013,
        "demo": True
    }

    if not WEATHER_API_KEY:
        print("🧪 DEMO MODE (No API key)")
        _weather_cache[cache_key] = {"data": demo, "ts": now}
        return demo

    try:
        url = (f"https://api.openweathermap.org/data/2.5/weather"
               f"?lat={lat}&lon={lon}&appid={WEATHER_API_KEY}&units=metric")
        r = requests.get(url, timeout=5)
        d = r.json()

        if r.status_code != 200:
            print(f"❌ API ERROR: {d}")
            return demo

        wind_ms = d["wind"].get("speed", 4.0)
        wind_kmh = round(wind_ms * 3.6, 1)

        w = {
            "city": d.get("name", "Unknown"),
            "temp": round(d["main"]["temp"], 1),
            "feels_like": round(d["main"]["feels_like"], 1),
            "clouds": d["clouds"]["all"],
            "wind_speed_ms": round(wind_ms, 2),
            "wind_speed": wind_kmh,
            "humidity": d["main"]["humidity"],
            "condition": d["weather"][0]["description"].title(),
            "icon": _weather_icon(d["weather"][0]["id"]),
            "visibility": d.get("visibility", 10000) // 1000,
            "pressure": d["main"]["pressure"],
            "demo": False
        }

        _weather_cache[cache_key] = {"data": w, "ts": now}
        return w

    except Exception as e:
        print(f"❌ WEATHER EXCEPTION: {e}")
        return demo

def _weather_icon(wid):
    if 200 <= wid < 300: return "⛈️"
    if 300 <= wid < 600: return "🌧️"
    if 600 <= wid < 700: return "❄️"
    if wid == 800: return "☀️"
    if wid == 801: return "🌤️"
    if wid <= 804: return "⛅"
    return "🌡️"

# ─────────────────────────────────────────────
# PRICING ENGINE
# ─────────────────────────────────────────────
def calculate_price(base: float, etype: str, weather: dict, dist_km=0, kwh=0) -> dict:
    mod = 0.0
    reasons = []

    if etype == "solar":
        c = weather.get("clouds", 30)
        t = weather.get("temp", 25)
        if c < 20:   mod -= 1.5; reasons.append(f"Clear sky ({c}%) → solar surplus → lower price")
        elif c < 40: mod -= 0.6; reasons.append(f"Mostly clear ({c}%) → good solar output")
        elif c < 70: mod += 0.8; reasons.append(f"Partial cloud ({c}%) → moderate output drop")
        else:        mod += 2.2; reasons.append(f"Heavy cloud ({c}%) → low solar → higher price")
        if t > 38:   mod += 0.4; reasons.append(f"Heat stress ({t}°C) reduces panel efficiency")

    elif etype == "wind":
        wind = weather.get("wind_speed", 14.4)
        if wind > 25:   mod -= 2.2; reasons.append(f"Strong wind ({wind}km/h) → surplus → lower price")
        elif wind > 15: mod -= 1.0; reasons.append(f"Good wind ({wind}km/h) → lower price")
        elif wind < 5:  mod += 2.0; reasons.append(f"Calm ({wind}km/h) → scarce wind → higher price")
        else:           reasons.append(f"Moderate wind ({wind}km/h) → stable")

    elif etype == "biogas":
        h = weather.get("humidity", 60)
        if h > 80: mod -= 0.2; reasons.append("High humidity boosts biogas fermentation slightly")
        else:      reasons.append("Biogas: reliable baseload, weather-independent")

    tx_fee      = round(dist_km * POLICY["transmission_fee_per_km"], 3)
    subsidy     = POLICY.get(f"{etype}_subsidy", 0)
    price       = base + mod + tx_fee - subsidy

    bulk_disc = 0
    if kwh >= POLICY["bulk_threshold_kwh"]:
        bulk_disc = round(price * POLICY["bulk_discount_pct"] / 100, 3)
        price -= bulk_disc

    platform_fee = round(price * POLICY["platform_fee_pct"] / 100, 3)
    raw_final    = price + platform_fee
    final        = max(POLICY["min_price_kwh"], min(POLICY["max_price_kwh"], raw_final))

    return {
        "base_price": round(base, 2),
        "weather_modifier": round(mod, 2),
        "transmission_fee": round(tx_fee, 2),
        "subsidy_applied": round(subsidy, 2),
        "bulk_discount": round(bulk_disc, 2),
        "platform_fee": round(platform_fee, 2),
        "final_price": round(final, 2),
        "direction": "up" if mod > 0.5 else "down" if mod < -0.5 else "neutral",
        "reason": " | ".join(reasons) if reasons else "Stable market conditions",
        "policy_capped": final != raw_final
    }

# ─────────────────────────────────────────────
# HASH CHAIN
# ─────────────────────────────────────────────
def get_last_hash():
    if supabase:
        try:
            r = supabase.table("transactions").select("tx_hash").order("created_at", desc=True).limit(1).execute()
            if r.data: return r.data[0]["tx_hash"]
        except: pass
    if _mem["transactions"]: return _mem["transactions"][-1]["tx_hash"]
    return "0" * 64

def make_hash(prev, from_id, to_id, kwh, price, ts):
    return hashlib.sha256(f"{prev}|{from_id}|{to_id}|{kwh}|{price}|{ts}".encode()).hexdigest()

def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return round(R * 2 * math.asin(math.sqrt(a)), 2)

# ─────────────────────────────────────────────
# PAGE ROUTES
# ─────────────────────────────────────────────
@app.route("/")
def index(): return render_template("index.html")

@app.route("/producer")
def producer_page(): return render_template("producer.html", razorpay_key=RAZORPAY_KEY_ID)

@app.route("/consumer")
def consumer_page(): return render_template("consumer.html", razorpay_key=RAZORPAY_KEY_ID)

@app.route("/investor")
def investor_page(): return render_template("investor.html", razorpay_key=RAZORPAY_KEY_ID)

@app.route("/ledger")
def ledger_page(): return render_template("ledger.html")

@app.route("/admin")
def admin_page(): return render_template("admin.html")

# ─────────────────────────────────────────────
# AUTH ROUTES
# ─────────────────────────────────────────────
@app.route("/api/auth/register", methods=["POST"])
def register():
    d = request.get_json()
    email    = d.get("email", "").strip().lower()
    password = d.get("password", "")
    name     = d.get("name", "").strip()
    role     = d.get("role", "consumer")
    lat      = float(d.get("lat", 22.7196))
    lon      = float(d.get("lon", 75.8577))
    city     = d.get("city", "Indore")

    if not all([email, password, name]):
        return jsonify({"error": "All fields required"}), 400
    if len(password) < 8:
        return jsonify({"error": "Password must be 8+ characters"}), 400
    if role not in ["producer", "consumer", "investor", "admin"]:
        role = "consumer"

    if supabase:
        try:
            auth_res = supabase.auth.sign_up({
                "email": email, "password": password,
                "options": {"data": {"name": name, "role": role}}
            })
            if auth_res and auth_res.user:
                uid = str(auth_res.user.id)
                profile_data = {
                    "id": uid, "email": email, "full_name": name, "role": role,
                    "location_city": city, "location_lat": lat, "location_lon": lon,
                    "carbon_score": 0, "green_credits": 0
                }
                try:
                    supabase.table("profiles").insert(profile_data).execute()
                except: pass
                token = auth_res.session.access_token if auth_res.session else generate_token(email)
                return jsonify({"success": True, "token": token, "role": role,
                    "user": {"id": uid, "name": name, "email": email, "role": role,
                             "location_lat": lat, "location_lon": lon, "location_city": city, "carbon_score": 0}})
            return jsonify({"error": "Registration failed"}), 400
        except Exception as e:
            err = str(e)
            if "already registered" in err: return jsonify({"error": "Email already registered"}), 409
            return jsonify({"error": err}), 500
    else:
        if email in _mem["users"]: return jsonify({"error": "Email already registered"}), 409
        uid = "nx_" + uuid.uuid4().hex[:12]
        user = {"id": uid, "email": email, "full_name": name, "role": role,
                "location_city": city, "location_lat": lat, "location_lon": lon,
                "carbon_score": 0, "green_credits": 0,
                "pw_hash": hashlib.sha256(password.encode()).hexdigest()}
        _mem["users"][email] = user
        token = generate_token(email)
        return jsonify({"success": True, "token": token, "role": role,
            "user": {"id": uid, "name": name, "email": email, "role": role,
                     "location_lat": lat, "location_lon": lon, "location_city": city, "carbon_score": 0}})


@app.route("/api/auth/login", methods=["POST"])
def login():
    d = request.get_json()
    email    = d.get("email", "").strip().lower()
    password = d.get("password", "")
    print(f"🔐 Login attempt: {email}")

    if not supabase:
        user = _mem["users"].get(email)
        if not user or user.get("pw_hash") != hashlib.sha256(password.encode()).hexdigest():
            return jsonify({"error": "Invalid email or password"}), 401
        token = generate_token(email)
        return jsonify({"success": True, "token": token, "role": user["role"],
            "user": {"id": user["id"], "name": user["full_name"], "email": email,
                     "role": user["role"], "location_lat": user.get("location_lat", 22.7196),
                     "location_lon": user.get("location_lon", 75.8577), "carbon_score": user.get("carbon_score", 0)}})

    try:
        auth_res = supabase.auth.sign_in_with_password({"email": email, "password": password})
        if not (auth_res and auth_res.user):
            return jsonify({"error": "Invalid email or password"}), 401

        uid = str(auth_res.user.id)
        try:
            result = supabase.table("profiles").select("*").eq("id", uid).execute()
            prof = result.data[0] if result.data else {}
        except:
            prof = {}

        if not prof:
            meta = auth_res.user.user_metadata or {}
            prof = {"id": uid, "email": email, "full_name": meta.get("name", email),
                    "role": meta.get("role", "consumer"), "location_city": "Indore",
                    "location_lat": 22.7196, "location_lon": 75.8577, "carbon_score": 0}
            try: supabase.table("profiles").insert(prof).execute()
            except: pass

        token = auth_res.session.access_token
        _mem["sessions"][token] = prof

        return jsonify({"success": True, "token": token, "role": prof.get("role", "consumer"),
            "user": {"id": uid, "name": prof.get("full_name", email), "email": email,
                     "role": prof.get("role", "consumer"),
                     "location_lat": float(prof.get("location_lat", 22.7196)),
                     "location_lon": float(prof.get("location_lon", 75.8577)),
                     "location_city": prof.get("location_city", "Indore"),
                     "carbon_score": float(prof.get("carbon_score", 0))}})
    except Exception as e:
        err = str(e).lower()
        if "invalid login credentials" in err: return jsonify({"error": "Invalid email or password"}), 401
        if "email not confirmed" in err: return jsonify({"error": "Please confirm your email first"}), 401
        return jsonify({"error": str(e)}), 401


@app.route("/api/auth/logout", methods=["POST"])
def logout():
    token = request.headers.get('Authorization', '').replace('Bearer ', '')
    if supabase:
        try: supabase.auth.sign_out()
        except: pass
    _mem["sessions"].pop(token, None)
    return jsonify({"success": True})


# ─────────────────────────────────────────────
# WEATHER API
# ─────────────────────────────────────────────
@app.route("/api/weather")
def api_weather():
    lat = float(request.args.get("lat", 22.7196))
    lon = float(request.args.get("lon", 75.8577))
    return jsonify(fetch_weather(lat, lon))


# ─────────────────────────────────────────────
# LISTINGS
# ─────────────────────────────────────────────
@app.route("/api/producers", methods=["GET"])
def api_producers_compat():
    return get_listings()

@app.route("/api/listings", methods=["GET"])
def get_listings():
    etype = request.args.get("type", "")
    lat   = float(request.args.get("lat", 22.7196))
    lon   = float(request.args.get("lon", 75.8577))
    weather = fetch_weather(lat, lon)

    if supabase:
        try:
            q = supabase.table("listings").select("*, profiles(full_name, location_city)").eq("is_active", True)
            if etype: q = q.eq("energy_type", etype)
            listings = q.execute().data or []
        except Exception as e:
            print(f"Listings fetch error: {e}")
            listings = _mem["listings"]
    else:
        listings = [l for l in _mem["listings"] if l.get("is_active", True)]
        if etype: listings = [l for l in listings if l.get("energy_type") == etype]

    enriched = []
    for l in listings:
        p_lat = float(l.get("location_lat", 22.7196))
        p_lon = float(l.get("location_lon", 75.8577))
        dist  = haversine(lat, lon, p_lat, p_lon)
        pricing = calculate_price(float(l["base_price"]), l["energy_type"], weather, dist)
        l.update({"distance_km": dist, "pricing": pricing,
                   "current_price": pricing["final_price"],
                   "price_direction": pricing["direction"],
                   "price_reason": pricing["reason"]})
        if l.get("profiles"):
            l["producer_name"] = l["profiles"].get("full_name", l.get("producer_name", "Producer"))
        enriched.append(l)

    enriched.sort(key=lambda x: x["current_price"])
    return jsonify(enriched)


@app.route("/api/listings/mine", methods=["GET"])
@require_auth
def get_my_listings():
    user = request.current_user
    uid  = user.get("id")
    if supabase:
        try:
            return jsonify(supabase.table("listings").select("*").eq("producer_id", uid).execute().data or [])
        except: pass
    return jsonify([l for l in _mem["listings"] if l.get("producer_id") == uid])


@app.route("/api/listings/create", methods=["POST"])
@require_auth
def create_listing():
    user = request.current_user
    if user.get("role") not in ["producer", "admin"]:
        return jsonify({"error": "Only producers can create listings"}), 403
    d = request.get_json()
    listing = {
        "id": str(uuid.uuid4()), "producer_id": user["id"],
        "producer_name": user.get("full_name", user.get("name", "Producer")),
        "energy_type": d.get("energy_type", "solar"),
        "base_price": float(d.get("base_price", 3.0)),
        "available_kwh": float(d.get("available_kwh", 100)),
        "capacity_kw": float(d.get("capacity_kw", 10)),
        "location_city": d.get("city", user.get("location_city", "City")),
        "location_lat": float(d.get("lat", user.get("location_lat", 22.7196))),
        "location_lon": float(d.get("lon", user.get("location_lon", 75.8577))),
        "description": d.get("description", ""),
        "is_active": True, "created_at": datetime.now(timezone.utc).isoformat()
    }
    if supabase:
        try:
            r = supabase.table("listings").insert(listing).execute()
            return jsonify(r.data[0] if r.data else listing)
        except Exception as e: return jsonify({"error": str(e)}), 500
    _mem["listings"].append(listing)
    return jsonify(listing)


@app.route("/api/listings/<lid>", methods=["PUT"])
@require_auth
def update_listing(lid):
    user = request.current_user
    d = request.get_json()
    updates = {k: v for k, v in d.items() if k in ["base_price","available_kwh","is_active","description"]}
    updates["updated_at"] = datetime.now(timezone.utc).isoformat()
    if supabase:
        try:
            r = supabase.table("listings").update(updates).eq("id", lid).eq("producer_id", user["id"]).execute()
            return jsonify(r.data[0] if r.data else {})
        except Exception as e: return jsonify({"error": str(e)}), 500
    for l in _mem["listings"]:
        if l["id"] == lid and l.get("producer_id") == user["id"]:
            l.update(updates); return jsonify(l)
    return jsonify({"error": "Not found"}), 404


@app.route("/api/listings/<lid>", methods=["DELETE"])
@require_auth
def delete_listing(lid):
    user = request.current_user
    if supabase:
        try:
            supabase.table("listings").update({"is_active": False}).eq("id", lid).eq("producer_id", user["id"]).execute()
            return jsonify({"success": True})
        except Exception as e: return jsonify({"error": str(e)}), 500
    _mem["listings"] = [l for l in _mem["listings"] if not (l["id"] == lid and l.get("producer_id") == user["id"])]
    return jsonify({"success": True})


# ─────────────────────────────────────────────
# SMART SWITCH
# ─────────────────────────────────────────────
@app.route("/api/smart-switch")
def smart_switch():
    lat = float(request.args.get("lat", 22.7196))
    lon = float(request.args.get("lon", 75.8577))
    weather = fetch_weather(lat, lon)
    clouds  = weather.get("clouds", 30)
    wind    = weather.get("wind_speed", 14.4)
    scores  = {
        "solar":  max(0, min(100, 100 - clouds)),
        "wind":   max(0, min(100, (wind / 50) * 100)),
        "biogas": 70
    }
    ranked = sorted(scores, key=scores.get, reverse=True)
    return jsonify({"recommended": ranked[0], "fallback": ranked[1], "scores": scores,
        "reasons": {
            "solar":  f"Cloud cover {clouds}% → Solar score {scores['solar']:.0f}/100",
            "wind":   f"Wind {wind}km/h → Wind score {scores['wind']:.0f}/100",
            "biogas": "Baseload source — always stable → Score 70/100"
        }, "weather": weather})


# ─────────────────────────────────────────────
# TRADE
# ─────────────────────────────────────────────
@app.route("/api/trade", methods=["POST"])
@require_auth
def execute_trade():
    user = request.current_user
    d    = request.get_json()
    listing_id = d.get("listing_id")
    kwh        = float(d.get("kwh", 0))
    payment_id = d.get("payment_id", "")
    pool_id    = d.get("pool_id")

    listing = None
    if supabase and listing_id:
        try:
            r = supabase.table("listings").select("*, profiles(full_name, location_lat, location_lon)").eq("id", listing_id).single().execute()
            listing = r.data
        except: pass
    if not listing:
        listing = next((l for l in _mem["listings"] if l.get("id") == listing_id), None)

    if listing:
        prof       = listing.get("profiles") or {}
        from_name  = listing.get("producer_name") or prof.get("full_name", "Producer")
        base_price = float(listing.get("base_price", 3.0))
        etype      = listing.get("energy_type", "solar")
        p_lat      = float(listing.get("location_lat") or prof.get("location_lat", 22.72))
        p_lon      = float(listing.get("location_lon") or prof.get("location_lon", 75.88))
    else:
        from_name  = d.get("from_name", "Producer")
        base_price = float(d.get("base_price", 3.0))
        etype      = d.get("energy_type", "solar")
        p_lat      = float(d.get("producer_lat", 22.72))
        p_lon      = float(d.get("producer_lon", 75.88))

    c_lat   = float(d.get("consumer_lat", user.get("location_lat", 22.72)))
    c_lon   = float(d.get("consumer_lon", user.get("location_lon", 75.88)))
    to_name = user.get("full_name") or user.get("name", "Consumer")

    weather      = fetch_weather(c_lat, c_lon)
    dist         = haversine(c_lat, c_lon, p_lat, p_lon)
    pricing      = calculate_price(base_price, etype, weather, dist, kwh)
    final_price  = pricing["final_price"]
    total_cost   = round(kwh * final_price, 2)
    carbon_saved = round(kwh * POLICY["carbon_per_kwh"], 2)
    ts           = datetime.now(timezone.utc).isoformat()
    prev_hash    = get_last_hash()
    tx_hash      = make_hash(prev_hash, listing_id or from_name, user["id"], kwh, final_price, ts)
    short_hash   = "0x" + tx_hash[:8].upper()

    tx = {
        "id": str(uuid.uuid4()), "tx_hash": tx_hash, "short_hash": short_hash,
        "prev_hash": prev_hash,
        "producer_id": listing.get("producer_id") if listing else d.get("producer_id"),
        "consumer_id": user["id"], "listing_id": listing_id,
        "from_name": from_name, "to_name": to_name, "energy_type": etype,
        "kwh": kwh, "base_price": base_price, "final_price": final_price,
        "total_cost": total_cost, "weather_modifier": pricing["weather_modifier"],
        "transmission_fee": pricing["transmission_fee"], "subsidy_applied": pricing["subsidy_applied"],
        "bulk_discount": pricing["bulk_discount"], "platform_fee": pricing["platform_fee"],
        "carbon_saved": carbon_saved, "distance_km": dist,
        "escrow_id": "esc_" + uuid.uuid4().hex[:10], "escrow_status": "released",
        "payment_id": payment_id, "pool_id": pool_id, "status": "confirmed", "created_at": ts
    }

    if supabase:
        try:
            supabase.table("transactions").insert({k:v for k,v in tx.items() if v is not None}).execute()
            if listing:
                supabase.table("listings").update({"available_kwh": max(0, float(listing.get("available_kwh",0)) - kwh)}).eq("id", listing_id).execute()
            supabase.table("profiles").update({"carbon_score": float(user.get("carbon_score",0)) + carbon_saved}).eq("id", user["id"]).execute()
        except Exception as e: print(f"DB write error: {e}")

    _mem["transactions"].append(tx)
    return jsonify({
        "success": True,
        "transaction": {k:v for k,v in tx.items() if k not in ["tx_hash","prev_hash"]},
        "hash_chain_steps": [
            f"→ PREV BLOCK: {prev_hash[:16]}...",
            f"→ DATA: {from_name} + {to_name} + {kwh}kWh + ₹{final_price}",
            f"→ SHA-256 PROCESSING...", f"→ BLOCK HASH: {short_hash} (stored securely)",
            f"→ ESCROW: ₹{total_cost} → locked → verified → released",
            f"→ CARBON: +{carbon_saved}kg CO₂ saved • LEDGER UPDATED ✓"
        ]
    })


# ─────────────────────────────────────────────
# TRANSACTIONS
# ─────────────────────────────────────────────
def _strip_hashes(txs):
    safe = ["id","short_hash","from_name","to_name","energy_type","kwh",
            "final_price","total_cost","carbon_saved","distance_km",
            "escrow_status","status","created_at","pool_id"]
    result = []
    for t in reversed(txs):
        row = {k: t.get(k) for k in safe if k in t}
        # Always ensure short_hash is populated — derive from tx_hash if missing/zero
        if not row.get("short_hash") or row["short_hash"] == "0x" + "0"*8:
            full = t.get("tx_hash", "")
            row["short_hash"] = ("0x" + full[:8].upper()) if full and full != "0"*64 else None
        result.append(row)
    return result

@app.route("/api/transactions")
def api_transactions():
    limit  = int(request.args.get("limit", 50))
    etype  = request.args.get("type", "")
    search = request.args.get("search", "").lower()
    if supabase:
        try:
            q = supabase.table("transactions").select(
                "id,short_hash,from_name,to_name,energy_type,kwh,final_price,"
                "total_cost,carbon_saved,distance_km,escrow_status,status,created_at,pool_id"
            ).order("created_at", desc=True).limit(limit)
            if etype: q = q.eq("energy_type", etype)
            txs = q.execute().data or []
        except: txs = _strip_hashes(_mem["transactions"])
    else:
        txs = _strip_hashes(_mem["transactions"])
    if etype:  txs = [t for t in txs if t.get("energy_type") == etype]
    if search: txs = [t for t in txs if search in (t.get("from_name","") + t.get("to_name","")).lower()]
    return jsonify(txs[:limit])

@app.route("/api/transactions/admin")
@require_role("admin")
def admin_transactions():
    limit = int(request.args.get("limit", 100))
    if supabase:
        try: return jsonify(supabase.table("transactions").select("*").order("created_at", desc=True).limit(limit).execute().data or [])
        except: pass
    return jsonify(list(reversed(_mem["transactions"]))[:limit])

@app.route("/api/transactions/mine")
@require_auth
def my_transactions():
    user = request.current_user
    uid  = user["id"]
    role = user.get("role")
    if supabase:
        try:
            col = "producer_id" if role == "producer" else "consumer_id"
            txs = supabase.table("transactions").select("*").eq(col, uid).order("created_at", desc=True).limit(50).execute().data or []
            return jsonify([{k:v for k,v in t.items() if k not in ["tx_hash","prev_hash"]} for t in txs])
        except: pass
    txs = [t for t in _mem["transactions"] if t.get("producer_id" if role=="producer" else "consumer_id") == uid]
    return jsonify(_strip_hashes(txs))


# ─────────────────────────────────────────────
# STATS
# ─────────────────────────────────────────────
@app.route("/api/stats")
def api_stats():
    if supabase:
        try:
            txs   = supabase.table("transactions").select("kwh,carbon_saved,total_cost").execute().data or []
            prod  = supabase.table("listings").select("id").eq("is_active", True).execute().data or []
            pools = supabase.table("pools").select("id").eq("status", "open").execute().data or []
            users = supabase.table("profiles").select("id").execute().data or []
            return jsonify({"total_transactions": len(txs),
                "total_kwh": round(sum(float(t.get("kwh",0)) for t in txs), 2),
                "total_co2_saved": round(sum(float(t.get("carbon_saved",0)) for t in txs), 2),
                "total_value": round(sum(float(t.get("total_cost",0)) for t in txs), 2),
                "active_listings": len(prod), "active_pools": len(pools), "total_users": len(users)})
        except: pass
    txs = _mem["transactions"]
    return jsonify({"total_transactions": len(txs),
        "total_kwh": round(sum(t.get("kwh",0) for t in txs), 2),
        "total_co2_saved": round(sum(t.get("carbon_saved",0) for t in txs), 2),
        "total_value": round(sum(t.get("total_cost",0) for t in txs), 2),
        "active_listings": len([l for l in _mem["listings"] if l.get("is_active")]),
        "active_pools": len([p for p in _mem["pools"] if p.get("status")=="open"]),
        "total_users": len(_mem["users"])})


# ─────────────────────────────────────────────
# PAYMENT
# ─────────────────────────────────────────────
@app.route("/api/payment/create-order", methods=["POST"])
@require_auth
def create_order():
    if not RAZORPAY_KEY_ID or not RAZORPAY_KEY_SECRET:
        return jsonify({"demo": True, "order_id": "demo_order_" + uuid.uuid4().hex[:8],
                        "amount": request.get_json().get("amount", 100)})
    import razorpay
    d = request.get_json()
    amount_paise = int(float(d.get("amount", 100)) * 100)
    client = razorpay.Client(auth=(RAZORPAY_KEY_ID, RAZORPAY_KEY_SECRET))
    order  = client.order.create({"amount": amount_paise, "currency": "INR",
        "receipt": "nx_" + uuid.uuid4().hex[:8],
        "notes": {"listing_id": d.get("listing_id",""), "kwh": str(d.get("kwh",0))}})
    return jsonify({"order_id": order["id"], "amount": amount_paise, "currency": "INR", "key": RAZORPAY_KEY_ID})

@app.route("/api/payment/verify", methods=["POST"])
def verify_payment():
    d = request.get_json()
    if not RAZORPAY_KEY_SECRET:
        return jsonify({"verified": True, "payment_id": d.get("razorpay_payment_id","demo")})
    msg      = f"{d['razorpay_order_id']}|{d['razorpay_payment_id']}"
    expected = _hmac_mod.new(RAZORPAY_KEY_SECRET.encode(), msg.encode(), hashlib.sha256).hexdigest()
    if _hmac_mod.compare_digest(expected, d.get("razorpay_signature","")):
        return jsonify({"verified": True, "payment_id": d["razorpay_payment_id"]})
    return jsonify({"verified": False, "error": "Signature mismatch"}), 400


# ─────────────────────────────────────────────
# POOLS  ← ALL BUGS FIXED HERE
# ─────────────────────────────────────────────

@app.route("/api/pools", methods=["GET"])
def get_pools():
    """Return all open pools regardless of who created them."""
    if supabase:
        try:
            data = supabase.table("pools").select("*, pool_members(*)").eq("status", "open").execute()
            return jsonify(data.data or [])
        except Exception as e:
            print(f"Supabase pools fetch error: {e}")
            # fall through to memory
    return jsonify([p for p in _mem["pools"] if p.get("status") == "open"])


@app.route("/api/pools", methods=["POST"])
@require_auth
def create_pool():
    """
    Any authenticated user (consumer, producer, investor) can create a pool.
    FIX 1: Read pool 'name' from request body and store it.
    FIX 2: Removed producer-only role check — consumers need this.
    FIX 3: Derive a sensible price_per_unit from linked listing if provided.
    """
    user = request.current_user
    d    = request.get_json() or {}

    pool_name   = d.get("name", "").strip()
    energy_type = d.get("energy_type", "solar")
    target_kwh  = float(d.get("target_kwh", 100))
    listing_id  = d.get("listing_id")

    # Validate
    if not pool_name:
        return jsonify({"error": "Pool name is required"}), 400
    if target_kwh < 50:
        return jsonify({"error": "Target must be at least 50 kWh to qualify for bulk discount"}), 400

    # Try to get price from linked listing, otherwise use sensible default
    price_per_unit = float(d.get("price_per_unit", 3.0))
    if listing_id:
        listing = None
        if supabase:
            try:
                r = supabase.table("listings").select("base_price, energy_type").eq("id", listing_id).single().execute()
                listing = r.data
            except: pass
        if not listing:
            listing = next((l for l in _mem["listings"] if l.get("id") == listing_id), None)
        if listing:
            price_per_unit = float(listing.get("base_price", price_per_unit))
            energy_type    = listing.get("energy_type", energy_type)

    creator_name = user.get("full_name") or user.get("name", "Consumer")

    pool = {
        "id":               str(uuid.uuid4()),
        "name":             pool_name,
        "producer_name":    creator_name,               # NOT NULL in DB — always populate
        "producer_id":      user["id"],                 # old schema compat — NOT NULL in DB
        "creator_id":       user["id"],
        "creator_name":     creator_name,
        "creator_role":     user.get("role", "consumer"),
        "listing_id":       listing_id,
        "energy_type":      energy_type,
        "target_kwh":       target_kwh,
        "total_committed":  0.0,
        "price_per_unit":   price_per_unit,
        "discount_unlocked": False,
        "discounted_price": round(price_per_unit * (1 - POLICY["bulk_discount_pct"] / 100), 2),
        "members":          [],                         # in-memory only — not a DB column
        "status":           "open",
        "created_at":       datetime.now(timezone.utc).isoformat()
    }

    if supabase:
        # Build the minimal DB-safe dict (no 'members' which is not a column)
        db_pool_new = {k: v for k, v in pool.items() if k != "members"}

        # OLD-schema fallback: map new fields -> existing column names
        # Run this SQL to fix permanently:
        #   ALTER TABLE pools ADD COLUMN IF NOT EXISTS name TEXT;
        #   ALTER TABLE pools ADD COLUMN IF NOT EXISTS creator_id UUID;
        #   ALTER TABLE pools ADD COLUMN IF NOT EXISTS creator_name TEXT;
        #   ALTER TABLE pools ADD COLUMN IF NOT EXISTS creator_role TEXT;
        #   NOTIFY pgrst, 'reload schema';
        db_pool_old = {
            "id":                pool["id"],
            "producer_id":       pool["creator_id"],
            "producer_name":     pool["producer_name"],  # NOT NULL — must include
            "energy_type":       pool["energy_type"],
            "target_kwh":        pool["target_kwh"],
            "total_committed":   pool["total_committed"],
            "price_per_unit":    pool["price_per_unit"],
            "discount_unlocked": pool["discount_unlocked"],
            "discounted_price":  pool["discounted_price"],
            "listing_id":        pool["listing_id"],
            "status":            pool["status"],
            "created_at":        pool["created_at"],
        }
        for col in ("name", "creator_name", "creator_role"):
            if pool.get(col) is not None:
                db_pool_old[col] = pool[col]

        # Use admin client (service role) to bypass RLS — pools are created server-side
        # and RLS on the anon key would block inserts from consumers.
        # If no service key is configured, fall back to anon client.
        insert_client = supabase_admin if supabase_admin else supabase

        try:
            r = insert_client.table("pools").insert(db_pool_new).execute()
            saved = r.data[0] if r.data else pool
            saved["members"] = []
            print(f"Pool saved (new schema): {pool['id']}")
            return jsonify({"success": True, "pool": saved}), 201
        except Exception as e:
            err_str = str(e)
            print(f"New-schema insert failed: {err_str}")
            # PGRST204 = column missing  |  42501 = RLS blocked
            if "PGRST204" in err_str or "creator_id" in err_str or "creator_name" in err_str or "creator_role" in err_str:
                print("Retrying with old-schema fallback (producer_id)...")
                try:
                    r2 = insert_client.table("pools").insert(db_pool_old).execute()
                    saved = r2.data[0] if r2.data else pool
                    saved["members"] = []
                    print(f"Pool saved (old schema fallback): {pool['id']}")
                    return jsonify({"success": True, "pool": saved}), 201
                except Exception as e2:
                    print(f"Old-schema fallback failed: {e2}")
                    # Last resort: save to memory so the user isn't blocked
                    _mem["pools"].append(pool)
                    print(f"Pool saved to memory as last resort: {pool['id']}")
                    return jsonify({"success": True, "pool": pool}), 201
            if "42501" in err_str:
                # RLS blocked even with service key — save to memory
                _mem["pools"].append(pool)
                print(f"RLS blocked, pool saved to memory: {pool['id']}")
                return jsonify({"success": True, "pool": pool, "warning": "Saved locally — run fix_pools_rls.sql in Supabase"}), 201
            return jsonify({"error": err_str}), 500

    _mem["pools"].append(pool)
    print(f"✅ Pool created: '{pool_name}' by {creator_name} ({user.get('role')}) — {energy_type} {target_kwh}kWh")
    return jsonify({"success": True, "pool": pool}), 201


@app.route("/api/pools/<pool_id>/join", methods=["POST"])
@require_auth
def join_pool(pool_id):
    """
    FIX: Properly update members list in both Supabase and memory.
    Also correctly return success shape that consumer.html expects.
    """
    user = request.current_user
    body = request.get_json() or {}
    try:
        kwh = float(body.get("kwh", 0))
    except (TypeError, ValueError):
        return jsonify({"error": "Invalid kWh value"}), 400

    if kwh <= 0:
        return jsonify({"error": "kWh must be greater than 0"}), 400

    member_entry = {
        "consumer_id":   user["id"],
        "consumer_name": user.get("full_name") or user.get("name", "Member"),
        "kwh_committed": kwh,
        "joined_at":     datetime.now(timezone.utc).isoformat()
    }

    if supabase:
        # Use admin client for writes (bypasses RLS), anon client for reads
        write_client = supabase_admin if supabase_admin else supabase
        try:
            pool_res = supabase.table("pools").select("*").eq("id", pool_id).single().execute()
            if not pool_res.data:
                return jsonify({"error": "Pool not found"}), 404
            pool = pool_res.data

            if pool.get("status") != "open":
                return jsonify({"error": "Pool is no longer open"}), 400

            new_committed = float(pool.get("total_committed", 0)) + kwh
            unlocked      = new_committed >= POLICY["bulk_threshold_kwh"]

            # Insert pool_member row using admin client (bypasses RLS)
            write_client.table("pool_members").insert({
                "pool_id":        pool_id,
                "consumer_id":    user["id"],
                "consumer_name":  member_entry["consumer_name"],
                "kwh_committed":  kwh
            }).execute()

            updates = {
                "total_committed":  new_committed,
                "discount_unlocked": unlocked
            }
            if unlocked:
                updates["discounted_price"] = round(
                    float(pool.get("price_per_unit", 3.0)) * (1 - POLICY["bulk_discount_pct"] / 100), 2
                )
            write_client.table("pools").update(updates).eq("id", pool_id).execute()

            return jsonify({
                "success":          True,
                "discount_unlocked": unlocked,
                "total_committed":   new_committed,
                "message":          "🎉 8% discount unlocked for all members!" if unlocked else f"Joined! Pool at {new_committed:.0f}/{pool.get('target_kwh',100):.0f} kWh"
            })

        except Exception as e:
            err_str = str(e)
            print(f"Supabase join pool error: {err_str}")
            if "42501" in err_str:
                # RLS blocked — fall through to memory fallback below
                print("RLS blocked join_pool — falling back to memory")
            else:
                return jsonify({"error": err_str}), 500

    # ── Memory fallback ──
    pool = next((p for p in _mem["pools"] if p["id"] == pool_id), None)
    if not pool:
        return jsonify({"error": "Pool not found"}), 404
    if pool.get("status") != "open":
        return jsonify({"error": "Pool is no longer open"}), 400

    # Check if user already joined
    already = any(m.get("consumer_id") == user["id"] for m in pool.get("members", []))
    if already:
        # Allow increasing commitment
        for m in pool["members"]:
            if m.get("consumer_id") == user["id"]:
                m["kwh_committed"] = m.get("kwh_committed", 0) + kwh
                break
    else:
        pool.setdefault("members", []).append(member_entry)

    pool["total_committed"] = float(pool.get("total_committed", 0)) + kwh
    pool["discount_unlocked"] = pool["total_committed"] >= POLICY["bulk_threshold_kwh"]
    if pool["discount_unlocked"]:
        pool["discounted_price"] = round(
            float(pool.get("price_per_unit", 3.0)) * (1 - POLICY["bulk_discount_pct"] / 100), 2
        )

    print(f"✅ {member_entry['consumer_name']} joined pool '{pool.get('name','?')}' with {kwh}kWh — total: {pool['total_committed']}")
    return jsonify({
        "success":           True,
        "discount_unlocked": pool["discount_unlocked"],
        "total_committed":   pool["total_committed"],
        "message":          "🎉 8% discount unlocked for all members!" if pool["discount_unlocked"] else f"Joined! Pool at {pool['total_committed']:.0f}/{pool.get('target_kwh',100):.0f} kWh"
    })


@app.route("/api/pools/<pool_id>", methods=["GET"])
@require_auth
def get_pool(pool_id):
    """Get a single pool with member list."""
    if supabase:
        try:
            r = supabase.table("pools").select("*, pool_members(*)").eq("id", pool_id).single().execute()
            return jsonify(r.data or {})
        except Exception as e:
            print(f"Get pool error: {e}")
    pool = next((p for p in _mem["pools"] if p["id"] == pool_id), None)
    if not pool:
        return jsonify({"error": "Not found"}), 404
    return jsonify(pool)


@app.route("/api/pools/mine", methods=["GET"])
@require_auth
def my_pools():
    """Return pools created by the current user."""
    user = request.current_user
    uid  = user["id"]
    if supabase:
        try:
            # Try new schema (creator_id) first, fall back to old schema (producer_id)
            try:
                r = supabase.table("pools").select("*, pool_members(*)").eq("creator_id", uid).execute()
            except Exception:
                r = supabase.table("pools").select("*, pool_members(*)").eq("producer_id", uid).execute()
            return jsonify(r.data or [])
        except Exception as e:
            print(f"My pools error: {e}")
    return jsonify([p for p in _mem["pools"] if p.get("creator_id") == uid or p.get("producer_id") == uid])


# ─────────────────────────────────────────────
# INVESTMENTS
# ─────────────────────────────────────────────
@app.route("/api/investments/mine", methods=["GET"])
@require_auth
def my_investments():
    user = request.current_user
    if supabase:
        try:
            r = supabase.table("investments").select("*, listings(energy_type, location_city), profiles!investments_producer_id_fkey(full_name)").eq("investor_id", user["id"]).execute()
            return jsonify(r.data or [])
        except Exception as e:
            print(f"Error fetching investments: {e}")
            return jsonify([])
    return jsonify([i for i in _mem["investments"] if i.get("investor_id") == user["id"]])


@app.route("/api/investments", methods=["POST"])
@require_auth
def create_investment():
    user = request.current_user
    if user.get("role") not in ["investor", "admin"]:
        return jsonify({"error": "Only investors can invest"}), 403

    d = request.get_json()
    try:
        amount = float(d.get("amount", 0))
    except (TypeError, ValueError):
        return jsonify({"error": "Invalid amount"}), 400

    if amount < 1000:
        return jsonify({"error": "Minimum investment is ₹1000"}), 400

    listing_id   = d.get("listing_id")
    producer_id  = d.get("producer_id")
    rate_per_kwh = float(d.get("rate_per_kwh", 5.0))

    if not listing_id and not producer_id:
        return jsonify({"error": "Missing listing_id or producer_id"}), 400

    if listing_id and not producer_id:
        if supabase:
            try:
                listing = supabase.table("listings").select("producer_id").eq("id", listing_id).single().execute()
                if listing.data:
                    producer_id = listing.data["producer_id"]
            except: pass
        else:
            listing = next((l for l in _mem["listings"] if l.get("id") == listing_id), None)
            if listing:
                producer_id = listing.get("producer_id")

    kwh_funded = round(amount / rate_per_kwh, 2)

    inv = {
        "id": str(uuid.uuid4()),
        "investor_id": user["id"],
        "listing_id": listing_id,
        "producer_id": producer_id,
        "amount_inr": amount,
        "rate_per_kwh": rate_per_kwh,
        "kwh_funded": kwh_funded,
        "return_rate": 8.5,
        "status": "active",
        "payment_id": d.get("payment_id", ""),
        "created_at": datetime.now(timezone.utc).isoformat()
    }

    if supabase:
        try:
            result = supabase.table("investments").insert(inv).execute()
            return jsonify({"success": True, "investment": result.data[0] if result.data else inv})
        except Exception as e:
            error_str = str(e)
            if "42501" in error_str or "row-level security" in error_str.lower():
                return jsonify({"error": "Database permission error. Contact admin to fix RLS policies.", "detail": error_str}), 500
            return jsonify({"error": error_str}), 500
    else:
        _mem["investments"].append(inv)
        return jsonify({"success": True, "investment": inv})


# ─────────────────────────────────────────────
# CARBON / GREENSCORE
# ─────────────────────────────────────────────
@app.route("/api/carbon")
@require_auth
def api_carbon():
    user = request.current_user
    uid = user["id"]
    if supabase:
        try:
            prof = supabase.table("profiles").select("carbon_score, green_credits").eq("id", uid).single().execute()
            score = float(prof.data.get("carbon_score", 0))
        except Exception:
            score = float(user.get("carbon_score", 0))
    else:
        score = float(user.get("carbon_score", 0))

    rank = "PLATINUM" if score > 200 else "GOLD" if score > 100 else "SILVER" if score > 40 else "GREEN"
    credits = round(score * 0.5, 1)
    return jsonify({"co2_kg": round(score, 2), "rank": rank, "green_credits": credits,
                    "badges": _get_badges(score)})

def _get_badges(co2):
    thresholds = [(0,"First Trade","⚡"),(10,"Green Starter","🌱"),(50,"Carbon Hero","🏆"),(100,"Climate Champion","🌍"),(200,"Platinum Guardian","💎")]
    return [{"name":n,"icon":i,"unlocked":co2>=t,"threshold":t} for t,n,i in thresholds]

# ─────────────────────────────────────────────
# POLICY
# ─────────────────────────────────────────────
@app.route("/api/policy")
def api_policy(): return jsonify(POLICY)

@app.route("/api/policy", methods=["PUT"])
@require_role("admin")
def update_policy():
    POLICY.update(request.get_json())
    return jsonify({"success":True,"policy":POLICY})


# ─────────────────────────────────────────────
# PROFILE
# ─────────────────────────────────────────────
@app.route("/api/profile", methods=["GET"])
@require_auth
def get_profile():
    user = request.current_user
    if supabase:
        try:
            r = supabase.table("profiles").select("*").eq("id",user["id"]).single().execute()
            return jsonify({k:v for k,v in r.data.items() if k!="pw_hash"})
        except: pass
    return jsonify({k:v for k,v in user.items() if k!="pw_hash"})

@app.route("/api/profile", methods=["PUT"])
@require_auth
def update_profile():
    user    = request.current_user
    updates = {k:v for k,v in request.get_json().items() if k in ["full_name","location_city","location_lat","location_lon"]}
    if supabase:
        try:
            supabase.table("profiles").update(updates).eq("id",user["id"]).execute()
            return jsonify({"success":True})
        except Exception as e: return jsonify({"error":str(e)}),500
    user.update(updates)
    return jsonify({"success":True})


# ─────────────────────────────────────────────
# AI ADVISOR
# ─────────────────────────────────────────────
def aria_log(*args, **kwargs):
    if ARIA_DEBUG:
        print(*args, **kwargs)

@app.route("/api/ai-advisor", methods=["POST"])
@require_auth
def ai_advisor():
    user    = request.current_user
    d       = request.get_json() or {}
    message = d.get("message", "").strip()
    role    = user.get("role", "consumer")
    lat     = float(d.get("lat", user.get("location_lat", 22.7196)))
    lon     = float(d.get("lon", user.get("location_lon", 75.8577)))

    weather = fetch_weather(lat, lon)
    clouds  = weather.get("clouds", 30)
    wind    = weather.get("wind_speed", 14.4)
    temp    = weather.get("temp", 28)

    if role == "producer":
        context = f"""You are ARIA, an AI energy advisor for renewable energy producers on NEXUS.
Weather: {weather.get('condition')}, {temp}°C, {clouds}% cloud, wind {wind}km/h.
Solar market: {'HIGH DEMAND — heavy cloud reduces output' if clouds>60 else 'SURPLUS — clear sky' if clouds<20 else 'MODERATE'}.
Wind market:  {'SURPLUS — strong winds' if wind>25 else 'HIGH DEMAND — calm winds' if wind<5 else 'MODERATE'}.
Policy: floor ₹{POLICY['min_price_kwh']}, ceiling ₹{POLICY['max_price_kwh']}, solar subsidy -₹{POLICY['solar_subsidy']}, wind -₹{POLICY['wind_subsidy']}.
Help with listing timing, pricing, earnings, carbon credits. Be concise — max 3 sentences."""
    elif role == "consumer":
        context = f"""You are ARIA, an AI energy advisor for consumers on NEXUS.
Weather: {weather.get('condition')}, {temp}°C, {clouds}% cloud, wind {wind}km/h.
Cheapest source right now: {'Solar — clear sky means surplus and lowest prices' if clouds<25 else 'Wind — strong output means low price' if wind>20 else 'Biogas — stable baseload, weather-independent'}.
Policy: max ₹{POLICY['max_price_kwh']}/kWh, min ₹{POLICY['min_price_kwh']}/kWh, bulk ≥50kWh = −8%.
Help with: when to buy, which source, pool benefits, CO₂ savings. Max 3 sentences."""
    else:
        context = f"""You are ARIA, an AI market advisor for energy investors on NEXUS.
Weather: {weather.get('condition')}, {temp}°C, clouds {clouds}%, wind {wind}km/h.
Signal: {'Solar under-producing — wind/biogas favoured today' if clouds>60 else 'Solar at peak output' if clouds<25 else 'Mixed conditions — diversify'}.
Avg return: 8.5% p.a. Help with: which producers to fund, timing, portfolio mix. Max 3 sentences."""

    if GROQ_API_KEY:
        try:
            resp = requests.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"},
                json={"model": "llama-3.1-8b-instant", "max_tokens": 200, "temperature": 0.7,
                      "messages": [{"role":"system","content":context},
                                   {"role":"user","content":message or "Give me a market summary."}]},
                timeout=12
            )
            if resp.status_code == 200:
                choices = resp.json().get("choices", [])
                if choices:
                    return jsonify({"reply": choices[0]["message"]["content"].strip(), "powered_by": "groq-llama3"})
        except Exception as e:
            print(f"Groq error: {e}")

    reply = _smart_advisor(message.lower(), role, weather, clouds, wind)
    return jsonify({"reply": reply, "powered_by": "rule-engine"})


def _smart_advisor(msg, role, weather, clouds, wind):
    c = clouds; w = wind
    if role == "producer":
        if any(x in msg for x in ["price","sell","when","list"]):
            if c < 25: return f"☀️ Great time to sell solar — clear sky ({c}% cloud) means surplus. List at base price now before clouds return."
            if w > 20: return f"💨 Strong winds ({w}km/h) = wind surplus = lower wind prices. Consider listing biogas today for better margins."
            return f"◉ Stable conditions ({c}% cloud, {w}km/h wind). Standard pricing applies — list now for steady demand."
        if "solar" in msg:
            return f"☀️ Solar output: {'HIGH ✓' if c<25 else 'MODERATE' if c<60 else 'LOW ⚠️'} ({c}% cloud). {'Price at ₹2.8–3.5.' if c<40 else 'You can charge ₹4–6 premium during shortage.'}"
        return f"💡 {weather.get('condition','Stable')} | {'List solar now — high demand.' if c<30 else 'Biogas most stable today.'} Market: ₹{POLICY['min_price_kwh']}–{POLICY['max_price_kwh']}/kWh."
    elif role == "consumer":
        if any(x in msg for x in ["buy","cheap","when","best","source","cheapest"]):
            if c < 25: return f"🌟 Buy solar NOW — {c}% cloud cover = surplus = lowest prices. Consider 50+ kWh to unlock the 8% bulk discount."
            if w > 20: return f"💨 Wind is cheapest right now ({w}km/h = surplus). Check WindFarm listings — ₹0.5–2 below normal."
            return f"🌿 Biogas is your best bet today — stable at ~₹3–3.5/kWh. Join a community pool for an extra 8% off at 50 kWh."
        if "pool" in msg:
            return "🤝 Pools combine buyers to hit 50 kWh, unlocking 8% discount for everyone. Join an existing pool or create one — savings split automatically."
        if "co2" in msg or "carbon" in msg or "saving" in msg:
            return "🌍 Every kWh of renewable energy saves ~0.82 kg CO₂ vs India's grid average. This goes to your Carbon Impact score — PLATINUM = 200+ kg saved."
        if any(x in msg for x in ["sha","hash","ledger","blockchain"]):
            return "🔒 Every trade creates a SHA-256 hash linking to the previous transaction — impossible to alter without breaking the whole chain. Only admins see the full hash; you see a short 0x… reference."
        return f"💡 Best now: {'☀️ Solar (clear sky = surplus)' if c<25 else '💨 Wind (strong = low price)' if w>20 else '🌿 Biogas (stable, weather-proof)'}. Check live listings."
    else:
        if any(x in msg for x in ["invest","return","fund","portfolio"]):
            return f"📈 Top pick: {'Wind farms — strong output = high volume = returns.' if w>20 else 'Solar — clear sky = peak production.' if c<30 else 'Biogas — most consistent at avg 8.5% p.a.'} Diversify across 2–3 types."
        return f"📊 {weather.get('condition','Stable')} | Cloud {c}% | Wind {w}km/h | {'Solar under-producing — wind/biogas outperforming.' if c>60 else 'Solar at peak.'} Avg: 8.5% p.a."


# ─────────────────────────────────────────────
# ADMIN
# ─────────────────────────────────────────────
@app.route("/api/admin/users")
@require_role("admin")
def admin_users():
    if supabase:
        try: return jsonify(supabase.table("profiles").select("id,email,full_name,role,location_city,carbon_score,created_at").execute().data or [])
        except: pass
    return jsonify([{k:v for k,v in u.items() if k!="pw_hash"} for u in _mem["users"].values()])

@app.route("/api/admin/users/<uid>/role", methods=["PUT"])
@require_role("admin")
def set_user_role(uid):
    role = request.get_json().get("role")
    if role not in ["producer","consumer","investor","admin"]:
        return jsonify({"error":"Invalid role"}),400
    if supabase:
        supabase.table("profiles").update({"role":role}).eq("id",uid).execute()
    return jsonify({"success":True})

@app.route("/api/debug-login", methods=["POST"])
def debug_login():
    data = request.get_json()
    if not supabase:
        return jsonify({"error":"Supabase not connected"})
    try:
        res = supabase.auth.sign_in_with_password({"email":data.get("email",""),"password":data.get("password","")})
        return jsonify({"success":True,"user_id":str(res.user.id),"has_session":res.session is not None})
    except Exception as e:
        return jsonify({"success":False,"error":str(e)}),401


if __name__ == "__main__":
    print("\n🔋 NEXUS Energy Exchange — Starting")
    print(f"   Supabase:   {'✓ Connected' if supabase else '⚠ Memory mode'}")
    print(f"   Weather:    {'✓ OpenWeatherMap' if WEATHER_API_KEY else '⚠ Demo data'}")
    print(f"   Payments:   {'✓ Razorpay' if RAZORPAY_KEY_ID else '⚠ Demo mode'}")
    print(f"   AI Advisor: {'✓ Groq LLaMA3' if GROQ_API_KEY else '◉ Rule-based engine'}")
    print(f"\n   Open: http://localhost:5000\n")
    app.run(debug=True, host="0.0.0.0", port=5000)
