import requests
import time
from datetime import datetime
import logging
import json
import re
import hashlib
import phonenumbers
from phonenumbers import geocoder
from collections import deque
import signal
import sys
import os
import threading
import uuid

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s [%(name)s]: %(message)s',
    datefmt='%H:%M:%S'
)

BOT_DIR = os.path.dirname(__file__)
CONFIG_PATH = os.path.join(BOT_DIR, 'config.json')
ACTIVITY_LOG_PATH = os.path.join(BOT_DIR, 'activity_log.json')
BOT_STATUS_PATH = os.path.join(BOT_DIR, 'bot_status.json')

_config_lock = threading.Lock()
_activity_lock = threading.Lock()
_status_lock = threading.Lock()

sent_message_hashes = deque(maxlen=500)
sent_otp_numbers = deque(maxlen=500)
sent_otp_codes = deque(maxlen=500)
_deque_lock = threading.Lock()

_running = True
_api_threads = {}
_api_status = {}

# ── Auto-relogin state ────────────────────────────────────────────────────────
_relogin_lock = threading.Lock()
_relogin_in_progress = set()   # api_ids currently being re-logged in
_last_relogin = {}             # api_id → unix timestamp of last successful relogin
_RELOGIN_COOLDOWN = 600        # seconds between automatic relogins (10 min)


def _notify_admins(config, text):
    """Send a plain-text notification to all admin Telegram IDs."""
    token = (config.get('telegram') or {}).get('bot_token', '') or config.get('bot_token', '')
    admins = (config.get('settings') or {}).get('admin_ids', []) or config.get('admin_ids', [])
    if not token or not admins:
        return
    for admin_id in admins:
        try:
            requests.post(
                f'https://api.telegram.org/bot{token}/sendMessage',
                json={'chat_id': admin_id, 'text': text, 'parse_mode': 'HTML'},
                timeout=10,
            )
        except Exception:
            pass


def _trigger_auto_relogin(api_id, api_cfg, logger):
    """
    Spawn a background thread to re-login and refresh credentials for api_id.
    Returns True if a relogin thread was started, False otherwise.
    """
    username = api_cfg.get('username', '')
    password = api_cfg.get('password', '')
    base_url  = api_cfg.get('base_url', '')

    if not (username and password and base_url):
        return False   # no credentials stored yet

    with _relogin_lock:
        if api_id in _relogin_in_progress:
            return False   # already running
        now = time.time()
        if now - _last_relogin.get(api_id, 0) < _RELOGIN_COOLDOWN:
            remaining = int(_RELOGIN_COOLDOWN - (now - _last_relogin.get(api_id, 0)))
            logger.info(f'Auto-relogin cooldown: {remaining}s remaining for {api_id}')
            return False
        _relogin_in_progress.add(api_id)

    def _do_relogin():
        log = logging.getLogger(f'Relogin:{api_id}')
        log.info(f'Auto-relogin started for {api_id} ({api_cfg.get("name", api_id)})')
        try:
            sys.path.insert(0, BOT_DIR)
            from panel_login import auto_login_panel
            result = auto_login_panel(
                base_url=base_url,
                username=username,
                password=password,
                progress_cb=lambda m: log.info(f'  {m}'),
            )
            cfg = load_config()
            api = next((a for a in cfg.get('apis', []) if a['id'] == api_id), None)
            if api:
                api.setdefault('cookies', {})['PHPSESSID'] = result['phpsessid']
                if result.get('sesskey'):
                    api['sesskey'] = result['sesskey']
                save_config(cfg)
                with _relogin_lock:
                    _last_relogin[api_id] = time.time()
                log.info(f'Auto-relogin SUCCESS for {api_id}')
                _notify_admins(cfg, (
                    f'✅ <b>Auto-Login Successful</b>\n'
                    f'API: <b>{api["name"]}</b>\n'
                    f'Session refreshed automatically — polling resumed.'
                ))
            else:
                log.warning(f'Auto-relogin: API {api_id} disappeared from config')
        except Exception as e:
            log.error(f'Auto-relogin FAILED for {api_id}: {e}')
            try:
                cfg = load_config()
                _notify_admins(cfg, (
                    f'❌ <b>Auto-Login Failed</b>\n'
                    f'API ID: <code>{api_id}</code>\n'
                    f'Error: {str(e)[:200]}\n\n'
                    f'Use the admin panel → Edit API → Re-Login (Auto) to retry manually.'
                ))
            except Exception:
                pass
        finally:
            with _relogin_lock:
                _relogin_in_progress.discard(api_id)

    threading.Thread(target=_do_relogin, daemon=True, name=f'relogin-{api_id}').start()
    logger.info(f'Auto-relogin thread launched for {api_id}')
    return True

COUNTRY_EMOJIS = {
    'AC': '🇦🇨', 'AD': '🇦🇩', 'AE': '🇦🇪', 'AF': '🇦🇫', 'AG': '🇦🇬', 'AI': '🇦🇮',
    'AL': '🇦🇱', 'AM': '🇦🇲', 'AO': '🇦🇴', 'AQ': '🇦🇶', 'AR': '🇦🇷', 'AS': '🇦🇸',
    'AT': '🇦🇹', 'AU': '🇦🇺', 'AW': '🇦🇼', 'AX': '🇦🇽', 'AZ': '🇦🇿', 'BA': '🇧🇦',
    'BB': '🇧🇧', 'BD': '🇧🇩', 'BE': '🇧🇪', 'BF': '🇧🇫', 'BG': '🇧🇬', 'BH': '🇧🇭',
    'BI': '🇧🇮', 'BJ': '🇧🇯', 'BL': '🇧🇱', 'BM': '🇧🇲', 'BN': '🇧🇳', 'BO': '🇧🇴',
    'BQ': '🇧🇶', 'BR': '🇧🇷', 'BS': '🇧🇸', 'BT': '🇧🇹', 'BV': '🇧🇻', 'BW': '🇧🇼',
    'BY': '🇧🇾', 'BZ': '🇧🇿', 'CA': '🇨🇦', 'CC': '🇨🇨', 'CD': '🇨🇩', 'CF': '🇨🇫',
    'CG': '🇨🇬', 'CH': '🇨🇭', 'CI': '🇨🇮', 'CK': '🇨🇰', 'CL': '🇨🇱', 'CM': '🇨🇲',
    'CN': '🇨🇳', 'CO': '🇨🇴', 'CP': '🇨🇵', 'CR': '🇨🇷', 'CU': '🇨🇺', 'CV': '🇨🇻',
    'CW': '🇨🇼', 'CX': '🇨🇽', 'CY': '🇨🇾', 'CZ': '🇨🇿', 'DE': '🇩🇪', 'DG': '🇩🇬',
    'DJ': '🇩🇯', 'DK': '🇩🇰', 'DM': '🇩🇲', 'DO': '🇩🇴', 'DZ': '🇩🇿', 'EA': '🇪🇦',
    'EC': '🇪🇨', 'EE': '🇪🇪', 'EG': '🇪🇬', 'EH': '🇪🇭', 'ER': '🇪🇷', 'ES': '🇪🇸',
    'ET': '🇪🇹', 'EU': '🇪🇺', 'FI': '🇫🇮', 'FJ': '🇫🇯', 'FK': '🇫🇰', 'FM': '🇫🇲',
    'FO': '🇫🇴', 'FR': '🇫🇷', 'GA': '🇬🇦', 'GB': '🇬🇧', 'GD': '🇬🇩', 'GE': '🇬🇪',
    'GF': '🇬🇫', 'GG': '🇬🇬', 'GH': '🇬🇭', 'GI': '🇬🇮', 'GL': '🇬🇱', 'GM': '🇬🇲',
    'GN': '🇬🇳', 'GP': '🇬🇵', 'GQ': '🇬🇶', 'GR': '🇬🇷', 'GS': '🇬🇸', 'GT': '🇬🇹',
    'GU': '🇬🇺', 'GW': '🇬🇼', 'GY': '🇬🇾', 'HK': '🇭🇰', 'HM': '🇭🇲', 'HN': '🇭🇳',
    'HR': '🇭🇷', 'HT': '🇭🇹', 'HU': '🇭🇺', 'IC': '🇮🇨', 'ID': '🇮🇩', 'IE': '🇮🇪',
    'IL': '🇮🇱', 'IM': '🇮🇲', 'IN': '🇮🇳', 'IO': '🇮🇴', 'IQ': '🇮🇶', 'IR': '🇮🇷',
    'IS': '🇮🇸', 'IT': '🇮🇹', 'JE': '🇯🇪', 'JM': '🇯🇲', 'JO': '🇯🇴', 'JP': '🇯🇵',
    'KE': '🇰🇪', 'KG': '🇰🇬', 'KH': '🇰🇭', 'KI': '🇰🇮', 'KM': '🇰🇲', 'KN': '🇰🇳',
    'KP': '🇰🇵', 'KR': '🇰🇷', 'KW': '🇰🇼', 'KY': '🇰🇾', 'KZ': '🇰🇿', 'LA': '🇱🇦',
    'LB': '🇱🇧', 'LC': '🇱🇨', 'LI': '🇱🇮', 'LK': '🇱🇰', 'LR': '🇱🇷', 'LS': '🇱🇸',
    'LT': '🇱🇹', 'LU': '🇱🇺', 'LV': '🇱🇻', 'LY': '🇱🇾', 'MA': '🇲🇦', 'MC': '🇲🇨',
    'MD': '🇲🇩', 'ME': '🇲🇪', 'MF': '🇲🇫', 'MG': '🇲🇬', 'MH': '🇲🇭', 'MK': '🇲🇰',
    'ML': '🇲🇱', 'MM': '🇲🇲', 'MN': '🇲🇳', 'MO': '🇲🇴', 'MP': '🇲🇵', 'MQ': '🇲🇶',
    'MR': '🇲🇷', 'MS': '🇲🇸', 'MT': '🇲🇹', 'MU': '🇲🇺', 'MV': '🇲🇻', 'MW': '🇲🇼',
    'MX': '🇲🇽', 'MY': '🇲🇾', 'MZ': '🇲🇿', 'NA': '🇳🇦', 'NC': '🇳🇨', 'NE': '🇳🇪',
    'NF': '🇳🇫', 'NG': '🇳🇬', 'NI': '🇳🇮', 'NL': '🇳🇱', 'NO': '🇳🇴', 'NP': '🇳🇵',
    'NR': '🇳🇷', 'NU': '🇳🇺', 'NZ': '🇳🇿', 'OM': '🇴🇲', 'PA': '🇵🇦', 'PE': '🇵🇪',
    'PF': '🇵🇫', 'PG': '🇵🇬', 'PH': '🇵🇭', 'PK': '🇵🇰', 'PL': '🇵🇱', 'PM': '🇵🇲',
    'PN': '🇵🇳', 'PR': '🇵🇷', 'PS': '🇵🇸', 'PT': '🇵🇹', 'PW': '🇵🇼', 'PY': '🇵🇾',
    'QA': '🇶🇦', 'RE': '🇷🇪', 'RO': '🇷🇴', 'RS': '🇷🇸', 'RU': '🇷🇺', 'RW': '🇷🇼',
    'SA': '🇸🇦', 'SB': '🇸🇧', 'SC': '🇸🇨', 'SD': '🇸🇩', 'SE': '🇸🇪', 'SG': '🇸🇬',
    'SH': '🇸🇭', 'SI': '🇸🇮', 'SJ': '🇸🇯', 'SK': '🇸🇰', 'SL': '🇸🇱', 'SM': '🇸🇲',
    'SN': '🇸🇳', 'SO': '🇸🇴', 'SR': '🇸🇷', 'SS': '🇸🇸', 'ST': '🇸🇹', 'SV': '🇸🇻',
    'SX': '🇸🇽', 'SY': '🇸🇾', 'SZ': '🇸🇿', 'TA': '🇹🇦', 'TC': '🇹🇨', 'TD': '🇹🇩',
    'TF': '🇹🇫', 'TG': '🇹🇬', 'TH': '🇹🇭', 'TJ': '🇹🇯', 'TK': '🇹🇰', 'TL': '🇹🇱',
    'TM': '🇹🇲', 'TN': '🇹🇳', 'TO': '🇹🇴', 'TR': '🇹🇷', 'TT': '🇹🇹', 'TV': '🇹🇻',
    'TW': '🇹🇼', 'TZ': '🇹🇿', 'UA': '🇺🇦', 'UG': '🇺🇬', 'UM': '🇺🇲', 'US': '🇺🇸',
    'UY': '🇺🇾', 'UZ': '🇺🇿', 'VA': '🇻🇦', 'VC': '🇻🇨', 'VE': '🇻🇪', 'VG': '🇻🇬',
    'VI': '🇻🇮', 'VN': '🇻🇳', 'VU': '🇻🇺', 'WF': '🇼🇫', 'WS': '🇼🇸', 'XK': '🇽🇰',
    'YE': '🇾🇪', 'YT': '🇾🇹', 'ZA': '🇿🇦', 'ZM': '🇿🇲', 'ZW': '🇿🇼'
}

# Custom emoji IDs for country flags — FlagsByKoylli pack (all 198 stickers verified)
# tge() is called at runtime so this must stay below tge() definition;
# we store raw IDs here and call tge() in format_message.
COUNTRY_FLAG_CE = {
    'AD': '5221987861733061751', 'AE': '5224565851427976312', 'AF': '5222096009009575868',
    'AG': '5224544866217765554', 'AL': '5224312057515486246', 'AM': '5224369957969603463',
    'AO': '5224379767674907895', 'AR': '5221980461504411710', 'AT': '5224520754271366661',
    'AU': '5224659803837574114', 'AZ': '5224426544163728284', 'BA': '5224496092569155254',
    'BB': '5222156533688712094', 'BD': '5224407289825340729', 'BE': '5224513182244024630',
    'BF': '5222356541725749790', 'BG': '5222092074819530668', 'BH': '5224492892818518587',
    'BI': '5224490444687158452', 'BJ': '5222024115552009151', 'BM': '5222482143749353810',
    'BN': '5224435958732042406', 'BO': '5224675484763170798', 'BR': '5224688610183228070',
    'BS': '5224504167107668172', 'BT': '5224541065171710147', 'BW': '5224288456670196085',
    'BY': '5280820319458707404', 'BZ': '5224316292353241916', 'CA': '5222001124592071204',
    'CD': '5224398158724871677', 'CF': '5222073662294733523', 'CG': '5222104268231684600',
    'CH': '5224707263226194753', 'CI': '5222104268231684600', 'CL': '5222350726340032308',
    'CM': '5222270788408717651', 'CN': '5224435456220868088', 'CO': '5224455152940886669',
    'CR': '5222453801260168022', 'CV': '5222347737042792258', 'CY': '5222431454545327055',
    'CZ': '5222073533445714675', 'DE': '5222165617544542414', 'DJ': '5224203012590810589',
    'DK': '5222297215342490217', 'DM': '5222337489250824921', 'DO': '5224286412265763450',
    'DZ': '5224260376174015500', 'EC': '5224191188545840926', 'EE': '5222195463272281351',
    'EG': '5222161185138292290', 'ER': '5222161185138292290', 'ES': '5222024776976970940',
    'ET': '5224467805914542024', 'FI': '5224282903277482188', 'FJ': '5221962676044838178',
    'FM': '5222280486444873367', 'FO': '5280985770188885026', 'FR': '5222029789203804982',
    'GA': '5224669733801963467', 'GB': '5224518800061245598', 'GD': '5222234560359577687',
    'GE': '5222152195771742239', 'GH': '5224511339703056124', 'GM': '5221949872747330159',
    'GN': '5222337588035073000', 'GQ': '5222172811614762423', 'GR': '5222463490706389920',
    'GT': '5222128302868672826', 'GW': '5224705704153066489', 'GY': '5224570532942329532',
    'HN': '5222229234600130045', 'HR': '5221967765581085099', 'HT': '5224683146984831315',
    'HU': '5224691998912427164', 'ID': '5224405893960969756', 'IE': '5224257017509588818',
    'IL': '5224720599099648709', 'IN': '5222300011366200403', 'IQ': '5221980268230882832',
    'IR': '5224374154152653367', 'IS': '5222063229819172521', 'IT': '5222460101977190141',
    'JM': '5222007034467074185', 'JO': '5222292177345853436', 'JP': '5222390089715299207',
    'KE': '5222089648163009103', 'KG': '5224388147156102493', 'KH': '5224189882875785448',
    'KI': '5224652244695134610', 'KM': '5222398735484466247', 'KR': '5222345550904439270',
    'KW': '5221949726718442491', 'KZ': '5222276376161171525', 'LA': '5224200843632324642',
    'LB': '5222244425899455269', 'LC': '5222000927023577045', 'LK': '5224277294050192388',
    'LR': '5221998371518034740', 'LS': '5224245850594619415', 'LT': '5224245902134226386',
    'LU': '5224499567197700690', 'LV': '5224401229626484931', 'LY': '5222194286451242896',
    'MA': '5224530035695693965', 'MC': '5221937224068640464', 'MD': '5224216473018314447',
    'ME': '5224463399278096980', 'MG': '5222042605386217334', 'MH': '5224538449536624503',
    'MK': '5222470435668505656', 'ML': '5224322352552096671', 'MM': '5222042605386217334',
    'MN': '5224192257992701543', 'MQ': '5281027792148909351', 'MT': '5224731388057497620',
    'MU': '5224238347286752315', 'MV': '5224393700548814960', 'MX': '5221971386238514431',
    'MY': '5224312886444174057', 'MZ': '5222470388423864826', 'NA': '5224690826386351746',
    'NE': '5222099049846420864', 'NG': '5224723614166691638', 'NL': '5224516489368841614',
    'NO': '5224465228934163949', 'NP': '5222444378101925267', 'NZ': '5224573595254009705',
    'OM': '5222396686785066306', 'PA': '5222111719999945107', 'PE': '5224482026551258766',
    'PG': '5224500164198149905', 'PH': '5222065042295376892', 'PK': '5224637061985742245',
    'PL': '5224670399521892983', 'PS': '5222041677673282461', 'PT': '5224404094369672274',
    'PY': '5222152565138929235', 'QA': '5222225596762830469', 'RO': '5222273794885826118',
    'RS': '5222145396838512729', 'RU': '5280582975270963511', 'RW': '5222449197055227754',
    'SA': '5224698145010624573', 'SB': '5222290588207954120', 'SC': '5224467496676896871',
    'SD': '5224372990216514135', 'SE': '5222201098269373561', 'SG': '5224194023224257181',
    'SI': '5224660718665607511', 'SK': '5222401879400528047', 'SL': '5224420995065983217',
    'SN': '5224358988623130949', 'SO': '5222370504664428325', 'SR': '5224567367551428669',
    'SS': '5224618146949773268', 'ST': '5221953304426198315', 'SV': '5224337131534559907',
    'SZ': '5224269666188274723', 'TD': '5222060468155204001', 'TG': '5222408051268532030',
    'TH': '5224638530864556281', 'TJ': '5222217865821696536', 'TL': '5224515905253291409',
    'TM': '5224256935905208951', 'TN': '5221991375016310330', 'TR': '5224601903383457698',
    'TT': '5224391883777651050', 'TZ': '5224397364155923150', 'UA': '5222250679371839695',
    'UG': '5222464040462200940', 'US': '5224321781321442532', 'UY': '5222466849370813232',
    'UZ': '5222404546575219535', 'VA': '5222420266155520507', 'VC': '5224541228380467535',
    'VN': '5222359651282071925', 'VU': '5222126748090512778', 'WS': '5224660353593387686',
    'XK': '5222197129719592160', 'YE': '5222300655611294950', 'ZA': '5224696216570309138',
    'ZM': '5224646626877911277', 'ZW': '5222060442385397848',
}


def load_config():
    with _config_lock:
        with open(CONFIG_PATH) as f:
            return json.load(f)


def save_config(config):
    with _config_lock:
        with open(CONFIG_PATH, 'w') as f:
            json.dump(config, f, indent=2)


def log_activity(entry):
    with _activity_lock:
        try:
            if os.path.exists(ACTIVITY_LOG_PATH):
                with open(ACTIVITY_LOG_PATH) as f:
                    logs = json.load(f)
            else:
                logs = []
        except Exception:
            logs = []
        logs.insert(0, entry)
        logs = logs[:500]
        with open(ACTIVITY_LOG_PATH, 'w') as f:
            json.dump(logs, f, indent=2)


def update_api_status(api_id, status_data):
    with _status_lock:
        _api_status[api_id] = status_data
        try:
            with open(BOT_STATUS_PATH, 'w') as f:
                json.dump(_api_status, f, indent=2)
        except Exception:
            pass


def escape_markdown(text):
    escape_chars = r'_*[]()~`>#+-=|{}.!'
    return re.sub(f'([{re.escape(escape_chars)}])', r'\\\1', str(text))


def html_escape(text):
    return str(text).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')


def tge(emoji_id, fallback='⭐'):
    return f'<tg-emoji emoji-id="{emoji_id}">{fallback}</tg-emoji>'


CE = {
    # ── UI / status (all verified from IconsEmoji_JABA pack) ──────────────────
    'green':     '5332440771180116150',   # 🟢  index 189
    'check':     '5273806972871787310',   # ✅  index 145
    'cross':     '5271934564699226262',   # ❌  index 146
    'alert':     '5287388737498529298',   # 🚨  index 164
    'fire':      '5332336747072208845',   # 🔥  index 167
    'shield':    '5363972600001216334',   # 🛡  index 161
    'star':      '5233537411044107383',   # ⭐  index 3
    'diamond':   '5199448307155350272',   # 💎  index 130
    'red_dot':   '5332667755906743671',   # 🔴  index 186
    'blue_dot':  '5332571076192910271',   # 🔵  index 190
    'yellow':    '5332345843812943191',   # 🟡  index 188
    'settings':  '5366231924597604153',   # ⚙️  index 160
    'robot':     '5310170944843579391',   # 🤖  index 7
    'megaphone': '5332757031096958807',   # 🔊  index 148
    'link':      '5332755643822520488',   # 🔗  index 158
    'info':      '5332679880599418983',   # ℹ️  index 155
    'lock':      '5363972600001216334',   # 🛡  index 161
    # ── app / service icons ───────────────────────────────────────────────────
    'whatsapp':  '5233354831984353090',   # 📞  index 24  — WA green-phone icon
    'telegram':  '5364125616801073577',   # ✈️  index 23  — paper plane
    'facebook':  '5233376087777501917',   # 💬  index 21  — blue chat bubble (Messenger)
    'instagram': '5364310996179503764',   # 📸  index 18  — camera
    'twitter':   '5233634911096693865',   # 🐦  index 20  — bird / X
    'tiktok':    '5391044040860906456',   # 🎵  index 19  — music note
    'snapchat':  '5233537411044107383',   # ⭐  index 3   — closest to ghost/star
    'viber':     '5332531536723984111',   # 📞  index 159 — phone
    'signal':    '5363972600001216334',   # 🛡  index 161 — shield lock
    'discord':   '5233333563306301418',   # 🎮  index 27  — game controller
    'reddit':    '5233582387941630314',   # 👾  index 26  — alien (Reddit mascot)
    'linkedin':  '5319084384962248505',   # 💻  index 32  — laptop / professional
    'google':    '5321244246705989720',   # 🔍  index 55  — magnifier
    'gmail':     '5366201992970518798',   # 📧  index 173 — email envelope
    'outlook':   '5332369758190845562',   # 📩  index 172 — inbox
    'yahoo':     '5366201992970518798',   # 📧  index 173 — email
    'apple':     '5318795767454923927',   # 🍎  index 56  — apple logo
    'microsoft': '5319084384962248505',   # 💻  index 32  — computer
    'amazon':    '5348149223223211884',   # 📦  index 72  — box / package
    'netflix':   '5332722143077613679',   # ▶️  index 182 — play button
    'spotify':   '5233578612665375810',   # 🎵  index 67  — music note (Spotify green)
    'youtube':   '5366477429223209600',   # 📺  index 66  — TV screen
    'paypal':    '5388622778817589921',   # 💰  index 74  — money bag
    'binance':   '5332345843812943191',   # 🟡  index 188 — yellow (Binance brand)
    'coinbase':  '5332571076192910271',   # 🔵  index 190 — blue dot
    'bitcoin':   '5336953394533780524',   # 🪙  index 99  — coin
    'uber':      '5332618260703624145',   # 🌐  index 157 — globe / map
    'lyft':      '5332618260703624145',   # 🌐  index 157 — globe
    'line':      '5233449944035123527',   # 💬  index 22  — chat bubble (Line green)
    'wechat':    '5440411975509096877',   # 💬  index 156 — chat bubble (WeChat)
    # ── generic helpers ───────────────────────────────────────────────────────
    'phone':     '5319228768877839193',   # 📱  index 57  — generic phone
    'chat':      '5233376087777501917',   # 💬  index 21  — generic chat
    'globe':     '5332618260703624145',   # 🌐  index 157 — generic globe
    'box':       '5348149223223211884',   # 📦  index 72
    'money':     '5388622778817589921',   # 💰  index 74
    'computer':  '5319084384962248505',   # 💻  index 32
    'camera':    '5364310996179503764',   # 📸  index 18
    'search':    '5321244246705989720',   # 🔍  index 55
    'email':     '5366201992970518798',   # 📧  index 173
    'music':     '5391044040860906456',   # 🎵  index 19
    'tv':        '5366477429223209600',   # 📺  index 66
    'coin':      '5336953394533780524',   # 🪙  index 99
    'exchange':  '5364066964727678118',   # 💱  index 84
}


# ── keywords to match inside message body ──────────────────────────────────────
SERVICE_KEYWORDS = {
    'whatsapp':  ['whatsapp', 'whats app'],
    'telegram':  ['telegram', 't.me/'],
    'facebook':  ['facebook', 'fb.com', 'meta'],
    'instagram': ['instagram', 'insta'],
    'twitter':   ['twitter', 'x.com', 'tweet'],
    'tiktok':    ['tiktok', 'tik tok'],
    'snapchat':  ['snapchat', 'snap'],
    'viber':     ['viber'],
    'signal':    ['signal app', 'signal.org'],
    'discord':   ['discord'],
    'linkedin':  ['linkedin'],
    'google':    ['google account', 'google'],
    'gmail':     ['gmail'],
    'outlook':   ['outlook', 'microsoft account', 'hotmail'],
    'yahoo':     ['yahoo'],
    'apple':     ['apple id', 'icloud', 'apple account'],
    'microsoft': ['microsoft'],
    'amazon':    ['amazon'],
    'netflix':   ['netflix'],
    'spotify':   ['spotify'],
    'youtube':   ['youtube'],
    'paypal':    ['paypal'],
    'binance':   ['binance'],
    'coinbase':  ['coinbase'],
    'bitcoin':   ['bitcoin', 'crypto'],
    'uber':      ['uber'],
    'lyft':      ['lyft'],
    'line':      ['line app'],
    'wechat':    ['wechat', 'weixin'],
    'reddit':    ['reddit'],
}


def detect_service(service_field, raw_text=''):
    """
    Return a normalised service key.
    Priority: panel field → scan message body → raw field value → 'unknown'.
    """
    def _match_keywords(text):
        t = text.lower()
        for key, kws in SERVICE_KEYWORDS.items():
            if any(kw in t for kw in kws):
                return key
        return None

    # 1. panel field (may already contain the service name)
    if service_field and service_field.strip().lower() not in ('', 'unknown', 'sms', 'n/a', '-'):
        matched = _match_keywords(service_field)
        if matched:
            return matched

    # 2. scan the raw SMS body
    if raw_text:
        matched = _match_keywords(raw_text)
        if matched:
            return matched

    # 3. return the panel value cleaned up, or 'unknown'
    return (service_field or 'unknown').strip()


SERVICE_CUSTOM_EMOJIS = {
    'whatsapp':  tge(CE['whatsapp'],  '📞'),
    'telegram':  tge(CE['telegram'],  '✈️'),
    'facebook':  tge(CE['facebook'],  '💬'),
    'instagram': tge(CE['instagram'], '📸'),
    'twitter':   tge(CE['twitter'],   '🐦'),
    'tiktok':    tge(CE['tiktok'],    '🎵'),
    'snapchat':  tge(CE['snapchat'],  '⭐'),
    'viber':     tge(CE['viber'],     '📞'),
    'signal':    tge(CE['signal'],    '🛡'),
    'discord':   tge(CE['discord'],   '🎮'),
    'reddit':    tge(CE['reddit'],    '👾'),
    'linkedin':  tge(CE['linkedin'],  '💻'),
    'google':    tge(CE['google'],    '🔍'),
    'gmail':     tge(CE['gmail'],     '📧'),
    'outlook':   tge(CE['outlook'],   '📩'),
    'yahoo':     tge(CE['yahoo'],     '📧'),
    'apple':     tge(CE['apple'],     '🍎'),
    'microsoft': tge(CE['microsoft'], '💻'),
    'amazon':    tge(CE['amazon'],    '📦'),
    'netflix':   tge(CE['netflix'],   '▶️'),
    'spotify':   tge(CE['spotify'],   '🎵'),
    'youtube':   tge(CE['youtube'],   '📺'),
    'paypal':    tge(CE['paypal'],    '💰'),
    'binance':   tge(CE['binance'],   '🟡'),
    'coinbase':  tge(CE['coinbase'],  '🔵'),
    'bitcoin':   tge(CE['bitcoin'],   '🪙'),
    'uber':      tge(CE['uber'],      '🌐'),
    'lyft':      tge(CE['lyft'],      '🌐'),
    'line':      tge(CE['line'],      '💬'),
    'wechat':    tge(CE['wechat'],    '💬'),
}


def extract_otp_code(raw_text):
    if not raw_text:
        return None
    text = str(raw_text).strip()

    malformed_patterns = [
        r'RESP=\d+-(\d+)-(\d+)',
        r'/(\d{4,8})[/-]',
        r'-(\d{4,8})-',
    ]
    for pattern in malformed_patterns:
        match = re.search(pattern, text)
        if match:
            for group in match.groups():
                if group and len(group) >= 4:
                    return group

    standard_patterns = [
        r'(?:^|\s)(\d{6})(?:\s|$)',
        r'(?:^|\s)(\d{4,8})(?:\s|$)',
        r'code[:\s]*(\d{4,8})',
        r'otp[:\s]*(\d{4,8})',
        r'verification[:\s]*(\d{4,8})',
        r'password[:\s]*(\d{4,8})',
        r'\b(\d{3}[- ]?\d{3})\b',
        r'\b(\d{4}[- ]?\d{4})\b',
        r'(?<!\d)(\d{4,8})(?!\d)'
    ]
    for pattern in standard_patterns:
        for match in re.finditer(pattern, text, re.IGNORECASE):
            code = match.group(1)
            if code:
                clean_code = re.sub(r'[^\d]', '', code)
                if 4 <= len(clean_code) <= 8:
                    return clean_code
    return None


def get_country_info(phone_number):
    if not phone_number or str(phone_number).strip() == 'Unknown':
        return {'formatted_number': 'Unknown', 'country': '🌐 Unknown Country', 'country_code': None}
    try:
        phone_number = str(phone_number).strip()
        if not phone_number.startswith('+'):
            phone_number = f'+{phone_number}'
        parsed = phonenumbers.parse(phone_number)
        cc = phonenumbers.region_code_for_number(parsed)
        name = geocoder.description_for_number(parsed, 'en') or 'Unknown Country'
        emoji = COUNTRY_EMOJIS.get(cc, '🌐')
        return {
            'formatted_number': re.sub(r'[^\d+]', '', phone_number),
            'country': f'{emoji} {name}',
            'country_code': cc
        }
    except Exception:
        return {
            'formatted_number': re.sub(r'[^\d+]', '', str(phone_number)),
            'country': '🌐 Unknown Country',
            'country_code': None
        }


def fetch_latest_otp(api_cfg, session):
    today = datetime.now().strftime('%Y-%m-%d')
    ts = str(int(time.time() * 1000))
    params = {
        'fdate1': f'{today} 00:00:00',
        'fdate2': f'{today} 23:59:59',
        'frange': '', 'fclient': '', 'fnum': '', 'fcli': '',
        'fgdate': '', 'fgmonth': '', 'fgrange': '',
        'fgclient': '', 'fgnumber': '', 'fgcli': '',
        'fg': '0',
        **({'sesskey': api_cfg['sesskey']} if api_cfg.get('sesskey') else {}),
        'sEcho': '1',
        'iColumns': '9',
        'sColumns': ',,,,,,,,',
        'iDisplayStart': '0',
        'iDisplayLength': '25',
        **{f'mDataProp_{i}': str(i) for i in range(9)},
        **{f'sSearch_{i}': '' for i in range(9)},
        **{f'bRegex_{i}': 'false' for i in range(9)},
        **{f'bSearchable_{i}': 'true' for i in range(9)},
        **{f'bSortable_{i}': 'true' for i in range(8)},
        'bSortable_8': 'false',
        'sSearch': '',
        'bRegex': 'false',
        'iSortCol_0': '0',
        'sSortDir_0': 'desc',
        'iSortingCols': '1',
        '_': ts,
    }
    response = session.get(api_cfg['url'], params=params, timeout=10)
    response.raise_for_status()
    return response.json()


COUNTRY_LANGUAGES = {
    'AF': 'Pashto', 'AL': 'Albanian', 'DZ': 'Arabic', 'AD': 'Catalan', 'AO': 'Portuguese',
    'AG': 'English', 'AR': 'Spanish', 'AM': 'Armenian', 'AU': 'English', 'AT': 'German',
    'AZ': 'Azerbaijani', 'BS': 'English', 'BH': 'Arabic', 'BD': 'Bengali', 'BB': 'English',
    'BY': 'Belarusian', 'BE': 'Dutch', 'BZ': 'English', 'BJ': 'French', 'BT': 'Dzongkha',
    'BO': 'Spanish', 'BA': 'Bosnian', 'BW': 'English', 'BR': 'Portuguese', 'BN': 'Malay',
    'BG': 'Bulgarian', 'BF': 'French', 'BI': 'French', 'CV': 'Portuguese', 'KH': 'Khmer',
    'CM': 'French', 'CA': 'English', 'CF': 'French', 'TD': 'French', 'CL': 'Spanish',
    'CN': 'Chinese', 'CO': 'Spanish', 'KM': 'Arabic', 'CG': 'French', 'CR': 'Spanish',
    'HR': 'Croatian', 'CU': 'Spanish', 'CY': 'Greek', 'CZ': 'Czech', 'DK': 'Danish',
    'DJ': 'French', 'DM': 'English', 'DO': 'Spanish', 'EC': 'Spanish', 'EG': 'Arabic',
    'SV': 'Spanish', 'GQ': 'Spanish', 'ER': 'Tigrinya', 'EE': 'Estonian', 'SZ': 'Swazi',
    'ET': 'Amharic', 'FJ': 'English', 'FI': 'Finnish', 'FR': 'French', 'GA': 'French',
    'GM': 'English', 'GE': 'Georgian', 'DE': 'German', 'GH': 'English', 'GR': 'Greek',
    'GD': 'English', 'GT': 'Spanish', 'GN': 'French', 'GW': 'Portuguese', 'GY': 'English',
    'HT': 'French', 'HN': 'Spanish', 'HU': 'Hungarian', 'IS': 'Icelandic', 'IN': 'Hindi',
    'ID': 'Indonesian', 'IR': 'Persian', 'IQ': 'Arabic', 'IE': 'English', 'IL': 'Hebrew',
    'IT': 'Italian', 'JM': 'English', 'JP': 'Japanese', 'JO': 'Arabic', 'KZ': 'Kazakh',
    'KE': 'Swahili', 'KI': 'English', 'KP': 'Korean', 'KR': 'Korean', 'KW': 'Arabic',
    'KG': 'Kyrgyz', 'LA': 'Lao', 'LV': 'Latvian', 'LB': 'Arabic', 'LS': 'Sesotho',
    'LR': 'English', 'LY': 'Arabic', 'LI': 'German', 'LT': 'Lithuanian', 'LU': 'French',
    'MG': 'Malagasy', 'MW': 'English', 'MY': 'Malay', 'MV': 'Dhivehi', 'ML': 'French',
    'MT': 'Maltese', 'MH': 'Marshallese', 'MR': 'Arabic', 'MU': 'French', 'MX': 'Spanish',
    'FM': 'English', 'MD': 'Romanian', 'MC': 'French', 'MN': 'Mongolian', 'ME': 'Serbian',
    'MA': 'Arabic', 'MZ': 'Portuguese', 'MM': 'Burmese', 'NA': 'English', 'NR': 'Nauruan',
    'NP': 'Nepali', 'NL': 'Dutch', 'NZ': 'English', 'NI': 'Spanish', 'NE': 'French',
    'NG': 'English', 'NO': 'Norwegian', 'OM': 'Arabic', 'PK': 'Urdu', 'PW': 'Palauan',
    'PA': 'Spanish', 'PG': 'English', 'PY': 'Spanish', 'PE': 'Spanish', 'PH': 'Filipino',
    'PL': 'Polish', 'PT': 'Portuguese', 'QA': 'Arabic', 'RO': 'Romanian', 'RU': 'Russian',
    'RW': 'Kinyarwanda', 'KN': 'English', 'LC': 'English', 'VC': 'English', 'WS': 'Samoan',
    'SM': 'Italian', 'ST': 'Portuguese', 'SA': 'Arabic', 'SN': 'French', 'RS': 'Serbian',
    'SC': 'French', 'SL': 'English', 'SG': 'English', 'SK': 'Slovak', 'SI': 'Slovenian',
    'SB': 'English', 'SO': 'Somali', 'ZA': 'Zulu', 'SS': 'English', 'ES': 'Spanish',
    'LK': 'Sinhala', 'SD': 'Arabic', 'SR': 'Dutch', 'SE': 'Swedish', 'CH': 'German',
    'SY': 'Arabic', 'TW': 'Chinese', 'TJ': 'Tajik', 'TZ': 'Swahili', 'TH': 'Thai',
    'TL': 'Portuguese', 'TG': 'French', 'TO': 'Tongan', 'TT': 'English', 'TN': 'Arabic',
    'TR': 'Turkish', 'TM': 'Turkmen', 'TV': 'Tuvaluan', 'UG': 'English', 'UA': 'Ukrainian',
    'AE': 'Arabic', 'GB': 'English', 'US': 'English', 'UY': 'Spanish', 'UZ': 'Uzbek',
    'VU': 'French', 'VE': 'Spanish', 'VN': 'Vietnamese', 'YE': 'Arabic', 'ZM': 'English',
    'ZW': 'English',
}

COUNTRY_SHORT_NAMES = {
    'GB': 'UK', 'US': 'USA', 'AE': 'UAE', 'MY': 'Malaysia', 'SG': 'Singapore',
    'IN': 'India', 'PK': 'Pakistan', 'BD': 'Bangladesh', 'NG': 'Nigeria',
    'GH': 'Ghana', 'KE': 'Kenya', 'ZA': 'S.Africa', 'ET': 'Ethiopia',
    'RU': 'Russia', 'UA': 'Ukraine', 'DE': 'Germany', 'FR': 'France',
    'IT': 'Italy', 'ES': 'Spain', 'PT': 'Portugal', 'NL': 'Netherlands',
    'BE': 'Belgium', 'CH': 'Switzerland', 'AT': 'Austria', 'PL': 'Poland',
    'CN': 'China', 'JP': 'Japan', 'KR': 'S.Korea', 'VN': 'Vietnam',
    'TH': 'Thailand', 'ID': 'Indonesia', 'PH': 'Philippines', 'TR': 'Turkey',
    'SA': 'Saudi', 'IQ': 'Iraq', 'IR': 'Iran', 'EG': 'Egypt',
    'AU': 'Australia', 'NZ': 'NZ', 'CA': 'Canada', 'BR': 'Brazil',
    'MX': 'Mexico', 'AR': 'Argentina', 'CO': 'Colombia',
}

SERVICE_EMOJIS = {
    'whatsapp': '📞',  'telegram': '✈️',  'facebook': '💬',  'instagram': '📸',
    'twitter':  '🐦',  'tiktok':   '🎵',  'google':   '🔍',  'gmail':    '📧',
    'snapchat': '⭐',  'viber':    '📞',  'signal':   '🛡',  'discord':  '🎮',
    'reddit':   '👾',  'linkedin': '💻',  'line':     '💬',  'wechat':   '💬',
    'amazon':   '📦',  'netflix':  '▶️',  'spotify':  '🎵',  'youtube':  '📺',
    'paypal':   '💰',  'apple':    '🍎',  'microsoft':'💻',  'outlook':  '📩',
    'yahoo':    '📧',  'uber':     '🌐',  'lyft':     '🌐',  'binance':  '🟡',
    'coinbase': '🔵',  'bitcoin':  '🪙',
}

SERVICE_SHORT_NAMES = {
    'whatsapp': 'WA',  'telegram': 'TG',  'facebook': 'FB',  'instagram': 'IG',
    'twitter':  'TW',  'tiktok':   'TT',  'google':   'GG',  'gmail':    'GM',
    'snapchat': 'SC',  'viber':    'VB',  'signal':   'SG',  'discord':  'DC',
    'reddit':   'RDT', 'linkedin': 'LI',  'line':     'LN',  'wechat':   'WC',  'amazon':   'AMZ',
    'netflix':  'NF',  'spotify':  'SP',  'youtube':  'YT',  'paypal':   'PP',
    'apple':    'APL', 'microsoft':'MS',  'outlook':  'OL',  'yahoo':    'YH',
    'uber':     'UBR', 'lyft':     'LFT', 'binance':  'BNB', 'coinbase': 'CB',
    'bitcoin':  'BTC',
}


def parse_button(text, url):
    if "style:green" in url:
        text = "🟢 " + text
        url = url.replace("/style:green", "")
    elif "style:blue" in url:
        text = "🔵 " + text
        url = url.replace("/style:blue", "")
    elif "style:red" in url:
        text = "🔴 " + text
        url = url.replace("/style:red", "")
    return text, url


def mask_number(number):
    num = re.sub(r'[^\d+]', '', str(number))
    if not num.startswith('+'):
        num = f'+{num}'
    digits = num[1:]
    if len(digits) <= 8:
        return num
    visible_start = digits[:4]
    visible_end = digits[-4:]
    return f'+{visible_start}••{visible_end}'


def format_message(number, service, raw_text, otp_code, api_name):
    current_time = datetime.now().strftime('%H:%M')
    country_info = get_country_info(number)
    cc = country_info.get('country_code') or 'XX'
    flag = COUNTRY_EMOJIS.get(cc, '🌐')
    short_name = COUNTRY_SHORT_NAMES.get(cc, country_info['country'].split(' ', 1)[-1] if ' ' in country_info['country'] else country_info['country'])
    svc_key = detect_service(service, raw_text)
    svc_lower = svc_key.lower()
    # Use real pack custom emoji; fall back to plain Unicode if service not mapped
    service_icon  = SERVICE_CUSTOM_EMOJIS.get(svc_lower) or SERVICE_EMOJIS.get(svc_lower, tge(CE['phone'], '📱'))
    service_short = SERVICE_SHORT_NAMES.get(svc_lower, svc_key[:3].upper() if svc_key else '???')
    masked = mask_number(number)
    green = tge(CE['green'], '🟢')
    # Flag: use pack custom emoji if available, otherwise plain Unicode flag
    flag_id = COUNTRY_FLAG_CE.get(cc)
    flag_icon = tge(flag_id, flag) if flag_id else flag
    return (
        f'{green} {flag_icon} <b>{html_escape(short_name)}</b> | {service_icon} {html_escape(service_short)} | <code>{html_escape(masked)}</code>  ⏰ {html_escape(current_time)}'
    )


def send_telegram_message(config, number, service, raw_text, otp_code, api_name):
    formatted = format_message(number, service, raw_text, otp_code, api_name)
    url = f'https://api.telegram.org/bot{config["telegram"]["bot_token"]}/sendMessage'
    btn_styles = config.get('button_styles', {})
    def _s(key):
        v = btn_styles.get(key, '')
        return {'style': v} if v else {}
    inline_keyboard = {
        'inline_keyboard': [
            [{**{'text': '📋 Tap to Copy OTP', 'copy_text': {'text': str(otp_code)}}, **_s('copy')}],
            [{**{'text': '🤖 Bot Link', 'url': 'https://t.me/ANG0X5BOT'}, **_s('bot_link')}],
            [{**{'text': '📢 Channel',  'url': 'https://t.me/meta_otp'}, **_s('channel')}],
        ]
    }
    tg_cfg = config['telegram']
    chat_ids = tg_cfg.get('chat_ids') or [tg_cfg.get('chat_id')]
    chat_ids = [cid for cid in chat_ids if cid]
    all_ok = True
    for cid in chat_ids:
        payload = {
            'chat_id': cid,
            'text': formatted,
            'parse_mode': 'HTML',
            'disable_web_page_preview': True,
            'reply_markup': json.dumps(inline_keyboard)
        }
        sent = False
        for attempt in range(3):
            try:
                r = requests.post(url, data=payload, timeout=10)
                r.raise_for_status()
                sent = True
                break
            except requests.exceptions.RequestException as e:
                if attempt < 2:
                    time.sleep((attempt + 1) * 2)
                else:
                    logging.error(f'Telegram send failed for {cid}: {str(e)}')
        if not sent:
            all_ok = False
    return all_ok


def api_worker(api_id):
    logger = logging.getLogger(f'API:{api_id}')
    logger.info(f'Worker started for API: {api_id}')

    session = requests.Session()
    session.verify = False
    consecutive_failures = 0

    while _running:
        try:
            config = load_config()
            api_cfg = next((a for a in config.get('apis', []) if a['id'] == api_id), None)

            if not api_cfg:
                logger.warning(f'API {api_id} no longer in config, stopping worker')
                break

            if not api_cfg.get('enabled', True):
                update_api_status(api_id, {
                    'name': api_cfg.get('name', api_id),
                    'status': 'disabled',
                    'last_check': datetime.now().isoformat(),
                    'error': None,
                    'otps_sent': _api_status.get(api_id, {}).get('otps_sent', 0)
                })
                time.sleep(5)
                continue

            session.headers.clear()
            session.headers.update(api_cfg.get('headers', {}))
            session.cookies.clear()
            session.cookies.update(api_cfg.get('cookies', {}))

            data = fetch_latest_otp(api_cfg, session)
            consecutive_failures = 0

            if data.get('aaData'):
                latest = data['aaData'][0]
                number = latest[2] or 'Unknown'
                service = latest[3] or 'Unknown'
                raw_text = latest[5] or ''

                otp_code = extract_otp_code(raw_text)
                if otp_code:
                    msg_hash = hashlib.sha256(f'{number}{service}{raw_text}'.encode()).hexdigest()
                    num_hash = hashlib.sha256(f'{number}{otp_code}'.encode()).hexdigest()
                    otp_hash = hashlib.sha256(otp_code.encode()).hexdigest()

                    with _deque_lock:
                        is_dup = (
                            msg_hash in sent_message_hashes or
                            num_hash in sent_otp_numbers or
                            otp_hash in sent_otp_codes
                        )

                    if not is_dup:
                        success = send_telegram_message(config, number, service, raw_text, otp_code, api_cfg['name'])
                        if success:
                            with _deque_lock:
                                sent_message_hashes.append(msg_hash)
                                sent_otp_numbers.append(num_hash)
                                sent_otp_codes.append(otp_hash)

                            logger.info(f'OTP Sent: {otp_code} | Service: {service} | Number: {number}')
                            country_info = get_country_info(number)
                            entry = {
                                'id': str(uuid.uuid4())[:8],
                                'timestamp': datetime.now().isoformat(),
                                'api_id': api_id,
                                'api_name': api_cfg['name'],
                                'number': number,
                                'service': service,
                                'otp_code': otp_code,
                                'country': country_info['country'],
                                'raw_text': raw_text
                            }
                            log_activity(entry)
                            prev = _api_status.get(api_id, {})
                            update_api_status(api_id, {
                                'name': api_cfg['name'],
                                'status': 'active',
                                'last_check': datetime.now().isoformat(),
                                'error': None,
                                'otps_sent': prev.get('otps_sent', 0) + 1
                            })
                            continue

            update_api_status(api_id, {
                'name': api_cfg.get('name', api_id),
                'status': 'active',
                'last_check': datetime.now().isoformat(),
                'error': None,
                'otps_sent': _api_status.get(api_id, {}).get('otps_sent', 0)
            })
            time.sleep(config['settings'].get('polling_interval', 1))

        except Exception as e:
            consecutive_failures += 1
            err_str = str(e)

            # Honour Retry-After header (rate limiting / temporary bans)
            retry_after = 0
            try:
                resp = getattr(e, 'response', None)
                if resp is not None:
                    retry_after = int(resp.headers.get('Retry-After', 0))
            except (ValueError, TypeError, AttributeError):
                retry_after = 0

            if retry_after > 0:
                display_err = f'Rate limited by server — waiting {retry_after}s then retrying'
                logger.warning(f'Rate limited (HTTP 503 Retry-After:{retry_after}s) — sleeping')
            else:
                display_err = err_str
                logger.error(f'Error: {err_str}')

            api_cfg_safe = {}
            try:
                config = load_config()
                api_cfg_safe = next((a for a in config.get('apis', []) if a['id'] == api_id), {})
            except Exception:
                pass
            update_api_status(api_id, {
                'name': api_cfg_safe.get('name', api_id),
                'status': 'error',
                'last_check': datetime.now().isoformat(),
                'error': display_err,
                'otps_sent': _api_status.get(api_id, {}).get('otps_sent', 0)
            })

            if retry_after > 0:
                sleep_time = retry_after + 5  # +5s buffer
                # After 3+ rate-limited failures, session may be invalid — try re-login in background
                if consecutive_failures >= 3:
                    _trigger_auto_relogin(api_id, api_cfg_safe, logger)
            elif consecutive_failures >= 2:
                # Non-rate-limit failures: likely session expired — trigger re-login
                sleep_time = min(30 * consecutive_failures, 300)
                _trigger_auto_relogin(api_id, api_cfg_safe, logger)
            else:
                sleep_time = consecutive_failures * 5
            time.sleep(sleep_time)


def manager_loop():
    logger = logging.getLogger('Manager')
    logger.info('API manager started')

    while _running:
        try:
            config = load_config()
            api_ids = {a['id'] for a in config.get('apis', [])}

            for api in config.get('apis', []):
                api_id = api['id']
                if api_id not in _api_threads or not _api_threads[api_id].is_alive():
                    logger.info(f'Starting worker for {api_id} ({api["name"]})')
                    t = threading.Thread(target=api_worker, args=(api_id,), daemon=True, name=f'worker-{api_id}')
                    t.start()
                    _api_threads[api_id] = t

            stale = set(_api_threads.keys()) - api_ids
            for sid in stale:
                logger.info(f'API {sid} removed from config')
                _api_threads.pop(sid, None)
                _api_status.pop(sid, None)

        except Exception as e:
            logger.error(f'Manager error: {str(e)}')

        time.sleep(10)


def signal_handler(sig, frame):
    global _running
    logging.info('Shutting down...')
    _running = False
    sys.exit(0)


def start_bot():
    global _running
    _running = True
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    logging.info('OTP Bot starting — multi-API mode')

    if not os.path.exists(ACTIVITY_LOG_PATH):
        with open(ACTIVITY_LOG_PATH, 'w') as f:
            json.dump([], f)

    manager_thread = threading.Thread(target=manager_loop, daemon=True, name='manager')
    manager_thread.start()

    try:
        while _running:
            time.sleep(1)
    except KeyboardInterrupt:
        _running = False


if __name__ == '__main__':
    start_bot()
