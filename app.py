import threading
import time
import csv
import json
import xml.etree.ElementTree as ET
import uuid
import argparse
import secrets
import string
import os
import sys
import signal
import sqlite3
import subprocess
from io import StringIO, BytesIO
from datetime import datetime, timedelta
from fnmatch import fnmatch
from urllib.parse import urlparse

import streamlit as st
import streamlit.components.v1 as components
from dotenv import load_dotenv

# Ensure the root project directory is in Python's path so 'src' is always found
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

# 1. Page Configuration (Must be the very first Streamlit command)
st.set_page_config(
    page_title="LibreCrawl - SEO Spider",
    page_icon="🕷️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 2. Sync Streamlit Cloud Secrets into os.environ before importing internal modules
try:
    for k, v in st.secrets.items():
        if isinstance(v, (str, int, float, bool)):
            os.environ[str(k)] = str(v)
except Exception:
    pass

# Fallback to local .env if running locally
load_dotenv()

# 3. Ensure Playwright Chromium is installed on Streamlit Community Cloud startup
@st.cache_resource
def ensure_playwright_installed():
    try:
        subprocess.run([sys.executable, "-m", "playwright", "install", "chromium"], check=True)
    except Exception as e:
        print(f"Playwright installation check warning: {e}")

ensure_playwright_installed()

# 4. Import internal LibreCrawl modules
from src.crawler import WebCrawler
from src.settings_manager import SettingsManager
from src.auth_db import (
    init_db, create_user, authenticate_user, get_user_by_id,
    log_guest_crawl, get_guest_crawls_last_24h, verify_user,
    set_user_tier, create_verification_token, verify_token,
    get_user_by_email, get_or_create_zoho_user, hash_password,
    get_crawls_last_24h, log_crawl_start
)
from src.crawl_db import (
    get_user_crawls, get_crawl_count, get_crawl_by_id,
    load_crawled_urls, load_crawl_links, load_crawl_issues,
    delete_crawl, set_crawl_status, get_database_size_mb,
    get_crashed_crawls
)
from src.email_service import send_verification_email, send_welcome_email
from src.zoho_oauth import ZohoOAuthConfig, ZohoOAuthError

# ==============================================================================
# CLI ARGUMENTS & ENVIRONMENT CONFIGURATION
# ==============================================================================
@st.cache_resource
def parse_cli_and_env():
    parser = argparse.ArgumentParser(description='LibreCrawl - SEO Spider Tool')
    parser.add_argument('--local', '-l', action='store_true')
    parser.add_argument('--disable-register', '-dr', action='store_true')
    parser.add_argument('--disable-guest', '-dg', action='store_true')
    parser.add_argument('--demo', '-dm', action='store_true')
    parser.add_argument('--dangerously-skip-auth', '-dsa', action='store_true')
    args, _ = parser.parse_known_args()

    local_mode = args.local or os.getenv('LOCAL_MODE', '').lower() in ('true', '1', 'yes')
    disable_register = args.disable_register or os.getenv('REGISTRATION_DISABLED', '').lower() in ('true', '1', 'yes')
    disable_guest = args.disable_guest or os.getenv('DISABLE_GUEST', '').lower() in ('true', '1', 'yes')
    demo_mode = args.demo or os.getenv('DEMO_MODE', '').lower() in ('true', '1', 'yes')
    skip_auth = args.dangerously_skip_auth or os.getenv('DANGEROUSLY_SKIP_AUTH', '').lower() in ('true', '1', 'yes')
    zoho = ZohoOAuthConfig(registration_disabled=disable_register)

    return {
        'LOCAL_MODE': local_mode,
        'DISABLE_REGISTER': disable_register,
        'DISABLE_GUEST': disable_guest,
        'DEMO_MODE': demo_mode,
        'SKIP_AUTH': skip_auth,
        'ZOHO': zoho
    }

CONFIG = parse_cli_and_env()
LOCAL_MODE = CONFIG['LOCAL_MODE']
DISABLE_REGISTER = CONFIG['DISABLE_REGISTER']
DISABLE_GUEST = CONFIG['DISABLE_GUEST']
DEMO_MODE = CONFIG['DEMO_MODE']
SKIP_AUTH = CONFIG['SKIP_AUTH']
ZOHO = CONFIG['ZOHO']

# ==============================================================================
# GLOBAL MULTI-TENANT STATE & BACKGROUND SERVICES
# ==============================================================================
@st.cache_resource
def init_global_services():
    """Initializes DB, crash recovery, signal handlers, and multi-tenant store once across all sessions."""
    os.makedirs("data", exist_ok=True)
    init_db()

    crawler_instances = {}  # session_id -> {'crawler': WebCrawler, 'settings': SettingsManager, 'last_accessed': datetime}
    instances_lock = threading.Lock()
    oauth_states = {}  # state -> timestamp (for Zoho OAuth across redirects)

    # Crash Recovery
    try:
        crashed = get_crashed_crawls()
        if crashed:
            for crawl in crashed:
                set_crawl_status(crawl['id'], 'failed')
                print(f"Recovered crashed crawl ID {crawl['id']} ({crawl['base_url']}) -> marked failed.")
    except Exception as e:
        print(f"Error during crash recovery: {e}")

    # Cleanup Thread (removes sessions inactive > 1 hour)
    def cleanup_loop():
        while True:
            time.sleep(300)
            timeout = timedelta(hours=1)
            now = datetime.now()
            with instances_lock:
                to_remove = [sid for sid, data in crawler_instances.items() if now - data['last_accessed'] > timeout]
                for sid in to_remove:
                    try:
                        crawler_instances[sid]['crawler'].stop_crawl()
                    except Exception:
                        pass
                    del crawler_instances[sid]

    cleanup_thread = threading.Thread(target=cleanup_loop, daemon=True)
    cleanup_thread.start()

    # Graceful Shutdown Signal Handler
    def graceful_shutdown(signum, frame):
        print("\nSaving all active crawls before shutdown...")
        try:
            with instances_lock:
                for sid, instance_data in list(crawler_instances.items()):
                    crawler = instance_data['crawler']
                    if crawler.is_running and crawler.crawl_id and crawler.db_save_enabled:
                        try:
                            crawler._save_batch_to_db(force=True)
                            crawler._save_queue_checkpoint()
                            set_crawl_status(crawler.crawl_id, 'paused')
                        except Exception as e:
                            print(f"Error saving crawl {crawler.crawl_id}: {e}")
        except Exception as e:
            print(f"Shutdown error: {e}")
        sys.exit(0)

    try:
        signal.signal(signal.SIGINT, graceful_shutdown)
        signal.signal(signal.SIGTERM, graceful_shutdown)
    except ValueError:
        pass

    return crawler_instances, instances_lock, oauth_states

crawler_instances, instances_lock, oauth_states = init_global_services()

# ==============================================================================
# HELPER & AUTH FUNCTIONS
# ==============================================================================
def get_db_path():
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data', 'users.db')

def generate_random_password(length=16):
    alphabet = string.ascii_letters + string.digits + string.punctuation
    return ''.join(secrets.choice(alphabet) for _ in range(length))

def get_client_ip():
    """Extract client IP from Streamlit context headers."""
    try:
        headers = st.context.headers
        if not headers:
            return "127.0.0.1"
        if 'Cf-Connecting-Ip' in headers:
            return headers['Cf-Connecting-Ip']
        if 'X-Forwarded-For' in headers:
            return headers['X-Forwarded-For'].split(',')[0].strip()
        if 'X-Real-Ip' in headers:
            return headers['X-Real-Ip']
    except Exception:
        pass
    return "127.0.0.1"

def init_session():
    if 'session_id' not in st.session_state:
        st.session_state.session_id = str(uuid.uuid4())
    if 'authenticated' not in st.session_state:
        st.session_state.authenticated = False
        st.session_state.user_id = None
        st.session_state.username = None
        st.session_state.tier = 'guest'
        st.session_state.current_crawl_id = None
    if 'custom_css' not in st.session_state:
        st.session_state.custom_css = ""

init_session()

def auto_login_local_mode():
    try:
        conn = sqlite3.connect(get_db_path())
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute('SELECT id, username, tier FROM users WHERE username = ?', ('local',))
        user = cursor.fetchone()

        if user:
            user_id = user['id']
        else:
            random_password = generate_random_password()
            password_hash = hash_password(random_password)
            cursor.execute('''
                INSERT INTO users (username, email, password_hash, verified, tier)
                VALUES (?, ?, ?, 1, 'admin')
            ''', ('local', 'local@localhost', password_hash))
            conn.commit()
            user_id = cursor.lastrowid

        conn.close()
        st.session_state.user_id = user_id
        st.session_state.username = 'local'
        st.session_state.tier = 'admin'
        st.session_state.authenticated = True
        return True
    except Exception as e:
        st.error(f"Error in auto_login_local_mode: {e}")
        return False

def skip_auth_login(username):
    try:
        conn = sqlite3.connect(get_db_path())
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute('SELECT id, username FROM users WHERE username = ?', (username,))
        user = cursor.fetchone()

        if user:
            user_id = user['id']
        else:
            password_hash = hash_password(generate_random_password())
            cursor.execute('''
                INSERT INTO users (username, email, password_hash, verified, tier)
                VALUES (?, ?, ?, 1, 'admin')
            ''', (username, f'{username}@skipauth.local', password_hash))
            conn.commit()
            user_id = cursor.lastrowid

        conn.close()
        st.session_state.user_id = user_id
        st.session_state.username = username
        st.session_state.tier = 'admin'
        st.session_state.authenticated = True
        return True, 'Logged in (authentication skipped)'
    except sqlite3.IntegrityError as e:
        return False, f'Username conflict: try a different username ({e})'
    except Exception as e:
        return False, f'Login error: {str(e)}'

def get_or_create_crawler():
    session_id = st.session_state.session_id
    user_id = st.session_state.user_id
    tier = st.session_state.tier

    with instances_lock:
        if session_id not in crawler_instances:
            crawler_instances[session_id] = {
                'crawler': WebCrawler(),
                'settings': SettingsManager(session_id=session_id, user_id=user_id, tier=tier),
                'last_accessed': datetime.now()
            }
        else:
            crawler_instances[session_id]['last_accessed'] = datetime.now()
        return crawler_instances[session_id]['crawler']

def get_session_settings():
    session_id = st.session_state.session_id
    user_id = st.session_state.user_id
    tier = st.session_state.tier

    with instances_lock:
        if session_id not in crawler_instances:
            crawler_instances[session_id] = {
                'crawler': WebCrawler(),
                'settings': SettingsManager(session_id=session_id, user_id=user_id, tier=tier),
                'last_accessed': datetime.now()
            }
        else:
            crawler_instances[session_id]['last_accessed'] = datetime.now()
        return crawler_instances[session_id]['settings']

# ==============================================================================
# ISSUE FILTERING & EXPORT FUNCTIONS
# ==============================================================================
def issue_matches_exclusion(issue, exclusion_patterns):
    path = urlparse(issue.get('url', '')).path
    for pattern in exclusion_patterns:
        if not pattern.strip() or pattern.strip().startswith('#'):
            continue
        if '*' in pattern:
            if fnmatch(path, pattern):
                return True
        elif path == pattern or path.startswith(pattern.rstrip('*')):
            return True
    return False

def filter_issues_by_exclusion_patterns(issues, exclusion_patterns):
    if not exclusion_patterns:
        return issues
    return [i for i in issues if not issue_matches_exclusion(i, exclusion_patterns)]

def get_exclusion_patterns(settings_manager):
    current_settings = settings_manager.get_settings()
    exclusion_text = current_settings.get('issueExclusionPatterns', '')
    return [p.strip() for p in exclusion_text.split('\n') if p.strip()]

def _format_csv_value(value, field):
    if field == 'analytics' and isinstance(value, dict):
        parts = []
        if value.get('gtag') or value.get('ga4_id'): parts.append('GA4')
        if value.get('google_analytics'): parts.append('GA')
        if value.get('gtm_id'): parts.append('GTM')
        if value.get('facebook_pixel'): parts.append('FB')
        if value.get('hotjar'): parts.append('HJ')
        if value.get('mixpanel'): parts.append('MP')
        return ', '.join(parts)
    elif field in ('og_tags', 'twitter_tags') and isinstance(value, dict):
        return f"{len(value)} tags" if value else ''
    elif field == 'json_ld' and isinstance(value, list):
        return f"{len(value)} scripts" if value else ''
    elif field == 'images' and isinstance(value, list):
        return f"{len(value)} images" if value else ''
    elif field == 'internal_links' and isinstance(value, (int, float)):
        return f"{int(value)} internal links" if value else '0 internal links'
    elif field == 'external_links' and isinstance(value, (int, float)):
        return f"{int(value)} external links" if value else '0 external links'
    elif field in ('h2', 'h3') and isinstance(value, list):
        return ', '.join(value[:3]) + ('...' if len(value) > 3 else '')
    elif isinstance(value, (dict, list)):
        return str(value)
    return value

XLSX_MIMETYPE = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
_XLSX_CELL_LIMIT = 32767
LINK_EXPORT_FIELDS = ['source_url', 'target_url', 'anchor_text', 'is_internal', 'target_domain', 'target_status', 'placement']
ISSUE_EXPORT_FIELDS = ['url', 'type', 'category', 'issue', 'details']

def _xlsx_cell(value):
    if value is None or isinstance(value, bool) or isinstance(value, (int, float)):
        return value
    from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE
    text = ILLEGAL_CHARACTERS_RE.sub('', str(value))
    return text[:_XLSX_CELL_LIMIT]

def _build_xlsx(sheet_title, headers, rows):
    from openpyxl import Workbook
    workbook = Workbook(write_only=True)
    sheet = workbook.create_sheet(title=sheet_title)
    sheet.append(list(headers))
    for row in rows:
        sheet.append([_xlsx_cell(v) for v in row])
    buffer = BytesIO()
    workbook.save(buffer)
    return buffer.getvalue()

def _link_export_row(link):
    return {
        'source_url': link.get('source_url', ''),
        'target_url': link.get('target_url', ''),
        'anchor_text': link.get('anchor_text', ''),
        'is_internal': 'Yes' if link.get('is_internal') else 'No',
        'target_domain': link.get('target_domain', ''),
        'target_status': link['target_status'] if link.get('target_status') is not None else 'Not crawled',
        'placement': link.get('placement', 'body')
    }

def _issue_export_row(issue):
    return {
        'url': issue.get('url', ''),
        'type': issue.get('type', ''),
        'category': issue.get('category', ''),
        'issue': issue.get('issue', ''),
        'details': str(issue.get('details', ''))
    }

def generate_urls_export(urls, fields, fmt):
    if fmt == 'csv':
        output = StringIO()
        writer = csv.DictWriter(output, fieldnames=fields)
        writer.writeheader()
        for u in urls:
            writer.writerow({f: _format_csv_value(u.get(f, ''), f) for f in fields})
        return output.getvalue().encode('utf-8'), 'text/csv', 'csv'
    elif fmt == 'json':
        data = {
            'export_date': time.strftime('%Y-%m-%d %H:%M:%S'),
            'total_urls': len(urls),
            'fields': fields,
            'data': [{f: u.get(f, '') for f in fields} for u in urls]
        }
        return json.dumps(data, indent=2, default=str).encode('utf-8'), 'application/json', 'json'
    elif fmt == 'xml':
        root = ET.Element('librecrawl_export', export_date=time.strftime('%Y-%m-%d %H:%M:%S'), total_urls=str(len(urls)))
        urls_el = ET.SubElement(root, 'urls')
        for u in urls:
            u_el = ET.SubElement(urls_el, 'url')
            for f in fields:
                f_el = ET.SubElement(u_el, f)
                f_el.text = str(u.get(f, ''))
        return ET.tostring(root, encoding='utf-8', xml_declaration=True), 'application/xml', 'xml'
    elif fmt == 'xlsx':
        content = _build_xlsx('URLs', fields, ([_format_csv_value(u.get(f, ''), f) for f in fields] for u in urls))
        return content, XLSX_MIMETYPE, 'xlsx'

def generate_links_export(links, fmt):
    if fmt == 'csv':
        output = StringIO()
        writer = csv.DictWriter(output, fieldnames=LINK_EXPORT_FIELDS)
        writer.writeheader()
        for l in links:
            writer.writerow(_link_export_row(l))
        return output.getvalue().encode('utf-8'), 'text/csv', 'csv'
    elif fmt == 'json':
        return json.dumps(links, indent=2, default=str).encode('utf-8'), 'application/json', 'json'
    elif fmt == 'xml':
        root = ET.Element('librecrawl_links', export_date=time.strftime('%Y-%m-%d %H:%M:%S'), total_links=str(len(links)))
        links_el = ET.SubElement(root, 'links')
        for l in links:
            l_el = ET.SubElement(links_el, 'link')
            row = _link_export_row(l)
            for f in LINK_EXPORT_FIELDS:
                f_el = ET.SubElement(l_el, f)
                f_el.text = str(row.get(f, ''))
        return ET.tostring(root, encoding='utf-8', xml_declaration=True), 'application/xml', 'xml'
    elif fmt == 'xlsx':
        content = _build_xlsx('Links', LINK_EXPORT_FIELDS, ([_link_export_row(l)[f] for f in LINK_EXPORT_FIELDS] for l in links))
        return content, XLSX_MIMETYPE, 'xlsx'

def generate_issues_export(issues, fmt):
    if fmt == 'csv':
        output = StringIO()
        writer = csv.DictWriter(output, fieldnames=ISSUE_EXPORT_FIELDS)
        writer.writeheader()
        for i in issues:
            writer.writerow(_issue_export_row(i))
        return output.getvalue().encode('utf-8'), 'text/csv', 'csv'
    elif fmt == 'json':
        issues_by_url = {}
        for i in issues:
            u = i.get('url', '')
            issues_by_url.setdefault(u, []).append({
                'type': i.get('type', ''), 'category': i.get('category', ''),
                'issue': i.get('issue', ''), 'details': i.get('details', '')
            })
        data = {
            'export_date': time.strftime('%Y-%m-%d %H:%M:%S'),
            'total_issues': len(issues),
            'total_urls_with_issues': len(issues_by_url),
            'issues_by_url': issues_by_url,
            'all_issues': issues
        }
        return json.dumps(data, indent=2, default=str).encode('utf-8'), 'application/json', 'json'
    elif fmt == 'xml':
        root = ET.Element('librecrawl_issues', export_date=time.strftime('%Y-%m-%d %H:%M:%S'), total_issues=str(len(issues)))
        issues_el = ET.SubElement(root, 'issues')
        for i in issues:
            i_el = ET.SubElement(issues_el, 'issue')
            row = _issue_export_row(i)
            for f in ISSUE_EXPORT_FIELDS:
                f_el = ET.SubElement(i_el, f)
                f_el.text = str(row.get(f, ''))
        return ET.tostring(root, encoding='utf-8', xml_declaration=True), 'application/xml', 'xml'
    elif fmt == 'xlsx':
        content = _build_xlsx('Issues', ISSUE_EXPORT_FIELDS, ([_issue_export_row(i)[f] for f in ISSUE_EXPORT_FIELDS] for i in issues))
        return content, XLSX_MIMETYPE, 'xlsx'

# ==============================================================================
# URL QUERY PARAMETER INTERCEPTORS (Email Verify & Zoho OAuth Callback)
# ==============================================================================
query_params = st.query_params

# 1. Email Verification Handler (?token=...)
if "token" in query_params:
    token = query_params.get("token")
    success, message, app_source, user_email = verify_token(token)
    if success and user_email:
        try:
            user = get_user_by_email(user_email)
            if user:
                send_welcome_email(user_email, user['username'], app_source or 'main')
        except Exception as e:
            print(f"Error sending welcome email: {e}")

    st.title("📧 Email Verification")
    if success:
        st.success(message)
        if app_source == 'workshop':
            workshop_url = os.getenv('WORKSHOP_APP_URL', 'https://workshop.librecrawl.com')
            st.markdown(f"[Continue to Plugin Workshop]({workshop_url})")
        else:
            if st.button("Proceed to Login"):
                st.query_params.clear()
                st.rerun()
    else:
        st.error(message)
        if st.button("Back to Login"):
            st.query_params.clear()
            st.rerun()
    st.stop()

# 2. Zoho OAuth Callback Handler (?code=...&state=...)
if "code" in query_params and "state" in query_params and ZOHO.enabled:
    code = query_params.get("code")
    state = query_params.get("state")
    accounts_server_param = query_params.get("accounts-server")

    if state not in oauth_states:
        st.error("Zoho login expired or state was invalid. Please try again.")
    else:
        oauth_states.pop(state, None)
        try:
            redirect_uri = ZOHO.redirect_uri or os.getenv('MAIN_APP_URL', 'http://localhost:8501')
            accounts_server = ZOHO.accounts_server_for(accounts_server_param)
            identity = ZOHO.fetch_identity(code, redirect_uri, accounts_server)

            if not ZOHO.domain_allowed(identity['email']):
                st.error("Your Zoho account's email domain is not allowed to log in here.")
            else:
                success, message, user_data = get_or_create_zoho_user(
                    identity['zoho_id'], identity['email'], identity['name'],
                    default_tier=ZOHO.default_tier, allow_create=ZOHO.allow_signup
                )
                if success:
                    st.session_state.user_id = user_data['id']
                    st.session_state.username = user_data['username']
                    st.session_state.tier = user_data['tier']
                    st.session_state.authenticated = True
                    st.query_params.clear()
                    st.rerun()
                else:
                    st.error(message)
        except ZohoOAuthError as e:
            st.error(f"Zoho OAuth error: {e}")

    if st.button("Return to Login"):
        st.query_params.clear()
        st.rerun()
    st.stop()

# ==============================================================================
# AUTHENTICATION FLOW
# ==============================================================================
if LOCAL_MODE and not st.session_state.authenticated:
    auto_login_local_mode()

if not st.session_state.authenticated:
    st.title("🕷️ LibreCrawl - SEO Spider")
    auth_tabs = ["Login"]
    if not DISABLE_REGISTER:
        auth_tabs.append("Register")
    tabs = st.tabs(auth_tabs)

    # --- LOGIN TAB ---
    with tabs[0]:
        if SKIP_AUTH:
            st.warning("⚠️ **DANGEROUSLY SKIP AUTH ENABLED**: Enter any username to log in without a password.")
            with st.form("skip_auth_form"):
                username = st.text_input("Username (max 50 chars)")
                submitted = st.form_submit_button("Login", use_container_width=True)
                if submitted:
                    username = username.strip()
                    if not username or len(username) > 50:
                        st.error("Valid username required (1-50 chars).")
                    else:
                        ok, msg = skip_auth_login(username)
                        if ok:
                            st.rerun()
                        else:
                            st.error(msg)
        else:
            with st.form("login_form"):
                username = st.text_input("Username")
                password = st.text_input("Password", type="password")
                submitted = st.form_submit_button("Login", use_container_width=True)
                if submitted:
                    ok, msg, user_data = authenticate_user(username.strip(), password)
                    if ok:
                        st.session_state.user_id = user_data['id']
                        st.session_state.username = user_data['username']
                        st.session_state.tier = 'admin' if LOCAL_MODE else user_data['tier']
                        st.session_state.authenticated = True
                        st.rerun()
                    else:
                        st.error(msg)

        col_guest, col_zoho = st.columns(2)
        if not DISABLE_GUEST:
            with col_guest:
                if st.button("👤 Continue as Guest (3 crawls / 24h)", use_container_width=True):
                    st.session_state.user_id = None
                    st.session_state.username = 'Guest'
                    st.session_state.tier = 'admin' if LOCAL_MODE else 'guest'
                    st.session_state.authenticated = True
                    st.rerun()

        if ZOHO.enabled:
            with col_zoho:
                state = secrets.token_urlsafe(32)
                oauth_states[state] = time.time()
                redirect_uri = ZOHO.redirect_uri or os.getenv('MAIN_APP_URL', 'http://localhost:8501')
                auth_url = ZOHO.authorize_url(redirect_uri, state)
                st.link_button("🔐 Login with Zoho", auth_url, use_container_width=True)

    # --- REGISTER TAB ---
    if not DISABLE_REGISTER:
        with tabs[1]:
            with st.form("register_form"):
                reg_user = st.text_input("Username")
                reg_email = st.text_input("Email")
                reg_pass = st.text_input("Password", type="password")
                reg_submit = st.form_submit_button("Create Account", use_container_width=True)

                if reg_submit:
                    ok, msg, uid = create_user(reg_user, reg_email, reg_pass)
                    if ok and LOCAL_MODE:
                        verify_user(uid)
                        set_user_tier(uid, 'admin')
                        st.success("Account created and auto-verified with Admin access (Local Mode)! Please log in.")
                    elif ok:
                        is_resend = (msg == 'resend')
                        token = create_verification_token(uid, app_source='main')
                        if token:
                            e_ok, e_msg = send_verification_email(reg_email, reg_user, token, app_source='main', is_resend=is_resend)
                            if e_ok:
                                st.success("Registration successful! Please check your email to verify your account.")
                            else:
                                st.warning(f"Account created, but verification email failed: {e_msg}")
                        else:
                            st.error("Verification token generation failed.")
                    else:
                        st.error(msg)
    st.stop()

# ==============================================================================
# AUTHENTICATED APPLICATION
# ==============================================================================
crawler = get_or_create_crawler()
settings_manager = get_session_settings()
current_settings = settings_manager.get_settings()

# Inject Custom CSS if configured
custom_css = current_settings.get('customCSS', '') or st.session_state.custom_css
if custom_css:
    st.markdown(f"<style>{custom_css}</style>", unsafe_allow_html=True)

# --- SIDEBAR: USER INFO & NAVIGATION ---
with st.sidebar:
    st.title("🕷️ LibreCrawl")
    user_id = st.session_state.user_id
    tier = st.session_state.tier
    username = st.session_state.username

    crawls_today = get_guest_crawls_last_24h(get_client_ip()) if tier == 'guest' else get_crawls_last_24h(user_id)
    remaining = max(0, 3 - crawls_today) if tier == 'guest' else "Unlimited"

    st.markdown(f"**User:** `{username}`  \n**Tier:** `{tier.upper()}`  \n**Crawls (24h):** `{crawls_today}` (Remaining: `{remaining}`)")

    if DEMO_MODE:
        st.caption("⚠️ Demo Mode: 1.5GB RAM limit active")

    st.divider()
    nav_page = st.radio(
        "Navigation",
        ["🕷️ Live Spider", "🗂️ Crawl History Dashboard", "⚙️ Settings", "🧠 Memory & Profiler"],
        label_visibility="collapsed"
    )
    st.divider()

    if st.button("🚪 Logout", use_container_width=True):
        st.session_state.clear()
        st.rerun()

# ==============================================================================
# PAGE 1: LIVE SPIDER & ANALYSIS
# ==============================================================================
if nav_page == "🕷️ Live Spider":
    col_url, col_start, col_pause, col_stop, col_refresh = st.columns([5, 1.2, 1.2, 1.2, 1.4])
    with col_url:
        target_url = st.text_input("Target URL", value=crawler.base_url or "", placeholder="https://example.com", label_visibility="collapsed")
    with col_start:
        start_btn = st.button("▶️ Start", type="primary", use_container_width=True, disabled=crawler.is_running)
    with col_pause:
        if getattr(crawler, 'is_paused', False):
            if st.button("▶️ Resume", use_container_width=True):
                ok, msg = crawler.resume_crawl()
                st.toast(msg)
                st.rerun()
        else:
            if st.button("⏸️ Pause", use_container_width=True, disabled=not crawler.is_running):
                ok, msg = crawler.pause_crawl()
                st.toast(msg)
                st.rerun()
    with col_stop:
        if st.button("⏹️ Stop", use_container_width=True, disabled=not crawler.is_running):
            ok, msg = crawler.stop_crawl()
            st.toast(msg)
            st.rerun()
    with col_refresh:
        auto_refresh = st.toggle("⚡ Auto-Poll", value=True)

    # --- NEW: Bulk URL File Uploader ---
    with st.expander("📁 Or Upload URL List (.txt / .csv)", expanded=False):
        uploaded_file = st.file_uploader("Upload a text file (one URL per line) or CSV file", type=["txt", "csv"])
        uploaded_urls = []
        if uploaded_file is not None:
            file_content = uploaded_file.getvalue().decode("utf-8")
            if uploaded_file.name.endswith('.csv'):
                reader = csv.reader(StringIO(file_content))
                for row in reader:
                    if row:
                        # Grab first column or check for 'url' header
                        val = row[0].strip()
                        if val.startswith('http'):
                            uploaded_urls.append(val)
            else:
                for line in file_content.splitlines():
                    line = line.strip()
                    if line and not line.startswith('#') and line.startswith('http'):
                        uploaded_urls.append(line)
            
            if uploaded_urls:
                st.info(f"Loaded **{len(uploaded_urls)} URLs** from file. Click 'Start Bulk Crawl' below to begin.")

    # Start button handling (supports single URL or uploaded file list)
    bulk_start_btn = False
    if uploaded_urls:
        bulk_start_btn = st.button(f"▶️ Start Bulk Crawl ({len(uploaded_urls)} URLs)", type="primary", use_container_width=True, disabled=crawler.is_running)

    if start_btn or bulk_start_btn:
        urls_to_crawl = uploaded_urls if (bulk_start_btn and uploaded_urls) else ([target_url] if target_url else [])
        
        if not urls_to_crawl:
            st.error("Please enter a valid URL or upload a file containing URLs.")
        else:
            if tier == 'guest' and not LOCAL_MODE:
                client_ip = get_client_ip()
                if get_guest_crawls_last_24h(client_ip) >= 3:
                    st.error("Guest limit reached: 3 crawls per 24 hours. Please register for unlimited crawls.")
                    st.stop()
                log_guest_crawl(client_ip)

            try:
                crawler.update_config(settings_manager.get_crawler_config())
            except Exception as e:
                st.warning(f"Could not apply settings: {e}")

            if DEMO_MODE:
                crawler.config['demo_mode'] = True
                crawler.config['demo_memory_limit_bytes'] = int(1.5 * 1024 * 1024 * 1024)

            # Start crawl with the primary URL
            primary_url = urls_to_crawl[0]
            ok, msg = crawler.start_crawl(primary_url, user_id=user_id, session_id=st.session_state.session_id)
            
            # If there are additional URLs from an uploaded file, seed them into the crawler queue if supported
            if ok and len(urls_to_crawl) > 1 and hasattr(crawler, 'queue'):
                for extra_url in urls_to_crawl[1:]:
                    crawler.queue.add(extra_url, depth=0)

            if ok and crawler.crawl_id:
                st.session_state.current_crawl_id = crawler.crawl_id
                log_crawl_start(user_id, primary_url)
                st.toast(f"Crawl started with {len(urls_to_crawl)} target(s): {msg}")
                st.rerun()
            else:
                st.error(msg)

    # Fetch Current Crawl Status & Filter Issues
    status_data = crawler.get_status()
    stats = status_data.get('stats', {})
    urls_data = status_data.get('urls', [])
    links_data = status_data.get('links', [])
    exclusion_patterns = get_exclusion_patterns(settings_manager)
    issues_data = filter_issues_by_exclusion_patterns(status_data.get('issues', []), exclusion_patterns)

    # Metrics Row
    m1, m2, m3, m4, m5, m6 = st.columns(6)
    m1.metric("Status", status_data.get('status', 'idle').upper())
    m2.metric("Crawled URLs", len(urls_data))
    m3.metric("Discovered", stats.get('discovered', len(urls_data)))
    m4.metric("Links Found", len(links_data))
    m5.metric("Issues Detected", len(issues_data))
    m6.metric("Speed / Depth", f"{stats.get('speed', 0)}/s | D:{stats.get('max_depth', 0)}")

    # Discover Custom JS Plugins from web/static/plugins/
    plugin_files = []
    plugins_dir = os.path.join("web", "static", "plugins")
    if os.path.isdir(plugins_dir):
        plugin_files = [f for f in os.listdir(plugins_dir) if f.endswith('.js') and not f.startswith('_')]

    main_tabs = st.tabs([
        f"🌐 URLs ({len(urls_data)})",
        f"🔗 Links ({len(links_data)})",
        f"⚠️ Issues ({len(issues_data)})",
        "🕸️ Site Graph Visualization",
        "📥 Export Data",
        f"🔌 Plugins ({len(plugin_files)})"
    ])

    # --- TAB 1: URLS ---
    with main_tabs[0]:
        if urls_data:
            f_col1, f_col2 = st.columns([3, 1])
            search_q = f_col1.text_input("Filter URLs by path or title", key="url_search")
            status_filter = f_col2.selectbox("Status Code", ["All", "2xx (Success)", "3xx (Redirect)", "4xx (Client Error)", "5xx (Server Error)"])

            filtered_urls = urls_data
            if search_q:
                q_low = search_q.lower()
                filtered_urls = [u for u in filtered_urls if q_low in u.get('url', '').lower() or q_low in str(u.get('title', '')).lower()]
            if status_filter != "All":
                prefix = int(status_filter[0]) * 100
                filtered_urls = [u for u in filtered_urls if prefix <= (u.get('status_code') or 0) < prefix + 100]

            st.dataframe(filtered_urls, use_container_width=True, height=500)
        else:
            st.info("No URLs crawled yet. Enter a URL above and click Start.")

    # --- TAB 2: LINKS ---
    with main_tabs[1]:
        if links_data:
            l_col1, l_col2 = st.columns([3, 1])
            link_q = l_col1.text_input("Filter Links by source, target, or anchor text", key="link_search")
            link_type = l_col2.selectbox("Link Type", ["All", "Internal Only", "External Only"])

            filtered_links = links_data
            if link_q:
                lq = link_q.lower()
                filtered_links = [l for l in filtered_links if lq in l.get('source_url', '').lower() or lq in l.get('target_url', '').lower() or lq in str(l.get('anchor_text', '')).lower()]
            if link_type == "Internal Only":
                filtered_links = [l for l in filtered_links if l.get('is_internal')]
            elif link_type == "External Only":
                filtered_links = [l for l in filtered_links if not l.get('is_internal')]

            st.dataframe(filtered_links, use_container_width=True, height=500)
        else:
            st.info("No links discovered yet.")

    # --- TAB 3: ISSUES ---
    with main_tabs[2]:
        if issues_data:
            i_col1, i_col2 = st.columns([2, 2])
            issue_cat = i_col1.selectbox("Category", ["All"] + sorted(list({i.get('category', '') for i in issues_data if i.get('category')})))
            issue_type = i_col2.selectbox("Severity / Type", ["All"] + sorted(list({i.get('type', '') for i in issues_data if i.get('type')})))

            filtered_issues = issues_data
            if issue_cat != "All":
                filtered_issues = [i for i in filtered_issues if i.get('category') == issue_cat]
            if issue_type != "All":
                filtered_issues = [i for i in filtered_issues if i.get('type') == issue_type]

            st.dataframe(filtered_issues, use_container_width=True, height=500)
        else:
            st.info("No SEO issues detected (or excluded by pattern settings).")

    # --- TAB 4: SITE STRUCTURE VISUALIZATION ---
    with main_tabs[3]:
        if urls_data:
            max_nodes = 500
            pages_to_vis = urls_data[:max_nodes]
            if len(urls_data) > max_nodes:
                st.caption(f"Showing top {max_nodes} of {len(urls_data)} pages for performance.")

            nodes = []
            edges = []
            url_to_id = {}

            for idx, page in enumerate(pages_to_vis):
                u = page.get('url', '')
                sc = page.get('status_code', 0)
                if 200 <= sc < 300: color = '#10b981'
                elif 300 <= sc < 400: color = '#3b82f6'
                elif 400 <= sc < 500: color = '#f59e0b'
                elif 500 <= sc < 600: color = '#ef4444'
                else: color = '#6b7280'

                node_id = f'node-{idx}'
                url_to_id[u] = node_id
                nodes.append({
                    'data': {
                        'id': node_id,
                        'label': u.split('/')[-1] or u.split('//')[-1],
                        'url': u,
                        'status_code': sc,
                        'title': page.get('title', ''),
                        'color': color,
                        'size': 30 if idx == 0 else 20,
                        'depth': page.get('depth', 0)
                    }
                })

            edges_set = set()
            for link in links_data:
                if link.get('is_internal'):
                    s_id = url_to_id.get(link.get('source_url', ''))
                    t_id = url_to_id.get(link.get('target_url', ''))
                    if s_id and t_id and s_id != t_id:
                        e_key = f'{s_id}-{t_id}'
                        if e_key not in edges_set:
                            edges_set.add(e_key)
                            edges.append({'data': {'id': f'edge-{e_key}', 'source': s_id, 'target': t_id}})

            cytoscape_html = f"""
            <div id="cy" style="width: 100%; height: 600px; background: #1e1e2e; border-radius: 8px;"></div>
            <script src="https://cdnjs.cloudflare.com/ajax/libs/cytoscape/3.26.0/cytoscape.min.js"></script>
            <script>
                var cy = cytoscape({{
                    container: document.getElementById('cy'),
                    elements: {json.dumps(nodes + edges)},
                    style: [
                        {{
                            selector: 'node',
                            style: {{
                                'background-color': 'data(color)',
                                'label': 'data(label)',
                                'width': 'data(size)',
                                'height': 'data(size)',
                                'color': '#ffffff',
                                'font-size': '10px'
                            }}
                        }},
                        {{
                            selector: 'edge',
                            style: {{
                                'width': 1,
                                'line-color': '#555',
                                'target-arrow-color': '#555',
                                'target-arrow-shape': 'triangle',
                                'curve-style': 'bezier'
                            }}
                        }}
                    ],
                    layout: {{ name: 'cose', animate: false }}
                }});
            </script>
            """
            components.html(cytoscape_html, height=620)
        else:
            st.info("Crawl a site to generate the interactive site structure graph.")

    # --- TAB 5: EXPORT DATA ---
    with main_tabs[4]:
        st.subheader("Export Crawl Data")
        exp_col1, exp_col2 = st.columns(2)
        export_format = exp_col1.selectbox("Export Format", ["csv", "xlsx", "json", "xml"])

        all_available_fields = [
            'url', 'status_code', 'title', 'meta_description', 'h1', 'h2', 'h3',
            'word_count', 'response_time', 'canonical', 'meta_robots', 'internal_links',
            'external_links', 'images', 'og_tags', 'twitter_tags', 'json_ld',
            'analytics', 'depth', 'issues_detected', 'links_detailed'
        ]
        selected_fields = exp_col2.multiselect(
            "Select Fields to Export",
            all_available_fields,
            default=['url', 'status_code', 'title', 'meta_description', 'h1', 'word_count', 'issues_detected', 'links_detailed']
        )

        if urls_data:
            if links_data and urls_data:
                status_lookup = {u['url']: u.get('status_code') for u in urls_data}
                for link in links_data:
                    t_url = link.get('target_url')
                    if t_url in status_lookup:
                        link['target_status'] = status_lookup[t_url]

            ts = int(time.time())
            regular_fields = [f for f in selected_fields if f not in ('issues_detected', 'links_detailed') and f.replace('_', '').isalnum()]

            dl_col1, dl_col2, dl_col3 = st.columns(3)
            if regular_fields:
                u_bytes, u_mime, u_ext = generate_urls_export(urls_data, regular_fields, export_format)
                dl_col1.download_button(
                    label=f"⬇️ Download URLs ({u_ext.upper()})",
                    data=u_bytes,
                    file_name=f"librecrawl_export_{ts}.{u_ext}",
                    mime=u_mime,
                    use_container_width=True
                )

            if 'links_detailed' in selected_fields and links_data:
                l_bytes, l_mime, l_ext = generate_links_export(links_data, export_format)
                dl_col2.download_button(
                    label=f"⬇️ Download Detailed Links ({l_ext.upper()})",
                    data=l_bytes,
                    file_name=f"librecrawl_links_{ts}.{l_ext}",
                    mime=l_mime,
                    use_container_width=True
                )

            if 'issues_detected' in selected_fields and issues_data:
                i_bytes, i_mime, i_ext = generate_issues_export(issues_data, export_format)
                dl_col3.download_button(
                    label=f"⬇️ Download SEO Issues ({i_ext.upper()})",
                    data=i_bytes,
                    file_name=f"librecrawl_issues_{ts}.{i_ext}",
                    mime=i_mime,
                    use_container_width=True
                )
        else:
            st.warning("No crawl data available in session to export.")

    # --- TAB 6: JS PLUGIN RUNNER ---
    with main_tabs[5]:
        if not plugin_files:
            st.info("No custom plugins found in `/web/static/plugins/`. Drop a `.js` file there to load it!")
        else:
            selected_plugin = st.selectbox("Select Plugin to Execute", plugin_files)
            plugin_path = os.path.join(plugins_dir, selected_plugin)
            with open(plugin_path, "r", encoding="utf-8") as f:
                plugin_js_code = f.read()

            payload = json.dumps({
                'urls': urls_data,
                'links': links_data,
                'issues': issues_data,
                'stats': stats
            }, default=str)

            plugin_bridge_html = f"""
            <style>
                body {{ font-family: sans-serif; color: #e2e8f0; background: #0f172a; margin: 0; padding: 10px; }}
                .plugin-content {{ padding: 15px; }}
                .data-table {{ width: 100%; border-collapse: collapse; margin-top: 10px; }}
                .data-table th, .data-table td {{ border: 1px solid #334155; padding: 8px; text-align: left; }}
                .stat-card {{ background: #1e293b; padding: 12px; border-radius: 6px; margin-bottom: 10px; }}
                .score-good {{ color: #10b981; }}
                .score-needs-improvement {{ color: #f59e0b; }}
                .score-poor {{ color: #ef4444; }}
            </style>
            <div id="plugin-container"></div>
            <script>
                const crawlData = {payload};
                window.LibreCrawlPlugin = {{
                    register: function(config) {{
                        config.isActive = true;
                        config.utils = {{
                            showNotification: (msg, type) => console.log(type, msg),
                            formatUrl: (u) => u,
                            escapeHtml: (t) => String(t).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
                        }};
                        if (config.onLoad) config.onLoad();
                        const container = document.getElementById('plugin-container');
                        if (config.onTabActivate) config.onTabActivate(container, crawlData);
                    }}
                }};
                {plugin_js_code}
            </script>
            """
            components.html(plugin_bridge_html, height=600, scrolling=True)

    # Live Auto-Polling Loop while crawl is running
    if crawler.is_running and auto_refresh:
        time.sleep(2)
        st.rerun()

# ==============================================================================
# PAGE 2: CRAWL HISTORY DASHBOARD
# ==============================================================================
elif nav_page == "🗂️ Crawl History Dashboard":
    st.title("🗂️ Crawl History & Recovery Dashboard")

    total_crawls = get_crawl_count(user_id)
    db_size = get_database_size_mb()

    conn = sqlite3.connect(get_db_path())
    cursor = conn.cursor()
    cursor.execute('SELECT status, COUNT(*) FROM crawls WHERE user_id = ? GROUP BY status', (user_id,))
    status_counts = {row[0]: row[1] for row in cursor.fetchall()}
    conn.close()

    sc1, sc2, sc3, sc4 = st.columns(4)
    sc1.metric("Total Stored Crawls", total_crawls)
    sc2.metric("Completed", status_counts.get('completed', 0))
    sc3.metric("Paused / Failed", status_counts.get('paused', 0) + status_counts.get('failed', 0))
    sc4.metric("Database Size", f"{db_size:.2f} MB")

    st.divider()
    filter_status = st.selectbox("Filter by Status", ["All", "completed", "running", "paused", "failed", "archived"])
    crawls_list = get_user_crawls(user_id, limit=100, offset=0, status_filter=None if filter_status == "All" else filter_status)

    if not crawls_list:
        st.info("No historical crawls found.")
    else:
        for c in crawls_list:
            cid = c['id']
            with st.expander(f"#{cid} — {c.get('base_url')} | Status: {c.get('status', '').upper()} | Started: {c.get('created_at', '')}"):
                b1, b2, b3, b4 = st.columns(4)
                if b1.button("📂 Load into Session", key=f"load_{cid}", use_container_width=True):
                    if crawler.is_running:
                        crawler.stop_crawl()
                    u = load_crawled_urls(cid)
                    l = load_crawl_links(cid)
                    i = load_crawl_issues(cid)
                    crawler.load_data(c, u, l, i)
                    st.success(f"Loaded {len(u)} URLs, {len(l)} links, and {len(i)} issues into active session!")

                if b2.button("▶️ Resume Crawl", key=f"resume_{cid}", use_container_width=True, disabled=c.get('status') in ('completed', 'running')):
                    if DEMO_MODE:
                        crawler.config['demo_mode'] = True
                        crawler.config['demo_memory_limit_bytes'] = int(1.5 * 1024 * 1024 * 1024)
                    ok, msg = crawler.resume_from_database(cid, user_id=user_id, session_id=st.session_state.session_id)
                    if ok:
                        st.session_state.current_crawl_id = cid
                        st.success(msg)
                    else:
                        st.error(msg)

                if b3.button("📦 Archive", key=f"arch_{cid}", use_container_width=True):
                    if set_crawl_status(cid, 'archived'):
                        st.toast(f"Crawl #{cid} archived.")
                        st.rerun()

                if b4.button("🗑️ Delete", key=f"del_{cid}", type="secondary", use_container_width=True):
                    if delete_crawl(cid):
                        st.toast(f"Crawl #{cid} deleted.")
                        st.rerun()

# ==============================================================================
# PAGE 3: SETTINGS
# ==============================================================================
elif nav_page == "⚙️ Settings":
    st.title("⚙️ Crawler & UI Settings")
    current_settings = settings_manager.get_settings()

    with st.expander("🎨 Quick Editors (Issue Exclusion Patterns & Custom CSS)", expanded=True):
        excl_patterns = st.text_area(
            "Issue Exclusion Patterns (one URL path pattern per line, supports * wildcards)",
            value=current_settings.get('issueExclusionPatterns', ''),
            height=120
        )
        css_input = st.text_area(
            "Custom CSS Styling",
            value=current_settings.get('customCSS', ''),
            height=120
        )
        current_settings['issueExclusionPatterns'] = excl_patterns
        current_settings['customCSS'] = css_input

    with st.expander("🛠️ Full Configuration JSON Editor (Depth, Delays, JS Rendering, PageSpeed API Key, Proxies)", expanded=True):
        settings_json_str = st.text_area(
            "Settings Configuration JSON",
            value=json.dumps(current_settings, indent=2),
            height=400
        )

    s_col1, s_col2 = st.columns(2)
    if s_col1.button("💾 Save & Apply Settings", type="primary", use_container_width=True):
        try:
            updated_dict = json.loads(settings_json_str)
            updated_dict['issueExclusionPatterns'] = excl_patterns
            updated_dict['customCSS'] = css_input
            ok, msg = settings_manager.save_settings(updated_dict)
            if ok:
                crawler.update_config(settings_manager.get_crawler_config())
                st.session_state.custom_css = css_input
                st.success(f"Settings saved and applied to crawler: {msg}")
            else:
                st.error(msg)
        except json.JSONDecodeError as e:
            st.error(f"Invalid JSON format: {e}")

    if s_col2.button("🔄 Reset to Defaults", use_container_width=True):
        ok, msg = settings_manager.reset_settings()
        if ok:
            crawler.update_config(settings_manager.get_crawler_config())
            st.session_state.custom_css = ""
            st.success(msg)
            st.rerun()
        else:
            st.error(msg)

# ==============================================================================
# PAGE 4: MEMORY & PROFILER DEBUGGER
# ==============================================================================
elif nav_page == "🧠 Memory & Profiler":
    st.title("🧠 Memory Monitor & RAM Profiler")

    with instances_lock:
        st.metric("Active Multi-Tenant Sessions", len(crawler_instances))
        instance_rows = []
        for sid, inst_data in crawler_instances.items():
            c_inst = inst_data['crawler']
            instance_rows.append({
                'session_id': sid[:8] + '...',
                'last_accessed': inst_data['last_accessed'].isoformat(),
                'urls_crawled': len(c_inst.crawl_results),
                'memory_stats': c_inst.memory_monitor.get_stats(),
                'data_sizes': c_inst.user_memory.get_stats()
            })

    st.subheader("Active Crawler Instances")
    st.json(instance_rows)

    st.divider()
    st.subheader("Deep Object Memory Breakdown")
    if st.button("🔍 Run Memory Profiler"):
        from src.core.memory_profiler import MemoryProfiler
        profiles = []
        with instances_lock:
            for sid, inst_data in crawler_instances.items():
                c_inst = inst_data['crawler']
                profiles.append({
                    'session_id': sid[:8] + '...',
                    'urls_crawled': len(c_inst.crawl_results),
                    'object_breakdown': MemoryProfiler.get_object_memory_breakdown(),
                    'data_sizes': c_inst.user_memory.get_stats()
                })
        st.json({'total_instances': len(profiles), 'profiles': profiles})
