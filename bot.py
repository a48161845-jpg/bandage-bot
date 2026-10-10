import asyncio
import copy
import html
import json
import logging
import os
import random
import re
import socket
import time
from logging.handlers import RotatingFileHandler
from contextlib import asynccontextmanager, suppress
from contextvars import ContextVar
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher
from functools import lru_cache, wraps

from aiogram import Bot, Dispatcher, Router, F
from aiogram.exceptions import TelegramNetworkError, TelegramRetryAfter, TelegramServerError
from aiogram.filters import Command
from aiogram.types import (
    BotCommand,
    BufferedInputFile,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)
from telethon import TelegramClient, events, utils as tg_utils
from telethon import errors as tl_errors
from telethon.errors import (
    FloodWaitError,
    PeerFloodError,
    PhoneCodeExpiredError,
    PhoneCodeInvalidError,
    RPCError,
    SessionPasswordNeededError,
)
from telethon.tl import functions
from telethon.tl import types as tl_types
from telethon.tl.types import (
    Channel,
    ChannelParticipantsAdmins,
    InputUserEmpty,
    User,
    UserStatusLastMonth,
    UserStatusLastWeek,
    UserStatusOffline,
    UserStatusOnline,
    UserStatusRecently,
)

CONFIG_FILE = "config.json"
PROCESSED_FILE = "processed_users.txt"
DM_STATES_FILE = "dm_states.json"
DM_META_FILE = "dm_meta.json"
REPLY_MAP_FILE = "reply_map.json"
ACC_INFO_FILE = "accounts_info.json"
SPAM_FILE = "spam_state.json"
STATS_FILE = "stats.json"
AGREED_FILE = "agreed.json"
SPAM_TZ = timezone(timedelta(hours=7))    # срок спам-блока показываем в МСК+4 (UTC+7); SpamBot отдаёт время в UTC
MSK = timezone(timedelta(hours=3))   # статистика считается по московскому времени (в России нет перехода на летнее)
CHAT_NAMES_FILE = "chat_names.json"
SEEN_FILE = "seen_msgs.json"

CONNECT_TIMEOUT = 40        # сек на подключение аккаунта (раньше могло висеть бесконечно и блокировать ВСЕ команды)
ACCEPT_POLL = 15            # как часто (сек) проверять заявки, даже если проверку никто не «разбудил»
STALE_SEC = 6 * 3600        # сообщения старше (бот был офлайн) не получают автоответ — только уведомление вам

log = logging.getLogger("bot")


def setup_logging():
    """Лог в консоль и в bot.log (2 МБ x 3 файла): раньше ошибки в обработке ЛС и автопринятии глотались молча."""
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s", "%d.%m %H:%M:%S")
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    if not root.handlers:
        sh = logging.StreamHandler()
        sh.setFormatter(fmt)
        root.addHandler(sh)
        with suppress(Exception):
            fh = RotatingFileHandler("bot.log", maxBytes=2_000_000, backupCount=2, encoding="utf-8")
            fh.setFormatter(fmt)
            root.addHandler(fh)
    for noisy in ("telethon", "aiogram.event"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

BLANK_ACCOUNT = {"api_id": 0, "api_hash": "", "phone": ""}

DEFAULT_CONFIG = {
    "bot_token": "",  # токен лучше хранить в config.json или переменной BOT_TOKEN
    "admin_ids": [7233257134],
    # Аккаунты по номерам: 1, 2 ... (новые добавляются командой /account N)
    "accounts": {"1": dict(BLANK_ACCOUNT), "2": dict(BLANK_ACCOUNT)},
    # Кто сейчас собирает, а кто отправляет (/collector, /sender, /swap)
    "roles": {"collector": "1", "sender": "2"},
    "chat_source_groups": [],
    "chat_exclude_groups": [],
    "message": "Привет!",
    "text_agree1": "",
    "text_refusal": "",
    "text_question": "",
    "text_source": "",
    "instance_port": 47213,
    "max_messages": 100,
    "max_inactive_days": 7,
    "scan_messages_limit": 1000,
    "dm_reply_enabled": True,
    "delay_between_messages": 3,
    "auto_accept": True,          # автопринятие согласившихся в группу (/accept); группа берётся из ссылки в /text2
    "accept_user_id": 0,          # ID аккаунта, который принимает в первую очередь (/acceptor); 0 — любой с правами
}

# Команда -> (ключ, тип, подсказка)
SETTINGS = {
    "text": ("message", "str", "Текст первого сообщения"),
    "text2": ("text_agree1", "str", "Ответ на согласие («да», «давай», «го»)"),
    "text4": ("text_refusal", "str", "Ответ на отказ («нет», «не»).\nМожно несколько вариантов через строку «---»: на 1-й отказ уйдёт 1-й вариант, на 2-й — 2-й и т.д. Так бот упрашивает по очереди"),
    "text5": ("text_question", "str", "Ответ на вопрос и сомнение («зачем», «хз», «не знаю»).\nМожно несколько вариантов через строку «---»: на каждый следующий вопрос уйдёт следующий вариант"),
    "text6": ("text_source", "str", "Ответ на «откуда у тебя мой контакт?».\nМожно подставлять {chat} — название чата, где нашли человека, и {name} — его имя"),
    "limit": ("max_messages", "int", "Лимит сообщений за запуск (число > 0)"),
    "active": ("max_inactive_days", "int0", "Макс. давность последней активности, дней (0 — не проверять)"),
    "scan": ("scan_messages_limit", "int", "Сколько последних сообщений сканировать в каждом чате"),
    "delay": ("delay_between_messages", "float0", "Пауза между отправками, сек"),
    "acceptor": ("accept_user_id", "int0", "ID аккаунта (его user ID, не номер 1/2/3), которым принимать заявки в первую очередь. 0 — любой подключённый аккаунт с правами админа в группе"),
}

# Временные сбои Telegram («internal issues», таймауты) — такие запросы повторяем.
TRANSIENT = tuple(
    getattr(tl_errors, name)
    for name in ("ServerError", "RpcMcgetFailError", "RpcCallFailError", "TimedOutError")
    if hasattr(tl_errors, name)
) + (asyncio.TimeoutError, ConnectionError)


# ---------------------------------------------------------
# Конфиг (с кэшем по времени изменения файла) и файлы
# ---------------------------------------------------------

_cfg_cache = {"stamp": None, "data": None}


def save_config(config):
    tmp = CONFIG_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(config, f, ensure_ascii=False, indent=4)
    os.replace(tmp, CONFIG_FILE)
    _cfg_cache["stamp"] = None


def load_config():
    try:
        stamp = os.stat(CONFIG_FILE).st_mtime_ns
    except OSError:
        stamp = None
    if stamp is not None and stamp == _cfg_cache["stamp"]:
        return copy.deepcopy(_cfg_cache["data"])

    config = copy.deepcopy(DEFAULT_CONFIG)
    if stamp is None:
        save_config(config)
        return config
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError):
        data = {}
    if isinstance(data, dict):
        accounts = data.pop("accounts", None)
        roles = data.pop("roles", None)
        legacy = {k: data.pop(k) for k in ("api_id", "api_hash", "phone") if k in data}
        config.update(data)
        if isinstance(accounts, dict):
            if "collector" in accounts or "sender" in accounts:
                # Старый формат (collector/sender) -> аккаунты 1 и 2
                accounts = {"1": accounts.get("collector", {}), "2": accounts.get("sender", {})}
                roles = roles or {"collector": "1", "sender": "2"}
            for slot, acc in accounts.items():
                if isinstance(acc, dict):
                    config["accounts"].setdefault(str(slot), dict(BLANK_ACCOUNT)).update(acc)
        elif legacy:
            config["accounts"]["1"].update(legacy)  # самый старый формат -> аккаунт 1
        if isinstance(roles, dict):
            for role in ("collector", "sender"):
                if str(roles.get(role)) in config["accounts"]:
                    config["roles"][role] = str(roles[role])
    _cfg_cache.update(stamp=stamp, data=config)
    return copy.deepcopy(config)


def read_processed():
    if not os.path.exists(PROCESSED_FILE):
        return set()
    with open(PROCESSED_FILE, "r", encoding="utf-8") as f:
        return {line.strip().lstrip("@").lower() for line in f if line.strip()}


def append_processed(username):
    with open(PROCESSED_FILE, "a", encoding="utf-8") as f:
        f.write(username.strip().lstrip("@").lower() + "\n")


admin_ids = {int(x) for x in load_config().get("admin_ids", []) if str(x).lstrip("-").isdigit()}

dp = Dispatcher()
router = Router()

login = {}        # uid -> {"slot", "step", "client", "phone", "hash"}
waiting = {}      # uid -> "set:<cmd>" | "account:<slot>"
run_locks = set()


# ---------------------------------------------------------
# Общие помощники: текст, сообщения, повторы
# ---------------------------------------------------------

def esc(value):
    return html.escape(str(value), quote=False)


def strip_tags(text):
    return html.unescape(re.sub(r"</?[a-z][^>]*>", "", text))


def short(text, n=40):
    text = " ".join(str(text).split())
    return text if len(text) <= n else text[: n - 1] + "…"


async def tg_call(fn, tries=5):
    """Вызов Bot API с повторами: ждёт при лимитах (retry_after) и сетевых сбоях. Прочие ошибки пробрасывает."""
    for attempt in range(tries):
        try:
            return await fn()
        except TelegramRetryAfter as e:
            await asyncio.sleep(float(e.retry_after) + 0.5)
        except (TelegramNetworkError, TelegramServerError, asyncio.TimeoutError, ConnectionError):
            if attempt == tries - 1:
                raise
            await asyncio.sleep(1.5 * (attempt + 1))
    return await fn()


async def notify(message, text, kb=None):
    """Отправка с HTML. Ждёт при лимитах и сетевых сбоях, при кривой разметке шлёт простым текстом, не падает."""
    plain = False
    for _ in range(3):
        try:
            return await tg_call(lambda: message.answer(strip_tags(text) if plain else text,
                                                        parse_mode=None if plain else "HTML", reply_markup=kb))
        except Exception as e:
            if "parse entities" in str(e).lower() and not plain:
                plain = True
                continue
            log.warning("notify: %s: %s", type(e).__name__, e)
            return None
    return None


async def edit(msg, text, kb=None):
    try:
        await tg_call(lambda: msg.edit_text(text, parse_mode="HTML", reply_markup=kb))
    except Exception as e:
        low = str(e).lower()
        if "not modified" in low:
            return
        if "parse entities" in low:
            with suppress(Exception):
                await tg_call(lambda: msg.edit_text(strip_tags(text), parse_mode=None, reply_markup=kb))
        else:
            log.warning("edit: %s: %s", type(e).__name__, e)


async def retry(fn, tries=4):
    """Повторяет запрос при временной ошибке Telegram (паузы 2, 4, 6 сек)."""
    for attempt in range(tries):
        try:
            return await fn()
        except TRANSIENT:
            if attempt == tries - 1:
                raise
            await asyncio.sleep(2 * (attempt + 1))


def admin(fn):
    """Только для админов бота + единая обработка ошибок (бот не молчит, если что-то упало)."""
    @wraps(fn)
    async def wrapper(event):
        user = getattr(event, "from_user", None)
        msg = event.message if isinstance(event, CallbackQuery) else event
        if not user or user.id not in admin_ids:
            if isinstance(event, CallbackQuery):
                await event.answer("Нет доступа", show_alert=True)
            else:
                await notify(msg, "❌ Нет доступа")
            return
        try:
            await fn(event)
        except Exception as e:
            log.exception("ошибка в команде")
            await notify(msg, f"❌ <b>Ошибка:</b> {esc(type(e).__name__)}: {esc(e)}")
    return wrapper


def args_of(message):
    return message.text.split()[1:]


def slots_sorted(cfg):
    return sorted(cfg["accounts"], key=int)


def role_mark(cfg, slot):
    if slot == cfg["roles"]["collector"]:
        return " · 📥 сборщик"
    if slot == cfg["roles"]["sender"]:
        return " · 📤 отправитель"
    return ""


def normalize_phone(value):
    value = re.sub(r"[\s\-()]", "", value)
    return value if re.fullmatch(r"\+[1-9]\d{7,14}", value) else None


def normalize_code(value):
    """Код вводится как ###.## — возвращаем цифры без точки."""
    value = value.strip().replace(" ", "")
    return value.replace(".", "") if re.fullmatch(r"\d{3}\.\d{2}", value) else None


def normalize_ref(token):
    """Чат из ввода: ID, id канала без -100, @юзернейм, ссылка t.me/..., ссылка на сообщение t.me/c/ID/N."""
    t = token.strip().strip(",;")
    t = re.sub(r"^(https?://)?(www\.)?t\.me/", "", t).split("?")[0]
    if re.fullmatch(r"c/\d+(/\d+)?", t):
        t = "-100" + t.split("/")[1]
    if not re.fullmatch(r"-?\d+", t):
        t = t.lstrip("@").split("/")[0]
    elif re.fullmatch(r"\d{9,}", t):
        t = "-100" + t
    return t


def parse_refs(text):
    seen, out = set(), []
    for tok in re.split(r"[\s,;]+", text.strip()):
        ref = normalize_ref(tok)
        if ref and ref not in seen:
            seen.add(ref)
            out.append(ref)
    return out


# ---------------------------------------------------------
# Аккаунты Telegram: сессии, подключение, проверка входа
# ---------------------------------------------------------

def session_file(slot):
    # Совместимость со старыми сессиями: 1 = my_session, 2 = sender_session
    return {"1": "my_session", "2": "sender_session"}.get(slot, f"session_{slot}")


def session_exists(slot):
    return os.path.exists(session_file(slot) + ".session")


def create_client(slot):
    acc = load_config()["accounts"].get(slot)
    api_id = int((acc or {}).get("api_id") or 0)
    api_hash = str((acc or {}).get("api_hash") or "").strip()
    if not api_id or not api_hash:
        raise ValueError(f"Аккаунт {slot}: не заданы API_ID/API_HASH (/account {slot})")
    # connection_retries: Telethon сам переподключается ~40 сек, потом сдаётся — дальше подхватывает watchdog_loop
    return TelegramClient(session_file(slot), api_id, api_hash,
                          connection_retries=20, retry_delay=2, auto_reconnect=True,
                          request_retries=5, timeout=15)


async def is_authorized(slot, quick=False):
    """Рабочая ли сессия аккаунта. quick=True — только наличие файла сессии."""
    if not session_exists(slot):
        return False
    if quick:
        return True
    try:
        return (await get_client(slot)) is not None
    except Exception:
        return False


@asynccontextmanager
async def account_client(slot):
    """Клиент аккаунта из пула (подключение общее, закрывать не нужно)."""
    client = await get_client(slot)
    if client is None:
        raise RuntimeError("не авторизован")
    yield client


async def with_chat_access(ref, job):
    """Пробует аккаунты по очереди (сборщик, отправитель, остальные) и выполняет job(client, entity, slot)
    на первом, у которого есть доступ к чату. Возвращает (slot, результат)."""
    cfg = load_config()
    roles = cfg["roles"]
    order = [roles["collector"], roles["sender"]] + [s for s in slots_sorted(cfg) if s not in roles.values()]
    for slot in order:
        acc = cfg["accounts"].get(slot, {})
        if not session_exists(slot) or not acc.get("api_id") or not acc.get("api_hash"):
            continue
        try:
            client = await get_client(slot)
        except Exception:
            continue                      # аккаунт не подключился — пробуем следующий, а не падаем целиком
        if client is None:
            continue
        try:
            entity = await retry(lambda: resolve_chat(client, ref))
        except FloodWaitError:
            raise
        except (ValueError, RPCError):
            continue                      # у этого аккаунта нет доступа — пробуем следующий
        return slot, await job(client, entity, slot)
    raise RuntimeError("ни один аккаунт не имеет доступа к этому чату")


_dialogs_at = {}


async def load_dialogs(client):
    """Подтянуть список диалогов, чтобы Telethon узнал чаты. Не чаще раза в минуту на клиента и не дольше 2 минут
    (раньше на каждый неизвестный чат читались ВСЕ диалоги без ограничения по времени)."""
    key = id(client)
    if time.time() - _dialogs_at.get(key, 0) < 60:
        return
    _dialogs_at[key] = time.time()
    with suppress(asyncio.TimeoutError):
        await asyncio.wait_for(client.get_dialogs(limit=None), 120)


async def resolve_chat(client, value):
    ref = int(value) if re.fullmatch(r"-?\d+", str(value)) else value
    try:
        return await client.get_entity(ref)
    except ValueError:
        await load_dialogs(client)  # Telethon мог ещё не знать чат
        return await client.get_entity(ref)


# ---------------------------------------------------------
# Статус и отчёты: минимум сообщений
# ---------------------------------------------------------

@asynccontextmanager
async def working(message, text="⏳ Работаю…"):
    """Одно временное сообщение на время долгой операции — после неё оно удаляется."""
    msg = await notify(message, text)
    try:
        yield
    finally:
        if msg is not None:
            with suppress(Exception):
                await msg.delete()


class Outbox:
    """Очередь сообщений в чат: каждое уходит отдельным сообщением (1 человек = 1 сообщение),
    с паузой ~1 с, чтобы не упереться в лимиты Telegram, и не тормозит саму рассылку."""

    def __init__(self, message, gap=1.1):
        self.message, self.gap = message, gap
        self.queue = asyncio.Queue()
        self.task = asyncio.create_task(self._worker())

    async def _worker(self):
        while True:
            text = await self.queue.get()
            try:
                await notify(self.message, text)
            except Exception:
                pass
            self.queue.task_done()
            await asyncio.sleep(self.gap)

    def add(self, line):
        self.queue.put_nowait(line)

    async def close(self, timeout=120):
        with suppress(Exception):
            await asyncio.wait_for(self.queue.join(), timeout)
        self.task.cancel()


# ---------------------------------------------------------
# Операции и остановка (/stop)
# ---------------------------------------------------------

class StopRun(BaseException):
    """Операцию попросили остановить (BaseException — чтобы её не поймали обычные except Exception)."""


class Op:
    def __init__(self, name):
        self.name = name
        self.stop = False
        self.task = asyncio.current_task()
        self.timer = None


ops = set()
current_op = ContextVar("current_op", default=None)


def check_stop():
    op = current_op.get()
    if op is not None and op.stop:
        raise StopRun()


async def interruptible_sleep(sec):
    loop = asyncio.get_running_loop()
    end = loop.time() + sec
    while True:
        check_stop()
        left = end - loop.time()
        if left <= 0:
            return
        await asyncio.sleep(min(0.5, left))


def request_stop():
    """Мягкая остановка: операция остановится на ближайшем шаге. Если застряла — через 12 сек обрываем жёстко."""
    loop = asyncio.get_running_loop()
    for op in list(ops):
        op.stop = True
        if op.timer is None and op.task is not None:
            op.timer = loop.call_later(12, op.task.cancel)
    return len(ops)


async def run_op(message, name, fn, extra=None):
    """Выполняет fn() как останавливаемую операцию. extra() — текст с промежуточным итогом при остановке."""
    op = Op(name)
    ops.add(op)
    token = current_op.set(op)
    stopped = False
    try:
        await fn()
    except StopRun:
        stopped = True
    except asyncio.CancelledError:
        if not op.stop:
            raise
        stopped = True
    finally:
        if op.timer is not None:
            op.timer.cancel()
        ops.discard(op)
        current_op.reset(token)
    if stopped:
        tail = extra() if extra else ""
        await notify(message, f"⏹ <b>{esc(name)} остановлена</b>" + (f"\n{tail}" if tail else ""))


def operation(name):
    """Декоратор: команда становится останавливаемой операцией."""
    def deco(fn):
        @wraps(fn)
        async def wrapper(message):
            await run_op(message, name, lambda: fn(message))
        return wrapper
    return deco


# ---------------------------------------------------------
# Распознавание ответов в ЛС: согласие / отказ / вопрос / «не пиши»
# ---------------------------------------------------------

# Раскладка EN -> RU (если человек писал не переключив раскладку: «lfdfq» = «давай»)
EN_TO_RU = dict(zip("qwertyuiop[]asdfghjkl;'zxcvbnm,.", "йцукенгшщзхъфывапролджэячсмитьбю"))
# Латинские буквы, похожие на кириллицу (смешанные слова: «дaвай»)
HOMOGLYPHS = {"a": "а", "e": "е", "o": "о", "c": "с", "p": "р", "x": "х", "y": "у",
              "k": "к", "m": "м", "t": "т", "b": "в", "h": "н"}
# Латиница и транслит -> русские слова
LATIN_MAP = {
    "yes": "да", "yep": "да", "yeah": "да", "yea": "да", "yup": "да", "da": "да", "ok": "ок", "okay": "окей",
    "okey": "окей", "sure": "конечно", "go": "го", "goo": "го", "gou": "го", "davai": "давай", "davay": "давай",
    "ladno": "ладно", "hochu": "хочу", "khochu": "хочу", "konechno": "конечно", "pogнали": "погнали",
    "net": "нет", "ne": "не", "no": "нет", "nope": "нет", "nah": "нет", "neа": "неа",
    "hi": "привет", "hello": "привет", "hey": "привет", "yo": "привет", "privet": "привет", "hola": "привет",
    "stop": "стоп", "spam": "спам", "report": "репорт", "block": "блок", "ban": "бан",
    "why": "зачем", "what": "что", "who": "кто", "how": "как", "where": "где",
}


def norm_token(tok):
    tok = tok.lower().replace("ё", "е")
    if tok in LATIN_MAP:
        return norm_token_ru(LATIN_MAP[tok])
    if re.fullmatch(r"[a-z\[\];',.]+", tok):                    # чистая латиница -> пробуем раскладку
        tok = "".join(EN_TO_RU.get(ch, ch) for ch in tok)
    elif re.search(r"[а-я]", tok):                                # смешанные алфавиты -> похожие буквы
        tok = "".join(HOMOGLYPHS.get(ch, ch) for ch in tok)
    return norm_token_ru(tok)


def norm_token_ru(tok):
    tok = re.sub(r"(.)\1+", r"\1", tok)                           # дааа -> да, хачуу -> хачу
    tok = re.sub(r"^(да){2,}$", "да", tok)                        # дада -> да
    tok = re.sub(r"^(не){2,}$", "не", tok)                        # нененене -> не
    return tok


def tokenize(text):
    return [norm_token(t) for t in re.findall(r"[a-zа-яё0-9\[\];',.]+|\+", text.lower().replace("’", "'")) if t.strip(",.;'[]")]


def _ph(phrases):
    return tuple(" ".join(norm_token_ru(t.replace("ё", "е")) for t in p.split()) for p in phrases)


def _words(words):
    return {norm_token_ru(w.replace("ё", "е")) for w in words}


# --- согласие ---
AGREE_WORDS = _words("""да ага агась угу конечно хочу хачу ок окей оке хорошо ладно давай давайте го погнали поехали можно
согласен согласна согласны готов готова жду интересно интересует вперед запросто базара нет бегу иду приду буду плюс +
гоу допустим заинтересован заинтересована""".split())
AGREE_PHRASES = _ph([
    "я хочу", "я хачу", "я в деле", "в деле", "я за", "тоже хочу", "хочу играть", "давай играть", "го играть",
    "го в мафию", "го мафия", "го в игру", "я из анонки", "из анонки", "конечно хочу", "да хочу", "ну да", "да да",
    "ага да", "угу да", "давай ссылку", "скинь ссылку", "кидай ссылку", "кидай", "скидывай", "дай ссылку", "где ссылка",
    "как попасть", "как зайти", "как вступить", "как присоединиться", "куда писать", "куда заходить", "где играть",
    "скинь", "скиньте", "кинь", "киньте", "пришли", "пришлите", "отправь", "отправьте", "покажи", "покажите",
    "присылай", "скинешь", "кинешь", "давай посмотрим", "давайте посмотрим", "скинь посмотрим", "дай посмотреть",
    "можно попробовать", "давай попробуем", "давайте попробуем", "я готов", "я готова", "я согласен", "я согласна",
    "я согласны", "с удовольствием", "с радостью", "почему бы и нет", "почему бы нет", "а почему бы и нет",
])
# «не против», «не откажусь», «нет вопросов» — отрицание, но по смыслу согласие. Проверяется ПЕРВЫМ.
AGREE_OVERRIDE = _ph([
    "не против", "не откажусь", "не откажусь не", "нет вопросов", "не вопрос", "не проблема", "нет проблем",
    "без проблем", "почему бы и нет", "почему бы нет", "а почему бы и нет", "не вижу проблем", "не возражаю",
])

# --- отказ ---
REFUSAL_WORDS = _words("""нет неа не ноу нах нахер нахуй хуй хер отстань отстаньте отвали отвалите хватит харе пас пасс
мимо против отказываюсь отказ забей забейте неинтересно никогда сука""".split())
REFUSAL_PHRASES = _ph([
    "не хочу", "не буду", "не надо", "не нужно", "не интересно", "не интересует", "не хочется", "не люблю",
    "не играю", "не по адресу", "не мое", "нет спасибо", "не спасибо", "спасибо нет", "нет уж", "ни за что",
    "не согласен", "не согласна", "не согласны", "не готов", "не готова", "не могу", "не до этого", "не до игр",
    "я против", "я не буду", "я не хочу", "не трогай", "мне не интересно", "мне это не надо", "мне не надо",
    "мне не нужно", "меня не интересует", "нет желания", "нет времени", "нет интереса", "нет настроения",
    "нет возможности", "не актуально", "уже не актуально", "не пойду", "не приду", "не вариант", "это не для меня",
    "пошел ты", "пошла ты", "иди ты", "иди на", "иди нах", "пошел нах", "пошли вы", "идите нах", "отвали от меня",
    "отстань от меня", "я отказываюсь", "нет не хочу", "нет не надо",
])
# После «не» эти слова означают отказ: «не хочу», «не готов», «не буду»... (ловит и опечатки/формы)
NEG_TARGETS = _words("""хочу хачу буду надо нужно готов готова согласен согласна согласны интересно интересует хочется могу
играю люблю пойду приду актуально нуждаюсь собираюсь планирую""".split())

# --- «не пиши»: молчим и закрываем диалог, чтобы не получить жалобу ---
STOP_WORDS = _words("""спам спамер спамишь стоп блок блокирую заблокирую пожалуюсь репорт бан забаню отпишись отписаться
удали удалите мошенник мошенники развод лохотрон разводилово""".split())
STOP_PHRASES = _ph([
    "не пиши", "не пишите", "не пиши мне", "не пишите мне", "перестань писать", "перестаньте писать", "хватит писать",
    "оставь меня", "оставьте меня", "не пиши больше", "больше не пиши", "больше не пишите", "удали мой номер",
    "я пожалуюсь", "это спам",
])

# --- вопрос ---
QUESTION_WORDS = _words("""зачем почему чего что кто кого куда откуда когда сколько как где какой какая какое какие чем разве
правда серьезно реально подробнее правила объясни поясни расскажи""".split())
QUESTION_STARTERS = _words("""зачем почему что чего кто кого куда откуда когда сколько как где какой какая какое какие чем разве а""".split())
QUESTION_PHRASES = _ph([
    "что это", "что за", "что за игра", "что за мафия", "что там", "что там делать", "что делать", "что нужно делать",
    "что надо делать", "как это", "как играть", "как это работает", "в чем суть", "в чем смысл", "в смысле", "а что",
    "а как", "а зачем", "а почему", "кто ты", "ты кто", "кто это", "кто вы", "вы кто", "откуда ты", "откуда знаешь",
    "откуда номер", "это что", "это кто", "ты бот", "вы бот", "это бот", "это реклама", "реклама", "расскажи подробнее",
    "что просто", "а что это", "а как это", "а что там делать", "а что за мафия", "а что просто", "что взамен",
    "что мне за это", "а что мне", "а если", "а можно", "а сколько", "сколько платят", "что за приглашение",
])
# «не знаю», «не понял» — человек не отказывается, а просит пояснить
QUESTION_OVERRIDE = _ph(["не знаю", "не понял", "не поняла", "не понимаю", "не уверен", "не уверена", "сомневаюсь", "хз", "мб"])
# «позже», «потом», «не сейчас» — решения ещё нет: не отвечаем сами, бот только сообщит вам
DELAY_WORDS = _words("позже попозже потом завтра сегодня-потом".split())
DELAY_PHRASES = _ph(["не сейчас", "в другой раз", "как нибудь", "когда нибудь", "на днях", "на неделе", "после работы", "через час"])
AGREE_LEADERS = _words("давай давайте го погнали поехали скинь скиньте кинь киньте пришли пришлите покажи покажите".split())

# --- «откуда у тебя мой контакт?» — отдельный вопрос со своим ответом (/text6) ---
SOURCE_ME = _words("""меня мой мое моя мои моего моей контакт номер ник юз юзернейм акк аккаунт профиль телеграм тг""".split())
SOURCE_VERBS = _words("""знаешь знаете узнал узнала узнали взял взяла взяли нашел нашла нашли вышел вышла вышли достал достала
достали раздобыл раздобыла получил получила получили""".split())
GREETINGS = _words("""привет приветик приветствую здарова здравствуй здравствуйте хай хей ку салют алло але йоу добрый""".split())

AGREE_EMOJI = set("👍👌✅🔥💪🤝😁😊🙂😉❤🥰😍🤩👏🙌✌🤙🆗💯😎😀😃😄➕")
REFUSAL_EMOJI = set("👎❌🚫🖕😡🤬😠🙅✋➖")
STOP_EMOJI = set("🛑⛔")
QUESTION_EMOJI = set("❓❔🤔🧐🤨😳🤷")


_fuzzy_cache = {}


def _fuzzy(tok, words, thr):
    if len(tok) < 4:
        return False
    key = (tok, id(words), thr)        # наборы слов — модульные константы, их id стабилен
    res = _fuzzy_cache.get(key)
    if res is None:
        res = any(len(w) >= 4 and SequenceMatcher(None, tok, w).ratio() >= thr for w in words)
        if len(_fuzzy_cache) > 20000:
            _fuzzy_cache.clear()
        _fuzzy_cache[key] = res
    return res


def _has(padded, phrases):
    return any(f" {p} " in padded for p in phrases)


def is_source_question(toks):
    """«откуда у тебя мой контакт», «как ты меня нашёл», «где взял мой номер»..."""
    if _has(" " + " ".join(toks) + " ", SOURCE_PHRASES):
        return True
    has_me = any(t in SOURCE_ME for t in toks)
    has_verb = any(t in SOURCE_VERBS for t in toks)
    if "откуда" in toks and (has_me or has_verb or "ты" in toks or "вы" in toks):
        return True
    if "как" in toks and has_verb and has_me:
        return True
    if "где" in toks and has_verb and has_me:
        return True
    return False


# --- дополнения к спискам: больше живых формулировок ---
AGREE_WORDS |= _words("""норм нормально отлично супер класс круто кайф прикольно любопытно звучит попробую попробуем
посмотрю гляну зайду присоединюсь""".split())
AGREE_PHRASES += _ph([
    "ну давай", "ладно давай", "давай давай", "хорошо давай", "я не против", "почему нет", "а почему нет",
    "хочу в игру", "запиши меня", "добавь меня", "добавьте меня", "возьми меня", "возьмите меня", "пригласи",
    "пригласите", "приглашай", "приглашайте", "я с вами", "я с тобой", "можно с вами", "в игру",
])
REFUSAL_WORDS |= _words("обойдусь".split())
REFUSAL_PHRASES += _ph([
    "не катит", "не зайдет", "не зашло", "это не мое", "я пас", "не мой формат", "не стоит", "увы нет",
    "к сожалению нет", "к сожалению не", "нет благодарю", "извини не", "извините не", "не надо мне",
])
NEG_TARGETS |= _words("катит зайдет зашло стоит умею".split())
STOP_WORDS |= _words("жалоба жалобу пожаловался надоел надоели надоело прекрати прекратите перестань перестаньте".split())
STOP_PHRASES += _ph(["хватит спамить", "не спамь", "не спамьте", "не звони", "не звоните"])
QUESTION_WORDS |= _words("бесплатно платно подвох условия цена цены".split())
QUESTION_PHRASES += _ph([
    "о чем речь", "о чем это", "о чем", "про что", "про что это", "в чем прикол", "в чем дело", "в чем подвох",
    "где подвох", "что предлагаешь", "что предлагаете", "что за предложение", "какие правила", "какие условия",
    "это бесплатно", "а бесплатно", "сколько стоит", "есть подвох", "что за хрень", "что за ерунда",
])
# «это не развод?», «не спам?» — человек сомневается, а не просит отстать
QUESTION_OVERRIDE += _ph(["это не развод", "не развод", "не лохотрон", "не мошенник", "не мошенники", "это не спам",
                          "не спам", "не обман", "не мошенничество"])

# --- ещё формулировки (раунд 2) ---
LATIN_MAP.update({"fuck": "хуй", "fck": "хуй", "wtf": "хуй", "shit": "говно", "thanks": "спасибо", "thx": "спасибо",
                  "ty": "спасибо", "spasibo": "спасибо", "pls": "пожалуйста", "plz": "пожалуйста"})

AGREE_WORDS |= _words("""ссылку ссылка заинтересовало заинтересовал заинтересовалась обязательно разумеется безусловно
однозначно айда сойдет""".split())
AGREE_PHRASES += _ph([
    "ок го", "го попробуем", "давай попробуем", "ща зайду", "жду ссылку", "пиши ссылку", "присылай ссылку", "а давай",
    "можно ссылку", "было бы интересно", "звучит интересно", "хотелось бы", "я бы поиграл", "я бы поиграла",
    "могу попробовать", "ладно попробую", "окей давай", "да можно", "можно да", "ну можно", "ну ладно", "почему бы",
    "буду играть", "уже иду", "заходи", "давай ссылку", "где играем", "когда играем", "когда игра", "во сколько игра",
    "в какое время игра", "хочу в чат", "хочу к вам", "возьмите в чат", "добавьте в чат", "добавь в чат",
])
AGREE_OVERRIDE += _ph(["базара нет", "не откажусь", "не против поиграть", "нет возражений"])

REFUSAL_PHRASES += _ph([
    "да ну", "да ну нафиг", "ну нафиг", "да нет", "не интересуюсь", "не увлекаюсь", "мне неинтересно", "я не играю",
    "не по теме", "не туда", "ошиблись", "вы ошиблись", "ты ошибся", "ошибся номером", "не тот человек",
    "не туда попали", "не туда написал", "не ко мне", "не для меня", "без меня", "не нуждаюсь", "не требуется",
    "не собираюсь", "не вижу смысла", "смысла нет", "нет смысла", "не вижу интереса", "не мое", "мне это не нужно",
    "мне это неинтересно", "оставь себе", "сам играй", "сами играйте",
])
REFUSAL_WORDS |= _words("неинтересно отказ нафиг не_надо".split())

STOP_WORDS |= _words("""полиция полицию прокуратура прокуратуру суд судом заявление жалуюсь жаловаться заблокировал
заблокировала заблокировать отписка""".split())
STOP_PHRASES += _ph([
    "не пишите мне больше", "удалите мой номер", "забудь меня", "забудьте меня", "удали меня", "удалите меня",
    "отпишите меня", "в бан", "кидаю в спам", "подам жалобу", "напишу жалобу", "иду жаловаться", "в покое",
    "оставь в покое", "оставьте в покое", "забудь мой номер", "забудьте мой номер", "не беспокой", "не беспокойте",
    "не доставай", "не доставайте", "не донимай", "не донимайте", "в черный список", "в чс",
])

QUESTION_WORDS |= _words("че чо шо чем".split())
QUESTION_STARTERS |= _words("че чо шо".split())
QUESTION_PHRASES += _ph([
    "что за группа", "что за чат", "что за канал", "что за сообщество", "что за анонка", "что такое мафия",
    "что такое анонка", "это про что", "про что речь", "а ты кто", "ты вообще кто", "вы вообще кто", "с кем я говорю",
    "с кем говорю", "это вы кто", "кто пишет", "а вы кто", "вы откуда", "зачем тебе это", "что от меня надо",
    "что тебе надо", "что вам нужно", "что вам от меня надо", "чего ты хочешь", "чего вы хотите", "что хочешь",
    "что хотите", "ты по какому вопросу", "по какому поводу", "по какому вопросу", "что ты хочешь", "что именно",
    "какая игра", "какая мафия", "какой формат", "сколько человек", "сколько игроков", "сколько длится", "где проходит",
    "и что", "и че", "че там", "че за", "че это", "шо за", "что за люди", "что за админы", "что за режимы",
    "какие режимы", "какие роли", "а там что", "как у вас", "как там у вас", "да ладно", "да ты что", "не может быть",
])
QUESTION_OVERRIDE += _ph(["хз", "хз хз", "х3", "незнаю", "хм", "хмм", "ну хз", "мб да", "мб", "может быть", "наверное", "посмотрим"])

DELAY_WORDS |= _words("вечером утром выходные выходных занят занята занято днем".split())
DELAY_PHRASES += _ph([
    "сейчас не могу", "не могу сейчас", "я занят", "я занята", "занят сейчас", "потом отвечу", "отвечу позже",
    "напишу позже", "на выходных", "в выходные", "попозже отвечу", "позже отвечу", "подумаю", "я подумаю", "дай подумать",
])

# --- «откуда мой контакт» — больше формулировок ---
SOURCE_PHRASES = _ph([
    "кто дал мой", "кто тебе дал", "кто вам дал", "кто тебе сказал мой", "кто тебе слил", "кто слил", "кто вам слил",
    "в каком чате", "из какого чата", "из какой группы", "в какой группе", "из какого ты чата", "как вы получили мой",
    "как у вас мой", "почему у тебя мой", "почему у вас мой", "зачем тебе мой номер", "зачем вам мой", "откуда инфа",
    "откуда информация", "откуда данные", "откуда мои данные", "где взял мои", "где взяли мои", "где взяла мои",
    "как нашли меня", "как нашел меня", "как нашла меня", "как ты меня нашел", "как вы меня нашли", "кто дал контакт",
    "кто дал мой контакт", "кто дал мой ник", "кто дал мой юз", "откуда у вас", "откуда у тебя", "откуда у вас мой",
])
GREETINGS |= _words("доброе доброго здрасте здрасьте хэй хелло хеллоу приветики".split())
AGREE_EMOJI |= set("🥳🎉🤗😋😌🤭")
REFUSAL_EMOJI |= set("🙄😒😑😤🤦💩🤮🤡")
STOP_EMOJI |= set("🚷📵🔇")
QUESTION_EMOJI |= set("♂♀")

FILLERS = _words("а ну так слушай слышь эй блин короче типа вот и же".split())
ACK_WORDS = _words("спасибо благодарю понял поняла понятно ясно ясненько принял принято спс пасиб пасибо мерси благодарствую сенкс".split())
ACK_PAIR = ACK_WORDS | _words("ок окей оке хорошо ладно угу".split())

# слабые «да/ок/норм» — согласие, только если в сообщении почти ничего больше нет («да», «ну да», «ок норм»).
AGREE_WEAK = _words("""да ага агась угу ок окей оке норм нормально хорошо ладно можно интересно допустим плюс + конечно
отлично супер класс круто кайф прикольно любопытно звучит""".split())
AGREE_STRONG = AGREE_WORDS - AGREE_WEAK

# --- мат и оскорбления: на них не отвечаем и не уговариваем, диалог закрывается ---
CURSE_PREFIX = ("хуй", "хуе", "хуя", "хуи", "хую", "пизд", "пезд", "бля", "мудак", "мудил", "мудо", "долбо", "дебил",
                "идиот", "урод", "тупой", "тупая", "тупое", "пидор", "пидр", "пидар", "гандон", "говн", "залуп", "чмо",
                "ублюд", "шлюх", "мраз", "тварь", "твари", "падл", "гнид", "дерьм", "жоп", "заеб", "отъеб", "сука",
                "суки", "суку", "сучк", "сучар", "нахуй", "нахер", "нахрен", "нафиг", "нафик", "похуй", "похер")
CURSE_EXACT = {"нах", "хер", "хрен", "бл", "сук"}
DIRECTED = {"нах", "нахуй", "нахер", "нахрен", "нафиг", "нафик", "хуй", "хер", "хрен", "похуй", "похер"}
GO_AWAY = {"иди", "идите", "пошел", "пошла", "пошли", "катись", "катитесь"}


def is_curse(tok):
    return tok in CURSE_EXACT or tok.startswith(CURSE_PREFIX) or re.match(r"^еб[аулнеиыот]", tok) is not None


def classify_clause(toks):
    if not toks:
        return None
    padded = " " + " ".join(toks) + " "
    core = list(toks)
    while core and core[0] in FILLERS:   # «а давай», «ну да», «слушай, а что это» — вводные слова не считаем
        core.pop(0)
    if not core:
        return None
    n = len(core)

    if _has(padded, AGREE_OVERRIDE):
        return "agreement"

    # мат: «Да ебанный рот», «А ты нахер пойти не хочешь?» — не согласие и не повод уговаривать
    if any(is_curse(t) for t in toks):
        strong = any(t in AGREE_STRONG for t in core)
        hostile = (any(t in DIRECTED or t in GO_AWAY or t in ("не", "нет") for t in toks)
                   or _has(padded, REFUSAL_PHRASES))
        return "agreement" if (strong and not hostile) else "stop"

    if _has(padded, QUESTION_OVERRIDE):
        return "question"
    if any(t in STOP_WORDS for t in toks) or _has(padded, STOP_PHRASES):
        return "stop"

    # отказ: фразы, «не + хочу/готов/буду...», короткие «нет/не/неа»
    if _has(padded, REFUSAL_PHRASES):
        return "refusal"
    if any(t == "не" and i + 1 < len(toks) and (toks[i + 1] in NEG_TARGETS or _fuzzy(toks[i + 1], NEG_TARGETS, 0.8))
           for i, t in enumerate(toks)):
        return "refusal"
    if n <= 3 and any(t in REFUSAL_WORDS for t in core):
        return "refusal"
    if n == 1 and core[0] not in AGREE_WORDS and not _fuzzy(core[0], AGREE_WORDS, 0.95) \
            and _fuzzy(core[0], REFUSAL_WORDS, 0.78):
        return "refusal"

    # вопрос / интерес
    if is_source_question(toks):
        return "source"
    if core[0] in AGREE_LEADERS:             # «давай посмотрим что там» — начало с согласия важнее
        return "agreement"
    if _has(padded, QUESTION_PHRASES):
        return "question"
    if _has(padded, AGREE_PHRASES):          # «как попасть», «где играть» — это интерес, а не вопрос
        return "agreement"
    if n <= 7 and core[0] in QUESTION_STARTERS:
        return "question"
    if n == 1 and (core[0] in QUESTION_WORDS or _fuzzy(core[0], QUESTION_WORDS, 0.82)):
        return "question"

    # «спасибо», «ок понял» — вежливое подтверждение
    if n <= 3 and all(t in ACK_PAIR for t in core) and any(t in ACK_WORDS for t in core):
        return "ack"

    # согласие: сильные слова («хочу», «давай», «го») — почти всегда; слабые («да», «ок») — только в коротких репликах
    if n <= 5 and any(t in AGREE_STRONG for t in core):
        return "agreement"
    if n <= 2 and any(t in AGREE_WEAK for t in core):
        return "agreement"
    if n <= 4 and any(t in AGREE_WEAK for t in core) and all(t in AGREE_WEAK or t in AGREE_STRONG or t in FILLERS for t in core):
        return "agreement"
    if n <= 3 and any(_fuzzy(t, AGREE_STRONG, 0.8) for t in core):
        return "agreement"

    # приветствие в ответ — человек вышел на связь, объясняем (как на вопрос)
    if n <= 3 and core[0] in GREETINGS:
        return "question"
    return None


def detect_trigger(text):
    """'agreement' | 'refusal' | 'question' | 'source' | 'stop' | 'ack' | None.
    Сообщение режется на части по знакам препинания; побеждает последняя решающая часть
    («не знаю, давай» -> согласие, «давай, но нет» -> отказ). «stop» важнее всего,
    «ack» («спасибо») — только если ничего другого в сообщении нет."""
    raw = (text or "").strip()
    if not raw:
        return None
    compact = re.sub(r"[\s\u200d\ufe0f]", "", raw)
    if compact and all(ch in AGREE_EMOJI or ch == "+" for ch in compact):
        return "agreement"
    if compact and all(ch in STOP_EMOJI for ch in compact):
        return "stop"
    if compact and all(ch in REFUSAL_EMOJI or ch == "-" for ch in compact):
        return "refusal"
    if compact and all(ch in QUESTION_EMOJI or ch == "?" for ch in compact):
        return "question"

    lowered = raw.lower()
    clauses = re.split(r"[.,!?;:\n]+|\s[-—–]\s|\sно\s", lowered)
    all_tokens = tokenize(lowered)
    padded_all = " " + " ".join(all_tokens) + " "
    if len(all_tokens) > 30:  # длинное сообщение — это разговор, а не ответ; реагируем только на «не пиши»
        return "stop" if (any(t in STOP_WORDS for t in all_tokens) or _has(padded_all, STOP_PHRASES)) else None

    result, ack = None, False
    for clause in clauses:
        kind = classify_clause(tokenize(clause))
        if kind == "stop":
            return "stop"
        if kind == "ack":
            ack = True
        elif kind:
            result = kind
    if result in ("agreement", "refusal") and (any(t in DELAY_WORDS for t in all_tokens) or _has(padded_all, DELAY_PHRASES)):
        return None   # «ладно, потом» / «давай позже» — решения нет, пусть решит человек
    if result is None and ack:
        return "ack"
    if result is None and "?" in raw and len(all_tokens) <= 12:
        result = "question"   # короткая реплика с вопросительным знаком
    return result


detect_trigger = lru_cache(maxsize=4096)(detect_trigger)   # одинаковые реплики («да», «нет») разбираются один раз


# Первое сообщение от человека, которому бот не писал (например «я из анонки»)
OPENER_WORDS = _words("го гоу мафия мафа".split())
OPENER_PHRASES = _ph(["по поводу мафии", "по поводу игры", "по поводу анонки", "насчет мафии", "насчет игры",
                      "хочу в анонку", "возьмите в игру", "возьмите в мафию", "возьми в игру", "я на мафию", "я на игру",
                      "мафия когда", "когда мафия", "где мафия", "где играем", "го на мафию", "го на игру", "го мафка",
                      "я по мафии", "я по объявлению", "по объявлению", "я по рекламе", "я на анонку", "я в мафию",
                      "я из анонки", "из анонки", "анонка", "го мафия", "го в мафию", "го играть", "го в игру",
                      "го в мафу", "го мафу", "го маф", "хочу в мафию", "хочу играть в мафию"])


def is_opener(text):
    toks = tokenize(text)
    if not toks or len(toks) > 6:
        return False
    if any(t in ("не", "нет") or is_curse(t) for t in toks):   # «не хочу в мафию» — это не приглашение
        return False
    padded = " " + " ".join(toks) + " "
    return _has(padded, OPENER_PHRASES) or (len(toks) == 1 and toks[0] in OPENER_WORDS)


# ---------------------------------------------------------
# Пул аккаунтов: все авторизованные аккаунты подключены постоянно и слушают ЛС
# ---------------------------------------------------------

pool = {}                    # slot -> подключённый TelegramClient
dm_states = {}               # "slot:user_id" -> text1 | text4 | text5 | text5x2 | text6 | text6x2 | done | manual
dm_meta = {}                 # "slot:user_id" -> {"chat", "ref", "name"} — откуда взяли человека
reply_map = {}               # "chat_id:message_id" уведомления -> "slot:user_id" (ответ из бота реплаем)
acc_info = {}                # slot -> {"id", "name", "username", "phone"} (кто это за аккаунт)
spam_state = {}              # slot -> результат проверки @SpamBot
chat_name_cache = {}         # ref чата -> название (для красивых списков)
recent_replies = {}          # (chat_id, hash текста) -> время: защита от дубля одного и того же текста
bot_ref = {"bot": None}
seen_msgs = {}               # "slot:chat:msg" -> ts — какие входящие уже обработаны (нет дублей после catch_up/перезапуска)
dead_slots = set()           # аккаунты с «мёртвой» сессией (уже сообщили админам)
warned = {}                  # ключ предупреждения -> время (чтобы не спамить)
bg_tasks = set()


def spawn(coro):
    """Фоновая задача со ссылкой (иначе сборщик мусора может её убить)."""
    t = asyncio.create_task(coro)
    bg_tasks.add(t)
    t.add_done_callback(bg_tasks.discard)
    return t

TRIGGER_LABELS = {"agreement": "✅ согласие", "refusal": "❌ отказ", "question": "❓ вопрос",
                  "source": "🔎 «откуда контакт?»", "stop": "🛑 просит не писать"}


_save_pending = {}
_save_timer = {"h": None}


def load_json(path, default):
    pending = _save_pending.get(path)          # ещё не записанные изменения важнее файла
    if pending is not None and isinstance(pending, type(default)):
        return pending
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, type(default)) else default
    except (OSError, json.JSONDecodeError):
        return default


def _write_json(path, data):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    os.replace(tmp, path)


def flush_saves():
    _save_timer["h"] = None
    for path in list(_save_pending):
        data = _save_pending.pop(path)
        try:
            _write_json(path, data)
        except Exception:
            log.exception("не удалось записать %s", path)


def save_json(path, data):
    """Запись откладывается на ~0.7 с: серия изменений = одна запись на диск, цикл событий не тормозит
    (раньше каждое изменение состояния целиком переписывало файл прямо в обработчике)."""
    _save_pending[path] = data
    if _save_timer["h"] is not None:
        return
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        flush_saves()
        return
    _save_timer["h"] = loop.call_later(0.7, flush_saves)


def load_state():
    global dm_states, dm_meta, reply_map, acc_info, spam_state, chat_name_cache, seen_msgs
    dm_states = load_json(DM_STATES_FILE, {})
    dm_meta = load_json(DM_META_FILE, {})
    reply_map = load_json(REPLY_MAP_FILE, {})
    acc_info = load_json(ACC_INFO_FILE, {})
    spam_state = load_json(SPAM_FILE, {})
    chat_name_cache = load_json(CHAT_NAMES_FILE, {})
    seen_msgs = load_json(SEEN_FILE, {})
    rebuild_dm_index()


def remember_chat(ref, name):
    if name and chat_name_cache.get(str(ref)) != name:
        chat_name_cache[str(ref)] = name
        save_json(CHAT_NAMES_FILE, chat_name_cache)


# ---- кто есть кто: понятные подписи аккаунтов ----

def title(slot):
    """Подпись аккаунта для сообщений: «Акк 6 · Иван (@ivan)». Уже экранирована для HTML."""
    info = acc_info.get(slot) or {}
    bits = []
    if info.get("name"):
        bits.append(esc(info["name"]))
    if info.get("username"):
        bits.append("@" + esc(info["username"]))
    if not bits:
        phone = (load_config()["accounts"].get(slot) or {}).get("phone")
        if phone:
            bits.append(esc(phone))
    return f"Акк {slot}" + (" · " + " ".join(bits) if bits else "")


# ---- состояния диалогов ----

dm_by_uid = {}        # uid -> {slot: state} — быстрый поиск по человеку (раньше каждый раз перебирался весь словарь)
dm_locks = {}         # (slot, uid) -> Lock: сообщения одного человека обрабатываются по очереди, а не теряются


def _split_key(k):
    slot, _, uid = k.rpartition(":")      # "slot:uid" -> (slot, uid); старый формат "uid" -> ("?", uid)
    return slot or "?", uid


def rebuild_dm_index():
    dm_by_uid.clear()
    for k, v in dm_states.items():
        slot, uid = _split_key(k)
        dm_by_uid.setdefault(uid, {})[slot] = v


def dm_lock(slot, uid):
    key = (str(slot), uid)
    lock = dm_locks.get(key)
    if lock is None:
        if len(dm_locks) > 2000:
            for k in [k for k, lk in dm_locks.items() if not lk.locked()]:
                dm_locks.pop(k, None)
        lock = dm_locks[key] = asyncio.Lock()
    return lock


def mark_seen(key):
    """True — сообщение новое; False — уже обрабатывали (повторная доставка после catch_up или перезапуска)."""
    if key in seen_msgs:
        return False
    seen_msgs[key] = int(time.time())
    while len(seen_msgs) > 3000:
        seen_msgs.pop(next(iter(seen_msgs)))
    save_json(SEEN_FILE, seen_msgs)
    return True


# старые записи (без номера аккаунта) тоже учитываются
def dm_get(slot, user_id):
    return dm_states.get(f"{slot}:{user_id}") or dm_states.get(str(user_id))


def set_dm_state(slot, user_id, state):
    dm_states[f"{slot}:{user_id}"] = state
    dm_by_uid.setdefault(str(user_id), {})[str(slot)] = state
    save_json(DM_STATES_FILE, dm_states)


def _drop_dm_state(slot, user_id):
    dm_states.pop(f"{slot}:{user_id}", None)
    row = dm_by_uid.get(str(user_id))
    if row is not None:
        row.pop(str(slot), None)
        if not row:
            dm_by_uid.pop(str(user_id), None)


def unset_dm_state(slot, user_id):
    _drop_dm_state(slot, user_id)
    dm_meta.pop(f"{slot}:{user_id}", None)
    save_json(DM_STATES_FILE, dm_states)
    save_json(DM_META_FILE, dm_meta)


def restore_dm_state(slot, user_id, prev):
    """Вернуть прежнее состояние диалога (если ответ не удалось доставить)."""
    if prev:
        set_dm_state(slot, user_id, prev)
    else:
        _drop_dm_state(slot, user_id)
        save_json(DM_STATES_FILE, dm_states)


def set_dm_meta(slot, user_id, meta):
    dm_meta[f"{slot}:{user_id}"] = meta
    save_json(DM_META_FILE, dm_meta)


def dm_meta_get(slot, user_id):
    return dm_meta.get(f"{slot}:{user_id}") or {}


def dm_any(user_id):
    """Состояния диалога этого человека по всем аккаунтам: [(slot, state)]."""
    return list(dm_by_uid.get(str(user_id), {}).items())


def messaged_ids():
    return {int(u) for u in dm_by_uid if u.lstrip("-").isdigit()}


def remove_processed(username):
    name = username.strip().lstrip("@").lower()
    with suppress(Exception):
        lines = [ln for ln in open(PROCESSED_FILE, "r", encoding="utf-8").read().splitlines() if ln.strip().lstrip("@").lower() != name]
        with open(PROCESSED_FILE, "w", encoding="utf-8") as f:
            f.write("".join(ln + "\n" for ln in lines))


def fmt_text(text, meta=None, user=None):
    """Подстановки в текстах: {chat} — чат, где нашли человека; {name} — его имя."""
    meta = meta or {}
    first = (getattr(user, "first_name", None) or meta.get("name") or "").strip()
    return (str(text or "").replace("{chat}", meta.get("chat") or "одном из чатов").replace("{name}", first)).strip()


# ---- общение с админом: сообщаем только о непонятном, ответ — реплаем ----

def trim_reply_map():
    while len(reply_map) > 1000:
        reply_map.pop(next(iter(reply_map)))
    save_json(REPLY_MAP_FILE, reply_map)


async def notify_admins(text, kb=None, key=None):
    """Сообщить админам бота о событии в ЛС. key=(slot, uid) — чтобы можно было ответить реплаем.
    Ждёт при лимитах Telegram и повторяет при сетевых сбоях (раньше уведомление просто терялось)."""
    bot = bot_ref["bot"]
    if bot is None:
        return
    for aid in admin_ids:
        sent = None
        for plain in (False, True):
            try:
                sent = await tg_call(lambda: bot.send_message(
                    aid, strip_tags(text) if plain else text,
                    parse_mode=None if plain else "HTML", reply_markup=kb))
                break
            except Exception as e:
                if not plain and "parse entities" in str(e).lower():
                    continue
                log.warning("notify_admins %s: %s: %s", aid, type(e).__name__, e)
                break
        if sent is None:
            continue
        if key is not None and getattr(sent, "message_id", None):
            reply_map[f"{aid}:{sent.message_id}"] = f"{key[0]}:{key[1]}"
    if key is not None:
        trim_reply_map()


async def warn_once(key, text, every=3600):
    """Предупреждение админам — не чаще раза в `every` секунд."""
    if time.time() - warned.get(key, 0) > every:
        warned[key] = time.time()
        await notify_admins(text)


async def dm_send(slot, user_id, text):
    """Отправить сообщение человеку с нужного аккаунта (из бота: кнопки, ответы)."""
    client = await get_client(slot)
    if client is None:
        raise RuntimeError(f"{title(slot)} не авторизован")
    peer = await client.get_input_entity(int(user_id))
    await client.send_message(peer, text)


# ---- ответы в ЛС ----

async def human_pause(event, lo=1.5, hi=4.0):
    """Пауза с «печатает…», как у живого человека."""
    delay = random.uniform(lo, hi)
    try:
        async with event.client.action(event.chat_id, "typing"):
            await asyncio.sleep(delay)
    except Exception:
        await asyncio.sleep(delay)


async def reply_text(event, text, meta=None, user=None):
    """-> 'ok' | 'dup' (такой же текст уже уходил) | 'empty' | 'error'. При лимитах и сбоях связи повторяет отправку."""
    text = fmt_text(text, meta, user)
    if not text:
        return "empty"
    now = time.time()
    if len(recent_replies) > 2000:
        for k in [k for k, t in recent_replies.items() if now - t > 600]:
            recent_replies.pop(k, None)
    # защита от дубля: тот же текст тому же человеку не чаще раза в 10 минут
    key = (event.chat_id, hash(text))
    if now - recent_replies.get(key, 0) < 600:
        return "dup"
    recent_replies[key] = now
    await human_pause(event)
    for attempt in range(3):
        try:
            await event.respond(text)
            return "ok"
        except FloodWaitError as e:
            if e.seconds > 90 or attempt == 2:
                break
            await asyncio.sleep(e.seconds + 1)
        except TRANSIENT:
            await asyncio.sleep(2 * (attempt + 1))
        except Exception:
            log.exception("не удалось ответить в ЛС")
            break
    recent_replies.pop(key, None)
    return "error"


def text_variants(text):
    """Текст можно писать в нескольких вариантах — разделяйте строкой «---»."""
    return [p.strip() for p in re.split(r"\n\s*-{3,}\s*\n", str(text or "")) if p.strip()]


def ladder_count(state, base):
    """Сколько раз уже отвечали на этот тип реплик: text5 -> 1, text5x2 -> 2 ..."""
    if state == base:
        return 1
    m = re.fullmatch(base + r"x(\d+)", state or "")
    return int(m.group(1)) if m else 0


def ladder_state(base, k):
    return base if k <= 1 else f"{base}x{k}"


async def handle_dm(event, cfg, slot, user, state, trigger, enabled):
    """Сценарий ответов. Состояние обновляется ДО отправки (чтобы не было дублей), а если отправка не удалась —
    возвращается обратно (чтобы вариант текста не «сгорал» зря).
    Отказ и вопрос: на каждое следующее сообщение уходит следующий вариант текста (уговариваем по очереди).
    Возвращает (заметка, нужно_ли_сообщить_админу): сообщаем только если бот не смог ответить сам."""
    uid = user.id
    meta = dm_meta_get(slot, uid)
    prev = None if state == "new" else state
    if trigger == "stop":
        set_dm_state(slot, uid, "done")
        return "", False
    if not enabled:
        return "автоответы выключены", True

    async def ladder(base, key, fallback_key=None):
        """Следующий вариант текста -> 'ok' | 'over' (варианты кончились) | 'dup' | 'empty' | 'error'."""
        variants = text_variants(cfg.get(key) or (cfg.get(fallback_key) if fallback_key else ""))
        k = ladder_count(state, base) + 1
        if k > len(variants):
            return "over"
        set_dm_state(slot, uid, ladder_state(base, k))
        res = await reply_text(event, variants[k - 1], meta, user)
        if res == "ok":
            stats_add(slot, "reply")
        elif res == "error":
            restore_dm_state(slot, uid, prev)
            stats_add(slot, "failed")
        return res

    def not_sent(res, what):
        if res == "error":
            return "не удалось отправить ответ (сбой связи или лимиты Telegram)", True
        return f"{what} пуст или это дубль", True

    if trigger == "refusal":
        res = await ladder("text4", "text_refusal")
        if res == "over":             # уговорили сколько могли — закрываем диалог молча
            set_dm_state(slot, uid, "done")
            return "", False
        return ("", False) if res == "ok" else not_sent(res, "/text4")
    if trigger == "question":
        res = await ladder("text5", "text_question")
        if res == "over":
            return "вопросов больше, чем вариантов в /text5", True
        return ("", False) if res == "ok" else not_sent(res, "/text5")
    if trigger == "source":
        res = await ladder("text6", "text_source", "text_question")
        if res == "over":
            return "повторно спрашивает про контакт", True
        return ("", False) if res == "ok" else not_sent(res, "/text6")
    # согласие: сначала фиксируем состояние, потом шлём (одинаковые тексты второй раз не уйдут)
    set_dm_state(slot, uid, "done")
    variants = text_variants(cfg.get("text_agree1"))
    res = await reply_text(event, variants[0] if variants else "", meta, user)
    if res in ("ok", "dup"):
        if res == "ok":
            stats_add(slot, "reply")
        agreed_add(slot, user, meta)            # согласился и получил ссылку — можно принять его заявку
        return "", False
    if res == "error":
        restore_dm_state(slot, uid, prev)
        stats_add(slot, "failed")
        return "не удалось отправить ссылку (сбой связи) — ответьте реплаем", True
    return "/text2 не задан", True


def media_kind(event):
    m = getattr(event, "message", None)
    if m is None:
        return "вложение"
    for attr, name in (("photo", "фото"), ("voice", "голосовое"), ("video", "видео"), ("sticker", "стикер"),
                       ("audio", "аудио"), ("document", "файл")):
        if getattr(m, attr, None):
            return name
    return "вложение"


def has_media(event):
    """Фото, стикер, голосовое, файл... (ссылки с превью не считаются)."""
    m = getattr(event, "message", None)
    return type(getattr(m, "media", None)).__name__ in ("MessageMediaPhoto", "MessageMediaDocument")


async def forward_media(event, header):
    """Присылает админам само вложение (скрин, стикер, голосовое, файл) — на него можно ответить реплаем."""
    bot = bot_ref["bot"]
    if bot is None:
        return
    m = event.message
    f = getattr(m, "file", None)
    data = None
    if (getattr(f, "size", 0) or 0) <= 20 * 1024 * 1024:
        with suppress(Exception):
            data = await m.download_media(file=bytes)
    sent_to = []
    for aid in admin_ids:
        with suppress(Exception):
            sent = await bot.send_message(aid, header + ("" if data else "\n<i>не удалось скачать вложение</i>"), parse_mode="HTML")
            sent_to.append((aid, sent.message_id))
    if not data:
        return sent_to
    mime = (getattr(f, "mime_type", "") or "").lower()
    name = getattr(f, "name", None) or "file" + (getattr(f, "ext", "") or "")
    if m.photo:
        kind = "photo"
    elif m.sticker:
        kind, name = ("sticker", "sticker.webp") if mime == "image/webp" else ("document", name)
    elif m.voice:
        kind, name = "voice", "voice.ogg"
    elif m.video_note:
        kind = "video_note"
    elif m.video and not m.gif:
        kind = "video"
    elif m.audio:
        kind = "audio"
    else:
        kind = "document"
    senders = {"photo": bot.send_photo, "sticker": bot.send_sticker, "voice": bot.send_voice, "video_note": bot.send_video_note,
               "video": bot.send_video, "audio": bot.send_audio, "document": bot.send_document}
    for aid, _mid in list(sent_to):
        file = BufferedInputFile(data, filename=name)
        try:
            sent = await senders[kind](aid, file)
        except Exception:   # например, у админа закрыты голосовые — шлём файлом
            try:
                sent = await bot.send_document(aid, BufferedInputFile(data, filename=name))
            except Exception:
                continue
        sent_to.append((aid, sent.message_id))
    return sent_to


async def process_dm(event, slot, user):
    """Разбор одного входящего ЛС. Вызывается под замком (slot, человек): сообщения идут по очереди, состояние читается свежим."""
    text = (event.raw_text or "").strip()
    state = dm_get(slot, user.id)
    cfg = load_config()
    who = f"@{user.username}" if getattr(user, "username", None) else (getattr(user, "first_name", None) or "без имени")

    async def tell(body, note):
        await notify_admins(f"💬 <b>{esc(who)}</b> · <code>{user.id}</code>\n📲 {title(slot)}\n{body}\n{note}",
                            key=(slot, user.id))

    if state and has_media(event):    # скрины, стикеры, голосовые — присылаем самим файлом (даже если диалог закрыт)
        header = f"💬 <b>{esc(who)}</b> · <code>{user.id}</code>\n📲 {title(slot)}\n📎 {media_kind(event)}"
        if text:
            header += f"\n«{esc(short(text, 300))}»"
        sent_to = await forward_media(event, header + "\n<i>↩️ ответьте реплаем</i>")
        for aid, mid in sent_to or []:
            reply_map[f"{aid}:{mid}"] = f"{slot}:{user.id}"
        trim_reply_map()
        return
    if state == "done" or not text:      # диалог закрыт — молчим
        return
    body = f"«{esc(short(text, 300))}»"
    if state == "manual":                # вы ведёте диалог сами — пересылаем всё
        await tell(body, "<i>↩️ вы ведёте диалог</i>")
        return

    # знакомому — полный сценарий, новому — только «го», «я из анонки» и подобное
    trigger = detect_trigger(text) if state else ("agreement" if is_opener(text) else None)
    if trigger is None:
        if state:
            await tell(body, "❔ <i>смысл не понял — ответьте реплаем</i>")
        return
    if trigger == "ack":
        return

    sent_ts = event.date.timestamp() if getattr(event, "date", None) else time.time()
    age = time.time() - sent_ts
    if age > STALE_SEC and trigger != "stop":     # бот был офлайн: отвечать на старое сообщение автоматом странно
        await tell(body, f"⏰ <i>сообщение пришло {fmt_left(age)} назад — автоответ пропущен, ответьте реплаем</i>")
        return

    with suppress(Exception):
        await event.mark_read()
    note, forward = await handle_dm(event, cfg, slot, user, state or "new", trigger, cfg.get("dm_reply_enabled", True))
    if forward:
        await tell(body, f"{TRIGGER_LABELS[trigger]} · <i>{esc(note)} — ответьте реплаем</i>")


async def on_private_message(event, slot):
    """Входящее ЛС на любой подключённый аккаунт.
    Админу уходит только то, что бот не понял или на что не может ответить сам. Всё остальное — молча.
    Ошибки больше не глотаются: пишутся в bot.log и (не чаще раза в 10 минут) приходят вам."""
    try:
        if not event.is_private or event.out:
            return
        if not mark_seen(f"{slot}:{event.chat_id}:{event.id}"):
            return
        user = await event.get_sender()
        if user is None or getattr(user, "bot", False):
            return
        async with dm_lock(slot, user.id):
            await process_dm(event, slot, user)
    except Exception as e:
        log.exception("ошибка обработки ЛС (акк %s)", slot)
        with suppress(Exception):
            await warn_once("dm_error", f"⚠️ Ошибка обработки ЛС ({title(slot)}): {esc(type(e).__name__)}: {esc(e)}", 600)


def make_dm_handler(slot):
    async def handler(event):
        await on_private_message(event, slot)
    return handler


# ---- подключения ----

slot_locks = {}


def slot_lock(slot):
    """Свой замок на каждый аккаунт: зависший аккаунт больше не блокирует остальные (раньше замок был один общий)."""
    lock = slot_locks.get(slot)
    if lock is None:
        lock = slot_locks[slot] = asyncio.Lock()
    return lock


def login_slots():
    return {st["slot"] for st in login.values()}


async def _drop(slot):
    client = pool.pop(slot, None)
    if client is not None:
        with suppress(Exception):
            await asyncio.wait_for(client.disconnect(), 15)


async def on_join_update(update):
    """Telegram сообщил о новой заявке в группу — сразу «будим» автопринятие, не ждём таймера."""
    wake_accept()


async def _catch_up(client):
    """Догнать сообщения, пришедшие, пока аккаунт был отключён."""
    with suppress(Exception):
        await asyncio.wait_for(client.catch_up(), 90)


async def get_client(slot):
    """Подключённый клиент аккаунта (создаёт подключение при необходимости) или None, если не авторизован."""
    client = pool.get(slot)
    if client is not None and client.is_connected():
        return client                              # быстрый путь: без замков и сети
    if slot in login_slots():
        return None                                # идёт вход: второй клиент на той же сессии «убил» бы её (AuthKeyDuplicated)
    async with slot_lock(slot):
        client = pool.get(slot)
        if client is not None and client.is_connected():
            return client
        await _drop(slot)
        if not session_exists(slot):
            return None
        client = create_client(slot)
        try:
            await asyncio.wait_for(client.connect(), CONNECT_TIMEOUT)
            if not await asyncio.wait_for(client.is_user_authorized(), 30):
                await client.disconnect()
                return None
            client.add_event_handler(make_dm_handler(slot), events.NewMessage(incoming=True))
            if hasattr(tl_types, "UpdatePendingJoinRequests"):
                client.add_event_handler(on_join_update, events.Raw(tl_types.UpdatePendingJoinRequests))
        except BaseException:
            with suppress(Exception):
                await client.disconnect()
            raise
        pool[slot] = client
        dead_slots.discard(slot)
        with suppress(Exception):   # запоминаем, кто это за аккаунт (имя, @ник, id)
            me = await client.get_me()
            info = {"id": me.id, "name": " ".join(x for x in (me.first_name, me.last_name) if x),
                    "username": me.username or "", "phone": f"+{me.phone}" if getattr(me, "phone", None) else ""}
            if acc_info.get(slot) != info:
                acc_info[slot] = info
                save_json(ACC_INFO_FILE, acc_info)
        spawn(_catch_up(client))
        log.info("аккаунт %s подключён", slot)
        return client


async def drop_client(slot):
    async with slot_lock(slot):
        await _drop(slot)


async def connect_all():
    """Подключить все аккаунты с сессиями параллельно — чтобы они сразу слушали ЛС (бот при этом уже отвечает на команды)."""
    async def one(slot):
        try:
            await get_client(slot)
        except Exception as e:
            log.warning("аккаунт %s не подключился: %s: %s", slot, type(e).__name__, e)

    slots = [s for s in slots_sorted(load_config()) if session_exists(s)]
    await asyncio.gather(*(one(s) for s in slots))


async def drop_all():
    await asyncio.gather(*(drop_client(s) for s in list(pool)), return_exceptions=True)


async def check_slot(slot, tick):
    client = pool.get(slot)
    healthy = client is not None and client.is_connected()
    if healthy and tick % 6 == 0:                  # раз в ~3 минуты — настоящий пинг: ловим «полуживое» соединение
        try:
            await asyncio.wait_for(client(functions.PingRequest(ping_id=random.getrandbits(62))), 20)
        except Exception as e:
            log.warning("аккаунт %s не отвечает на пинг (%s) — переподключаю", slot, type(e).__name__)
            healthy = False
    if healthy:
        return
    if slot in dead_slots and tick % 10:           # сессия мёртвая — не долбим Telegram каждые 30 секунд
        return
    try:
        if client is not None:
            await drop_client(slot)
        new = await get_client(slot)
    except Exception as e:
        log.warning("аккаунт %s: переподключение не удалось: %s: %s", slot, type(e).__name__, e)
        return
    if new is None:
        if slot not in dead_slots:
            dead_slots.add(slot)
            await notify_admins(f"⚠️ {title(slot)} не авторизован (сессия слетела). Войдите заново: /login {slot}")
    else:
        log.info("аккаунт %s переподключён сторожем", slot)


async def watchdog_loop():
    """Следит, чтобы аккаунты всегда были подключены и слушали ЛС (раньше после обрыва связи никто не переподключал,
    пока вы сами не вызывали команду, и бот «молчал»)."""
    tick = 0
    await asyncio.sleep(20)
    while True:
        tick += 1
        cfg = load_config()
        busy = login_slots()
        slots = [s for s in slots_sorted(cfg) if session_exists(s) and s not in busy]
        await asyncio.gather(*(check_slot(s, tick) for s in slots), return_exceptions=True)
        await asyncio.sleep(30)


async def ensure_sender():
    return await get_client(load_config()["roles"]["sender"])


# ---------------------------------------------------------
# Спам-блок аккаунтов: проверка через @SpamBot
# ---------------------------------------------------------

MONTHS = {"янв": 1, "фев": 2, "мар": 3, "апр": 4, "мая": 5, "май": 5, "июн": 6, "июл": 7, "авг": 8, "сен": 9, "окт": 10,
          "ноя": 11, "дек": 12, "jan": 1, "feb": 2, "apr": 4, "may": 5, "jun": 6, "jul": 7, "aug": 8,
          "sep": 9, "oct": 10, "nov": 11, "dec": 12}
EN_MAR = "mar"  # англ. «mar» = март (в словаре рус. «мар»)
FREE_MARKERS = ("no limits", "free as a bird", "good news", "никаких ограничений", "добрые вести", "ограничений не",
                "не ограничен", "not limited", "no restrictions")
LIMIT_MARKERS = ("ограничен", "ограничения", "limited", "restricted", "annoying", "не понравились", "жалоб", "reports")
PERMANENT_MARKERS = ("навсегда", "permanent", "бессрочн", "until further notice", "неопределенный срок", "неопределённый срок")


def parse_spambot(text):
    """-> (status, until_ts|None, permanent). status: free | limited | unknown."""
    low = (text or "").lower().replace("ё", "е")
    if any(k in low for k in FREE_MARKERS):
        return "free", None, False
    until = None
    m = re.search(r"(\d{1,2})\s+([a-zа-я]+)\.?,?\s+(\d{4})(?:[^\d]{1,8}(\d{1,2}):(\d{2}))?", low)
    if m:
        key = m.group(2)[:3]
        month = MONTHS.get(key) or (3 if key == EN_MAR else None)
        if month:
            with suppress(ValueError):
                until = datetime(int(m.group(3)), month, int(m.group(1)), int(m.group(4) or 0), int(m.group(5) or 0), tzinfo=timezone.utc).timestamp()
    if until is None:
        m = re.search(r"(\d{4})-(\d{2})-(\d{2})(?:[ t](\d{2}):(\d{2}))?", low)
        if m:
            with suppress(ValueError):
                until = datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)), int(m.group(4) or 0), int(m.group(5) or 0), tzinfo=timezone.utc).timestamp()
    permanent = any(k in low for k in PERMANENT_MARKERS)
    if until or permanent or any(k in low for k in LIMIT_MARKERS):
        return "limited", until, permanent
    return "unknown", None, False


def fmt_left(sec):
    sec = max(0, int(sec))
    d, rem = divmod(sec, 86400)
    h, rem = divmod(rem, 3600)
    m = rem // 60
    parts = ([f"{d} д"] if d else []) + ([f"{h} ч"] if h else []) + ([f"{m} мин"] if m and not d else [])
    return " ".join(parts) or "меньше минуты"


def is_blocked(slot):
    s = spam_state.get(slot) or {}
    if s.get("status") != "limited":
        return False
    if s.get("permanent") or not s.get("until"):
        return True
    return s["until"] > time.time()


def spam_line(slot):
    """Короткая строка о спам-блоке для панели. Пустая, если всё хорошо / нет данных."""
    s = spam_state.get(slot) or {}
    if s.get("status") == "limited":
        if s.get("permanent"):
            return "⛔ спам-блок: бессрочно"
        if s.get("until"):
            left = s["until"] - time.time()
            until = datetime.fromtimestamp(s["until"], SPAM_TZ).strftime("%d.%m %H:%M") + " МСК+4"
            return f"⛔ спам-блок: ещё <b>{fmt_left(left)}</b> (до {until})" if left > 0 else "⌛ срок блока истёк — проверьте /spam"
        return "⛔ спам-блок (срок неизвестен)"
    return ""


async def check_spambot(slot):
    """Зайти с аккаунта в @SpamBot, разобрать ответ и сохранить. Возвращает словарь состояния."""
    info = {"status": "unknown", "until": None, "permanent": False, "checked": time.time(), "text": ""}
    try:
        client = await get_client(slot)
        if client is None:
            info["text"] = "аккаунт не авторизован"
            return info
        async with client.conversation("SpamBot", timeout=25) as conv:
            await conv.send_message("/start")
            text = (await conv.get_response()).raw_text or ""
            with suppress(Exception):   # иногда SpamBot присылает второе сообщение с подробностями
                text += "\n" + ((await asyncio.wait_for(conv.get_response(), 5)).raw_text or "")
        status, until, permanent = parse_spambot(text)
        info.update(status=status, until=until, permanent=permanent, text=text.strip()[:400])
    except Exception as e:
        info["text"] = f"не удалось проверить: {type(e).__name__}"
        return info          # сбой проверки не затирает прежнее известное состояние
    spam_state[slot] = info
    save_json(SPAM_FILE, spam_state)
    return info


def spam_summary(slot, info):
    if info["status"] == "limited":
        if info.get("permanent"):
            return "⛔ ограничен бессрочно"
        if info.get("until"):
            left = info["until"] - time.time()
            until = datetime.fromtimestamp(info["until"], SPAM_TZ).strftime("%d.%m.%Y %H:%M") + " МСК+4"
            return f"⛔ спам-блок: осталось <b>{fmt_left(left)}</b> (до {until})" if left > 0 else "⌛ срок блока уже истёк"
        return "⛔ ограничен, срок SpamBot не указал"
    if info["status"] == "free":
        return "✅ ограничений нет"
    return f"❔ не удалось разобрать ответ: «{esc(short(info.get('text') or '—', 200))}»"


# ---------------------------------------------------------
# Чаты и люди: статусы, админы, скан сообщений
# ---------------------------------------------------------

def status_text(st):
    if isinstance(st, UserStatusOnline):
        return "онлайн"
    if isinstance(st, UserStatusRecently):
        return "был(а) недавно"
    if isinstance(st, UserStatusLastWeek):
        return "был(а) на этой неделе"
    if isinstance(st, UserStatusLastMonth):
        return "был(а) в этом месяце"
    if isinstance(st, UserStatusOffline):
        return f"был(а) {st.was_online:%Y-%m-%d %H:%M} UTC"
    return "скрыт / давно"


def is_recently_active(user, max_days):
    st = getattr(user, "status", None)
    if isinstance(st, (UserStatusOnline, UserStatusRecently)):
        return True
    if isinstance(st, UserStatusLastWeek):
        return max_days >= 7
    if isinstance(st, UserStatusLastMonth):
        return max_days >= 30
    if isinstance(st, UserStatusOffline):
        was = st.was_online if st.was_online.tzinfo else st.was_online.replace(tzinfo=timezone.utc)
        return was >= datetime.now(timezone.utc) - timedelta(days=max_days)
    return False


def chat_names(entity, ref):
    """(полная подпись, короткая подпись): название / @юз / ID — что есть."""
    name = getattr(entity, "title", None) or " ".join(
        x for x in (getattr(entity, "first_name", None), getattr(entity, "last_name", None)) if x
    )
    uname = f"@{entity.username}" if getattr(entity, "username", None) else ""
    full = " · ".join(x for x in (name, uname, str(ref)) if x)
    return full, (name or uname or str(ref))


def full_chat_id(chat):
    return int(f"-100{chat.id}") if isinstance(chat, Channel) else -chat.id


def chat_key(entity):
    return ("c" if isinstance(entity, Channel) else "g", entity.id)


async def fetch_admins(client, entity, strict=False):
    """Все админы и владелец чата: [{id, username, name, bot, creator, rank}]."""
    out = []
    try:
        if isinstance(entity, Channel):
            users = await retry(lambda: client.get_participants(entity, filter=ChannelParticipantsAdmins))
        else:
            users = await retry(lambda: client.get_participants(entity))
        for u in users:
            p = getattr(u, "participant", None)
            kind = type(p).__name__
            creator = kind.endswith("Creator")
            if not (isinstance(entity, Channel) or creator or "Admin" in kind):
                continue
            out.append({
                "id": u.id,
                "username": (getattr(u, "username", None) or "").lower() or None,
                "name": " ".join(x for x in (getattr(u, "first_name", None), getattr(u, "last_name", None)) if x),
                "bot": bool(getattr(u, "bot", False)),
                "creator": creator,
                "rank": getattr(p, "rank", None),
            })
    except FloodWaitError:
        raise
    except Exception:
        if strict:
            raise
        return []
    return out


async def fetch_admin_ids(client, entity):
    return {a["id"] for a in await fetch_admins(client, entity)}


async def scan_messages(client, entity, limit, handle):
    """Идёт по последним сообщениям (от новых к старым), вызывает handle(msg, sender).
    При сбое Telegram ждёт и продолжает с того же места. Возвращает (просмотрено, неполно)."""
    seen = offset_id = attempts = 0
    while seen < limit:
        try:
            async for msg in client.iter_messages(entity, limit=limit - seen, offset_id=offset_id):
                check_stop()
                offset_id = msg.id
                seen += 1
                attempts = 0
                handle(msg, await msg.get_sender())
            return seen, False
        except TRANSIENT:
            attempts += 1
            if attempts > 5:
                return seen, True
            await interruptible_sleep(3 * attempts)
    return seen, False


async def first_access(clients, ref):
    """Первый аккаунт из списка, у которого есть доступ к чату: (client, entity)."""
    error = None
    for c in clients:
        try:
            return c, await retry(lambda c=c: resolve_chat(c, ref))
        except Exception as e:
            error = e
    if isinstance(error, ValueError) and "input entity" in str(error):
        raise RuntimeError("ни один аккаунт не состоит в этом чате (или неверный ID)")
    raise error


async def scan_chat(clients, ref, limit, max_days):
    """Источник: скан последних сообщений (боты и люди без юзернейма пропускаются, считается активность).
    Возвращает dict: short, chat_id, users {id: {username, active}}, admins {id}, partial."""
    client, entity = await first_access(clients, ref)
    _, short_name = chat_names(entity, ref)
    remember_chat(ref, short_name)
    users = {}
    cutoff = datetime.now(timezone.utc) - timedelta(days=max_days) if max_days > 0 else None

    def handle(msg, sender):
        if not isinstance(sender, User) or sender.id in users:
            return  # от новых к старым: первое попадание = последнее сообщение человека
        if sender.bot or sender.deleted or not sender.username:
            return
        date = msg.date if msg.date.tzinfo else msg.date.replace(tzinfo=timezone.utc)
        active = cutoff is None or date >= cutoff or is_recently_active(sender, max_days)
        users[sender.id] = {"username": sender.username.lower(), "active": active}

    _, partial = await scan_messages(client, entity, limit, handle)
    return {"short": short_name, "chat_id": tg_utils.get_peer_id(entity), "users": users,
            "admins": await fetch_admin_ids(client, entity), "partial": partial}


async def resolve_exclusion(clients, ref):
    """Чат-исключение: сообщения НЕ сканируются, проверка идёт по общим группам отправителя.
    clients = [сборщик, отправитель]. Если отправитель в чате не состоит — запасной вариант:
    список участников у того аккаунта, который в чате есть."""
    resolved, error = [], None
    for n, client in enumerate(clients):
        try:
            resolved.append((n, client, await retry(lambda client=client: resolve_chat(client, ref))))
        except Exception as e:
            error = e
    if not resolved:
        if isinstance(error, ValueError) and "input entity" in str(error):
            raise RuntimeError("ни один аккаунт не состоит в этом чате (или неверный ID)")
        raise error

    _, short_name = chat_names(resolved[0][2], ref)
    remember_chat(ref, short_name)
    admins = set()
    for _n, client, ent in resolved:
        admins |= await fetch_admin_ids(client, ent)

    members = set()
    if not any(n == 1 for n, _c, _e in resolved):   # отправитель в чате не состоит
        for _n, client, ent in resolved:
            with suppress(Exception):
                for u in await client.get_participants(ent, aggressive=True):
                    members.add(u.id)
    return {"short": short_name, "key": chat_key(resolved[0][2]), "admins": admins, "members": members}


async def common_chats(client, entity):
    """Все общие чаты аккаунта с человеком."""
    chats, max_id = [], 0
    while True:
        res = await retry(lambda: client(functions.messages.GetCommonChatsRequest(user_id=entity, max_id=max_id, limit=100)))
        batch = list(res.chats)
        chats += batch
        if len(batch) < 100:
            return chats
        max_id = batch[-1].id


async def excluded_by_common_groups(sender, entity, excl_keys):
    """Название чата-исключения, если он есть среди общих чатов отправителя с человеком, иначе None."""
    for ch in await common_chats(sender, entity):
        hit = excl_keys.get(chat_key(ch))
        if hit:
            return hit
    return None


def err_text(e):
    return str(e) if isinstance(e, RuntimeError) else f"{type(e).__name__}: {e}"


# ---------------------------------------------------------
# /run: скан чатов -> проверка по чатам-исключениям -> рассылка
# ---------------------------------------------------------

REASONS = {
    "UsernameNotOccupiedError": "юзернейм не найден",
    "UsernameInvalidError": "неверный юзернейм",
    "UserPrivacyRestrictedError": "закрыты личные сообщения",
    "UserIsBlockedError": "заблокировал аккаунт",
    "InputUserDeactivatedError": "аккаунт удалён",
    "PeerFloodError": "Flood",
}
FLOOD_LIMIT = 5   # столько flood подряд -> остановка и проверка отправителя в SpamBot


def reason_of(e):
    if isinstance(e, FloodWaitError):
        return f"FloodWait {e.seconds} с"
    if isinstance(e, RuntimeError):
        return str(e)
    return REASONS.get(type(e).__name__) or type(e).__name__


def person_line(icon, name, uid, info, tail=""):
    """❌ @username (id) · Группа (id группы) · причина"""
    line = f"{icon} @{esc(name)} (<code>{uid}</code>) · {esc(short(info['chat'], 30))} (<code>{info['chat_id']}</code>)"
    return line + (f" · {esc(tail)}" if tail else "")


async def with_flood_wait(fn, cap=300):
    """Повторяет запрос, пережидая короткий FloodWait (длиннее cap — пробрасывает ошибку)."""
    try:
        return await fn()
    except FloodWaitError as e:
        if e.seconds > cap:
            raise
        await interruptible_sleep(e.seconds + 1)
        return await fn()


async def run_collection_and_send(message, cfg, uid):
    if run_locks:
        await notify(message, "⚠️ Рассылка уже идёт. Остановить: /stop")
        return
    run_locks.add(uid)
    stats = {"sent": 0, "failed": 0, "excluded": 0}

    def extra():
        return f"✅ {stats['sent']} · ❌ {stats['failed']} · 🚫 {stats['excluded']}"

    try:
        await run_op(message, "Рассылка", lambda: run_job(message, cfg, stats), extra)
    finally:
        run_locks.discard(uid)


async def run_job(message, cfg, stats):
    t0 = time.monotonic()
    sources, excludes = cfg["chat_source_groups"], cfg["chat_exclude_groups"]
    col, snd = cfg["roles"]["collector"], cfg["roles"]["sender"]
    if not sources:
        await notify(message, "❌ Нет чатов-источников. Добавьте: /source")
        return
    if col == snd:
        await notify(message, "❌ Сборщик и отправитель — один аккаунт. Назначьте разные: /collector, /sender")
        return
    limit, max_days = int(cfg["scan_messages_limit"]), int(cfg["max_inactive_days"])
    max_messages, delay = int(cfg["max_messages"]), float(cfg["delay_between_messages"])

    if not await is_authorized(col):
        await notify(message, f"❌ {title(col)} (сборщик) не авторизован: /login {col}")
        return
    sender = await ensure_sender()
    if sender is None:
        await notify(message, f"❌ {title(snd)} (отправитель) не авторизован: /login {snd}")
        return
    if is_blocked(snd):   # раньше был спам-блок — перепроверяем в @SpamBot
        await check_spambot(snd)
        if is_blocked(snd):
            await notify(message, f"⛔ {title(snd)} под спам-блоком\n{spam_line(snd)}\n\n"
                                  "Поставьте другого отправителя: /sender N или /swap")
            return

    await notify(message, f"🚀 <b>Запуск</b>\n📥 {title(col)}\n📤 {title(snd)}")

    # 1-2. Скан чатов-источников и подготовка чатов-исключений
    found, admins, own_ids = {}, set(), set()
    excl_keys, excl_members = {}, {}
    src_lines, exc_lines = [], []
    async with account_client(col) as collector, working(message, "🔍 Сканирую чаты…"):
        clients = [collector, sender]
        for c in clients:
            with suppress(Exception):
                own_ids.add((await c.get_me()).id)
        for ref in sources:
            check_stop()
            try:
                res = await scan_chat(clients, ref, limit, max_days)
            except Exception as e:
                src_lines.append(f"⚠️ <code>{esc(ref)}</code> · {esc(err_text(e))}")
                continue
            active = {i: u for i, u in res["users"].items() if u["active"]}
            for i, u in active.items():
                found.setdefault(i, {"username": u["username"], "chat": res["short"], "chat_id": res["chat_id"]})
            admins |= res["admins"]
            src_lines.append(f"• {esc(res['short'])} · <b>{len(active)}</b>" + (" ⚠️ скан неполный" if res["partial"] else ""))
        for ref in excludes:
            check_stop()
            try:
                res = await resolve_exclusion(clients, ref)
            except Exception as e:
                exc_lines.append(f"⚠️ <code>{esc(ref)}</code> · {esc(err_text(e))}")
                continue
            excl_keys[res["key"]] = res["short"]
            for i in res["members"]:
                excl_members.setdefault(i, res["short"])
            admins |= res["admins"]
            exc_lines.append(f"• {esc(res['short'])}")

    if excludes and not excl_keys:
        await notify(message, "❌ Ни один чат-исключение не удалось проверить — запуск остановлен, "
                              "чтобы не написать исключённым.\n" + "\n".join(exc_lines))
        return

    blocked = admins | own_ids
    processed, messaged = read_processed(), messaged_ids()
    free = {i: u for i, u in found.items() if i not in blocked}
    done_before = {i for i, u in free.items() if u["username"] in processed or i in messaged}
    queue = sorted(((i, u) for i, u in free.items() if i not in done_before), key=lambda x: x[1]["username"])
    n_adm = sum(1 for i in found if i in admins)
    await notify(
        message,
        "📥 <b>Каналы</b>\n" + "\n".join(src_lines)
        + ("\n\n🚫 <b>Исключения</b>\n" + "\n".join(exc_lines) if exc_lines else "")
        + f"\n\n👥 Найдено: <b>{len(found)}</b> · админов: {n_adm} · уже писали: {len(done_before)}\n"
        f"📋 К проверке: <b>{len(queue)}</b>")
    if not queue:
        return

    # 3-4. Проверка по чатам-исключениям идёт В ФОНЕ параллельно с рассылкой: несколько проверяющих сразу берут людей
    # из очереди и отдают чистых отправителю. Рассылка не ждёт конца проверки, но НИКОМУ не пишет до его проверки.
    out, stopped = Outbox(message), None
    ready_q = asyncio.Queue()
    chk = {"clean": 0, "stop": False, "flood": 0}
    status = await notify(message, "📍 Проверка чатов исключения...")
    pending = iter(queue)

    async def check_worker():
        try:
            for uid_, info in pending:             # один общий итератор: каждый человек достаётся одному проверяющему
                if chk["clean"] >= max_messages or chk["stop"]:
                    return
                check_stop()
                name = info["username"]
                if uid_ in excl_members:
                    stats["excluded"] += 1
                    out.add(person_line("❌", name, uid_, info, f"Исключение: {excl_members[uid_]}"))
                    continue
                step, hit = "поиск", None
                try:
                    entity = await with_flood_wait(lambda: retry(lambda: sender.get_entity(name)))
                    if getattr(entity, "bot", False) or getattr(entity, "deleted", False):
                        raise RuntimeError("бот или удалённый аккаунт")
                    if excl_keys:
                        step = "исключения"
                        hit = await with_flood_wait(lambda: retry(lambda: excluded_by_common_groups(sender, entity, excl_keys)))
                except FloodWaitError as e:
                    chk["flood"], chk["stop"] = e.seconds, True
                    return
                except Exception as e:             # не смогли проверить — на всякий случай не пишем
                    stats["failed"] += 1
                    tail = reason_of(e) if step == "поиск" else f"исключения не проверены ({reason_of(e)})"
                    out.add(person_line("❌", name, uid_, info, tail))
                    continue
                if hit:
                    stats["excluded"] += 1
                    out.add(person_line("❌", name, uid_, info, f"Исключение: {hit}"))
                    continue
                chk["clean"] += 1
                ready_q.put_nowait((uid_, name, entity, info))
        except StopRun:
            chk["stop"] = True

    async def checker():
        try:
            await asyncio.gather(*(check_worker() for _ in range(3)))
        finally:
            ready_q.put_nowait(None)               # сигнал «проверять больше некого»
            if status is not None:
                with suppress(Exception):
                    await status.delete()

    check_task = asyncio.create_task(checker())
    try:
        floods, attempts, started = 0, 0, False
        while True:
            item = await ready_q.get()
            if item is None:
                break
            if attempts >= max_messages:           # проверяющие могли набрать чуть больше лимита — лишних не пишем
                break
            uid_, name, entity, info = item
            if not started:
                started = True
                await notify(message, f"📤 <b>Рассылка</b> · до {min(len(queue), max_messages)} чел.")
            check_stop()
            if attempts:
                await interruptible_sleep(delay)
            attempts += 1
            reserved = False   # человек записан ДО отправки, чтобы при сбое/остановке не получить сообщение дважды

            def unreserve():
                remove_processed(name)
                unset_dm_state(snd, entity.id)

            try:
                append_processed(name)
                set_dm_state(snd, entity.id, "text1")
                set_dm_meta(snd, entity.id, {"chat": info["chat"], "ref": info["chat_id"], "name": name})
                reserved = True
                await asyncio.shield(sender.send_message(entity, cfg["message"]))   # даже жёсткий стоп не оборвёт отправку
                reserved, floods = False, 0
                stats["sent"] += 1
                stats_add(snd, "sent")
                out.add(person_line("✅", name, entity.id, info))
            except (PeerFloodError, FloodWaitError) as e:
                if reserved:
                    unreserve()
                floods += 1
                stats["failed"] += 1
                stats_add(snd, "failed")
                out.add(person_line("❌", name, entity.id, info, reason_of(e)))
                if isinstance(e, FloodWaitError) and e.seconds > 300:
                    stopped = f"⏳ FloodWait {e.seconds} с — рассылка остановлена."
                elif floods >= FLOOD_LIMIT:
                    sb = await check_spambot(snd)
                    stopped = (f"🛑 <b>{FLOOD_LIMIT} flood подряд — рассылка остановлена.</b>\n"
                               f"{title(snd)}: {spam_summary(snd, sb)}\n"
                               "Поставьте другого отправителя: /sender N или /swap")
                elif isinstance(e, FloodWaitError):
                    await interruptible_sleep(e.seconds + 1)
            except TRANSIENT as e:
                # сбой связи: сообщение могло дойти — оставляем в «уже писали», чтобы не отправить повторно
                floods = 0
                stats["failed"] += 1
                stats_add(snd, "failed")
                out.add(person_line("⚠️", name, entity.id, info, f"{type(e).__name__}: возможно доставлено"))
            except Exception as e:
                if reserved:
                    unreserve()
                floods = 0
                stats["failed"] += 1
                stats_add(snd, "failed")
                out.add(person_line("❌", name, entity.id, info, reason_of(e)))
            if stopped:
                break
        if stopped is None and chk["flood"]:
            stopped = f"⏳ FloodWait {chk['flood']} с при проверке — рассылка остановлена."
    finally:
        chk["stop"] = True                         # остановить проверяющих, если рассылка закончилась раньше
        if not check_task.done():
            check_task.cancel()
            with suppress(Exception, asyncio.CancelledError):
                await check_task
        if status is not None:
            with suppress(Exception):
                await status.delete()
        await out.close()

    sec = int(time.monotonic() - t0)
    await notify(
        message,
        ("🏁 <b>Завершено досрочно</b>" if stopped else "🏁 <b>Готово</b>") + f" · ⏱ {sec // 60}:{sec % 60:02d}\n"
        f"✅ Отправлено: <b>{stats['sent']}</b>\n❌ Ошибок: {stats['failed']}\n🚫 Исключено: {stats['excluded']}"
        + (f"\n\n{stopped}" if stopped else ""))


# ---------------------------------------------------------
# /start и /command
# ---------------------------------------------------------

COMMANDS_TEXT = """<b>📋 Команды</b>

<b>▶️ Рассылка</b>
/run — запуск: скан чатов → проверка исключений → отправка
/stop — остановить
/reset — забыть, кому уже писали

<b>👥 Аккаунты</b>
/account N — добавить аккаунт и войти (N — номер)
/login N · /logout N — войти / выйти
/collector N · /sender N — кто собирает / кто пишет
/swap — поменять их местами
/spam — проверка спам-блока
/stats — статистика за день по аккаунтам (/stats вчера)

<b>🔎 Чаты</b>
/source — чаты-источники (откуда берём людей)
/exclude — чаты-исключения (им не пишем)
<i>Добавляются по одному, можно удалить любой или очистить всё</i>

<b>📝 Тексты</b>
/text — первое сообщение
/text2 — ответ на согласие
/text4 — ответ на отказ
/text5 — ответ на вопрос и сомнение
<i>В /text4 и /text5 можно писать несколько вариантов через «---» — бот будет упрашивать по очереди</i>
/text6 — ответ на «откуда мой контакт?»
<i>В текстах: {chat} — чат, где нашли человека, {name} — имя</i>
/auto — автоответы вкл/выкл

<b>🚪 Автопринятие в группу</b>
/accept — вкл/выкл. Группа — из ссылки в /text2, принимает аккаунт с правами
/acceptor — предпочтительный аккаунт (ID), 0 — любой с правами

<b>⚙️ Параметры</b>
/scan — сколько сообщений сканировать
/limit — сколько писать за запуск
/active — давность активности, дней
/delay — пауза между отправками, сек

<b>🧰 Инструменты</b>
/info — всё о человеке (@юзер или ID)
/admin_list — админы чата
/scan_group — файл с людьми чата
/cancel — отменить ввод"""


def start_kb():
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📋 Команды", callback_data="cmds")]])


def slot_state(slot):
    """ok — подключён; login — идёт вход; offline — сессия есть, но соединения нет; none — не вошли."""
    if slot in login_slots():
        return "login"
    c = pool.get(slot)
    if c is not None and c.is_connected():
        return "ok"
    return "offline" if session_exists(slot) else "none"


async def accounts_text(cfg):
    """Короткий список: статус · номер · имя/@ник · роль. Статус берётся из пула без сетевых запросов —
    /start отвечает мгновенно, даже если какой-то аккаунт сейчас переподключается."""
    roles = cfg["roles"].values()
    shown = [s for s in slots_sorted(cfg)
             if s in roles or cfg["accounts"][s]["api_id"] or cfg["accounts"][s]["phone"] or session_exists(s)]
    icons = {"ok": "🟢", "login": "🟡", "offline": "🟠", "none": "🔴"}
    rows = []
    for slot in shown:
        st = slot_state(slot)
        info = acc_info.get(slot) or {}
        who = " ".join(x for x in (esc(info.get("name") or ""), f"@{esc(info['username'])}" if info.get("username") else "") if x)
        who = who or esc(cfg["accounts"][slot]["phone"] or "не настроен")
        row = f"{icons[st]} <b>{slot}</b> · {who}{role_mark(cfg, slot)}"
        if st == "none":
            row += f" · /login {slot}"
        elif st == "offline":
            row += " · переподключается…"
        elif st == "login":
            row += " · идёт вход"
        spam = spam_line(slot)
        rows.append(row + (f"\n      {spam}" if spam else ""))
    return "\n".join(rows) if rows else "нет аккаунтов — /account 1"


async def dashboard():
    cfg = load_config()
    running = f"⏳ Идёт: <b>{esc(', '.join(o.name for o in ops))}</b> · /stop\n\n" if ops else ""
    return (
        "<b>📨 Рассылка</b>\n\n" + running +
        "<b>👥 Аккаунты</b>\n" + await accounts_text(cfg) + "\n\n"
        f"🔎 Источников: <b>{len(cfg['chat_source_groups'])}</b> · 🚫 исключений: <b>{len(cfg['chat_exclude_groups'])}</b>\n"
        f"💬 Автоответы: <b>{'вкл' if cfg['dm_reply_enabled'] else 'выкл'}</b> · ✉️ написано: <b>{len(read_processed())}</b>\n"
        f"🚪 Автопринятие: <b>{'вкл' if cfg.get('auto_accept') else 'выкл'}</b>\n\n"
        "▶️ /run · ⏹ /stop")


async def do_swap():
    cfg = load_config()
    r = cfg["roles"]
    r["collector"], r["sender"] = r["sender"], r["collector"]
    save_config(cfg)
    return r


def do_reset():
    for path in (PROCESSED_FILE, DM_STATES_FILE, DM_META_FILE, REPLY_MAP_FILE):
        _save_pending.pop(path, None)
        with suppress(FileNotFoundError):
            os.remove(path)
    dm_states.clear()
    dm_meta.clear()
    reply_map.clear()
    dm_by_uid.clear()


@router.message(Command("start", "accounts"))
@admin
async def cmd_start(message: Message):
    await notify(message, await dashboard(), start_kb())


@router.message(Command("command", "help"))
@admin
async def cmd_command(message: Message):
    await notify(message, COMMANDS_TEXT)


async def spam_check_all(message, slots=None):
    cfg = load_config()
    slots = slots or [s for s in slots_sorted(cfg) if session_exists(s)]
    if not slots:
        await notify(message, "❌ Нет авторизованных аккаунтов.")
        return
    lines = []
    async with working(message, "🛡 Проверяю SpamBot…"):
        for slot in slots:
            check_stop()
            info = await check_spambot(slot)
            lines.append(f"<b>{title(slot)}</b>\n{spam_summary(slot, info)}")
    await notify(message, "🛡 <b>SpamBot</b>\n\n" + "\n\n".join(lines))


@router.message(Command("spam"))
@admin
@operation("Проверка SpamBot")
async def cmd_spam(message: Message):
    args = args_of(message)
    await spam_check_all(message, [args[0]] if args and args[0] in load_config()["accounts"] else None)


# ---------------------------------------------------------
# Кнопки: только «Команды» в /start и управление списками чатов
# ---------------------------------------------------------

@router.callback_query()
@admin
async def on_button(call: CallbackQuery):
    data = call.data or ""
    if data == "cmds":
        await call.answer()
        await notify(call.message, COMMANDS_TEXT)
        return
    if data.startswith("ch:"):
        parts = data.split(":", 3)           # ch:<список>:<действие>[:<чат>]
        if len(parts) >= 3 and parts[1] in CHAT_LISTS:
            await chat_button(call, parts[1], parts[2], parts[3] if len(parts) > 3 else None)
            return
    await call.answer()


@router.message(Command("cancel"))
@admin
async def cmd_cancel(message: Message):
    await close_login(message.from_user.id)
    waiting.pop(message.from_user.id, None)
    await notify(message, "✅ Отменено")


@router.message(Command("reset"))
@admin
async def cmd_reset(message: Message):
    do_reset()
    await notify(message, "✅ Прогресс сброшен")


@router.message(Command("run"))
@admin
async def cmd_run(message: Message):
    await run_collection_and_send(message, load_config(), message.from_user.id)


@router.message(Command("stop"))
@admin
async def cmd_stop(message: Message):
    n = request_stop()
    await notify(message, "⏹ Останавливаю… дождусь завершения текущего шага (до ~12 сек)." if n else "Сейчас ничего не запущено.")


@router.message(Command("swap"))
@admin
async def cmd_swap(message: Message):
    r = await do_swap()
    await notify(message, f"🔄 Сборщик: <b>{title(r['collector'])}</b>, отправитель: <b>{title(r['sender'])}</b>")


@router.message(Command("collector", "sender"))
@admin
async def cmd_role(message: Message):
    role = "collector" if message.text.split()[0].lstrip("/").startswith("collector") else "sender"
    other = "sender" if role == "collector" else "collector"
    cfg = load_config()
    args = args_of(message)
    if not args or args[0] not in cfg["accounts"]:
        await notify(message, f"Использование: /{role} N\nАккаунты: {', '.join(slots_sorted(cfg))}")
        return
    if cfg["roles"][other] == args[0]:
        await notify(message, "❌ Этот аккаунт уже занят другой ролью. Используйте /swap.")
        return
    cfg["roles"][role] = args[0]
    save_config(cfg)
    await notify(message, f"✅ {'Сборщик' if role == 'collector' else 'Отправитель'}: <b>{title(args[0])}</b>")


# ---------------------------------------------------------
# Чаты-источники и чаты-исключения: добавление по одному, удаление, очистка
# ---------------------------------------------------------

CHAT_LISTS = {
    "source": ("chat_source_groups", "🔎 Чаты-источники", "отсюда собираем людей"),
    "exclude": ("chat_exclude_groups", "🚫 Чаты-исключения", "людям из этих чатов не пишем"),
}


async def lookup_chat_name(ref):
    """Название чата (проверка, что хоть один аккаунт его видит) или None."""
    async def job(client, entity, slot):
        return chat_names(entity, ref)[1]
    try:
        _, name = await asyncio.wait_for(with_chat_access(ref, job), 90)
    except Exception:
        return None
    remember_chat(ref, name)
    return name


def chat_list_view(kind):
    key, head, hint = CHAT_LISTS[kind]
    refs = [str(r) for r in load_config()[key]]
    rows = []
    for n, ref in enumerate(refs, 1):
        name = chat_name_cache.get(ref)
        rows.append(f"{n}. <b>{esc(name)}</b> · <code>{esc(ref)}</code>" if name else f"{n}. <code>{esc(ref)}</code>")
    text = f"<b>{head}</b> ({len(refs)})\n<i>{hint}</i>\n\n" + ("\n".join(rows) if rows else "<i>пока пусто</i>")
    dels = [InlineKeyboardButton(text=f"✖ {n}", callback_data=f"ch:{kind}:del:{ref}") for n, ref in enumerate(refs, 1)]
    kb = [dels[i:i + 5] for i in range(0, len(dels), 5)]
    ctrl = [InlineKeyboardButton(text="➕ Добавить", callback_data=f"ch:{kind}:add")]
    if refs:
        ctrl.append(InlineKeyboardButton(text="🧹 Очистить всё", callback_data=f"ch:{kind}:clear"))
    kb.append(ctrl)
    return text, InlineKeyboardMarkup(inline_keyboard=kb)


async def add_chat(message, kind, raw):
    """Добавляет ОДИН чат. True — если ввод понятен (чат добавлен или уже был)."""
    key = CHAT_LISTS[kind][0]
    refs = parse_refs(raw)
    if not refs:
        await notify(message, "❌ Не понял чат. Отправьте ID, @юзернейм или ссылку t.me/…\nОтмена: /cancel")
        return False
    ref = refs[0]
    cfg = load_config()
    exists = ref in {str(r) for r in cfg[key]}
    if exists:
        lines = ["ℹ️ Этот чат уже в списке"]
    else:
        cfg[key].append(ref)
        save_config(cfg)
        async with working(message, "🔎 Проверяю чат…"):
            name = await lookup_chat_name(ref)
        lines = [f"✅ Добавлен: <b>{esc(name)}</b>" if name else f"✅ Добавлен: <code>{esc(ref)}</code>"]
        if not name:
            lines.append("⚠️ Ни один аккаунт пока не видит этот чат — проверьте ID и что аккаунт в нём состоит")
    if len(refs) > 1:
        lines.append("<i>Чаты добавляются по одному — взял первый, остальные отправьте отдельно.</i>")
    text, kb = chat_list_view(kind)
    await notify(message, "\n".join(lines) + "\n\n" + text, kb)
    return True


async def chat_button(call, kind, action, ref=None):
    key = CHAT_LISTS[kind][0]
    if action == "add":
        waiting[call.from_user.id] = f"add:{kind}"
        await call.answer()
        await notify(call.message, "✍️ Отправьте <b>один</b> чат: ID, @юзернейм или ссылку.\nОтмена: /cancel")
        return
    cfg = load_config()
    toast = None
    if action == "clear":
        cfg[key], toast = [], "Список очищен"
    elif action == "del":
        cfg[key] = [r for r in cfg[key] if str(r) != ref]
        toast = "Удалено"
    save_config(cfg)
    await call.answer(toast)
    text, kb = chat_list_view(kind)
    await edit(call.message, text, kb)


@router.message(Command(*CHAT_LISTS))
@admin
async def cmd_chats(message: Message):
    kind = message.text.split()[0].lstrip("/").split("@")[0]
    args = args_of(message)
    if args:                                  # /source @чат — добавить сразу
        await add_chat(message, kind, " ".join(args))
        return
    text, kb = chat_list_view(kind)
    await notify(message, text, kb)


# ---------------------------------------------------------
# Тексты и параметры (одним обработчиком)
# ---------------------------------------------------------

@router.message(Command(*SETTINGS))
@admin
async def cmd_setting(message: Message):
    name = message.text.split()[0].lstrip("/").split("@")[0]
    key, _kind, hint = SETTINGS[name]
    current = load_config()[key]
    waiting[message.from_user.id] = f"set:{name}"
    await notify(message, f"✍️ <b>{esc(hint)}</b>\n\n<b>Сейчас:</b>\n<code>{esc(current) if current != '' else 'не задано'}</code>\n\n"
                          "Отправьте новое значение.\nОтмена: /cancel")


@router.message(Command("auto", "dmtoggle"))
@admin
async def cmd_auto(message: Message):
    cfg = load_config()
    cfg["dm_reply_enabled"] = not cfg["dm_reply_enabled"]
    save_config(cfg)
    await notify(message, "💬 Автоответы <b>включены</b>" if cfg["dm_reply_enabled"] else "💬 Автоответы <b>выключены</b>")


def parse_setting(kind, text, current):
    if kind == "str":
        return text
    if kind == "int":
        value = int(text)
        if value <= 0:
            raise ValueError
        return value
    if kind == "int0":
        value = int(text)
        if value < 0:
            raise ValueError
        return value
    value = float(text.replace(",", "."))
    if value < 0:
        raise ValueError
    return value


# ---------------------------------------------------------
# Аккаунты: данные, вход, выход
# ---------------------------------------------------------

def save_account(cfg, slot, api_id, api_hash, phone_raw):
    """Возвращает (ошибка, предупреждение)."""
    phone = normalize_phone(phone_raw)
    if not api_id.isdigit() or not api_hash or not phone:
        return "❌ Формат: api_id api_hash +79991234567", None
    acc = cfg["accounts"].setdefault(slot, dict(BLANK_ACCOUNT))
    acc.update(api_id=int(api_id), api_hash=api_hash, phone=phone)
    save_config(cfg)
    same = [s for s, a in cfg["accounts"].items() if s != slot and a.get("phone") == phone]
    return None, (f"⚠️ Этот номер уже указан у аккаунта {same[0]}." if same else None)


@router.message(Command("account"))
@admin
async def cmd_account(message: Message):
    uid = message.from_user.id
    args = args_of(message)
    if not args or not args[0].isdigit() or len(args) not in (1, 2, 4):
        await notify(message,
                     "<b>Использование</b>\n/account N — ввести данные по шагам\n"
                     "/account N api_id api_hash +79991234567\n"
                     "/account N +79991234567 — взять API у уже настроенного аккаунта")
        return
    slot = str(int(args[0]))
    cfg = load_config()
    if len(args) == 2:
        donor = next((a for s, a in sorted(cfg["accounts"].items(), key=lambda x: int(x[0]))
                      if s != slot and a["api_id"] and a["api_hash"]), None)
        if not donor:
            await notify(message, "❌ Нет настроенного аккаунта, откуда взять API. Используйте полный формат.")
            return
        args = [args[0], str(donor["api_id"]), donor["api_hash"], args[1]]
    if len(args) == 4:
        err, warn = save_account(cfg, slot, args[1], args[2], args[3])
        if err:
            await notify(message, err)
            return
        await notify(message, f"✅ {title(slot)} сохранён" + (f"\n{warn}" if warn else ""))
        await begin_login(message, uid, slot)
        return
    acc = cfg["accounts"].get(slot, BLANK_ACCOUNT)
    waiting[uid] = f"account:{slot}"
    await notify(message,
                 f"<b>{title(slot)}</b>\nAPI_ID: {acc['api_id'] or 'не задан'}\nТелефон: {esc(acc['phone'] or 'не задан')}\n\n"
                 "Введите: api_id api_hash +телефон\nИли /cancel")


@router.message(Command("logout"))
@admin
async def cmd_logout(message: Message):
    args = args_of(message)
    if not args or not args[0].isdigit():
        await notify(message, "Использование: /logout N")
        return
    slot = str(int(args[0]))
    if run_locks:
        await notify(message, "⏳ Идёт рассылка — сначала остановите её: /stop")
        return
    await close_login(message.from_user.id)
    await drop_client(slot)
    with suppress(FileNotFoundError):
        os.remove(session_file(slot) + ".session")
    await notify(message, f"✅ Сессия удалена ({title(slot)}).")


async def begin_login(message, uid, slot):
    acc = load_config()["accounts"][slot]
    if run_locks:
        await notify(message, "⏳ Идёт рассылка — сначала остановите её: /stop")
        return
    await close_login(uid)
    waiting.pop(uid, None)
    login[uid] = {"slot": slot, "step": "phone"}
    if acc["phone"]:
        await notify(message, f"📱 {title(slot)}: отправляю код на {esc(acc['phone'])}…")
        await handle_login(message, uid, acc["phone"])
    else:
        await notify(message, f"<b>{title(slot)}</b>\n📱 Введите номер телефона: +79991234567\n\nОтмена: /cancel")


@router.message(Command("login"))
@admin
async def cmd_login(message: Message):
    uid = message.from_user.id
    args = args_of(message)
    cfg = load_config()
    if not args or args[0] not in cfg["accounts"]:
        await notify(message, f"Использование: /login N\nАккаунты: {', '.join(slots_sorted(cfg))}")
        return
    slot = args[0]
    acc = cfg["accounts"][slot]
    if not acc["api_id"] or not acc["api_hash"]:
        await notify(message, f"❌ Сначала /account {slot}")
        return
    if run_locks:
        await notify(message, "⏳ Идёт рассылка — сначала остановите её: /stop")
        return
    await begin_login(message, uid, slot)


async def close_login(uid):
    st = login.pop(uid, None)
    if not st:
        return
    if st.get("client"):
        with suppress(Exception):
            await st["client"].disconnect()
    with suppress(Exception):
        await get_client(st["slot"])  # аккаунт снова в пуле и слушает ЛС


async def handle_login(message, uid, text):
    st = login[uid]
    slot = st["slot"]
    cfg = load_config()

    if st["step"] == "phone":
        phone = normalize_phone(text)
        if not phone:
            await notify(message, "❌ Неверный формат. Пример: +79991234567")
            return
        await drop_client(slot)  # освобождаем файл сессии на время входа
        try:
            client = st["client"] = create_client(slot)
            await client.connect()
            cfg["accounts"][slot]["phone"] = phone
            save_config(cfg)
            if await client.is_user_authorized():
                await close_login(uid)
                await notify(message, f"✅ {title(slot)} уже авторизован.")
                return
            sent = await client.send_code_request(phone, force_sms=False)
            st.update(step="code", phone=phone, hash=sent.phone_code_hash)
            await notify(message, "📱 Код отправлен. Введите его в формате <b>###.##</b>, например <code>123.45</code>")
        except FloodWaitError as e:
            await close_login(uid)
            await notify(message, f"⏳ Подождите {e.seconds} сек. и повторите вход.")
        except Exception as e:
            await close_login(uid)
            await notify(message, f"❌ Ошибка: {esc(type(e).__name__)}: {esc(e)}")
        return

    client = st["client"]

    if st["step"] == "code":
        code = normalize_code(text)
        if not code:
            await notify(message, "❌ Формат кода: <b>###.##</b>, например <code>123.45</code>")
            return
        try:
            await client.sign_in(phone=st["phone"], code=code, phone_code_hash=st["hash"])
            await close_login(uid)
            await notify(message, f"✅ Вход выполнен ({title(slot)}).")
        except SessionPasswordNeededError:
            st["step"] = "password"
            await notify(message, "🔐 Введите пароль 2FA:")
        except PhoneCodeExpiredError:
            try:
                sent = await client.send_code_request(st["phone"], force_sms=False)
                st["hash"] = sent.phone_code_hash
                await notify(message, "⏰ Код истёк, отправил новый. Введите его (###.##):")
            except Exception as e:
                await close_login(uid)
                await notify(message, f"❌ Не удалось отправить новый код: {esc(type(e).__name__)}")
        except PhoneCodeInvalidError:
            await notify(message, "❌ Неверный код, введите ещё раз.")
        except FloodWaitError as e:
            await close_login(uid)
            await notify(message, f"⏳ Подождите {e.seconds} сек. и повторите вход.")
        except Exception as e:
            await close_login(uid)
            await notify(message, f"❌ Ошибка входа: {esc(type(e).__name__)}: {esc(e)}")
        return

    if st["step"] == "password":
        try:
            await client.sign_in(password=text)
            await close_login(uid)
            await notify(message, f"✅ Вход выполнен ({title(slot)}).")
        except Exception as e:
            if "password" in type(e).__name__.lower():
                await notify(message, "❌ Неверный пароль 2FA, попробуйте ещё раз.")
            else:
                await close_login(uid)
                await notify(message, f"❌ Ошибка 2FA: {esc(type(e).__name__)}: {esc(e)}")


# ---------------------------------------------------------
# /info — полная информация о человеке по всем аккаунтам
# ---------------------------------------------------------

async def admin_status(client, chat, entity):
    """('creator'|'admin'|None, должность). Если прямой запрос не прошёл — смотрим по списку админов."""
    try:
        perms = await retry(lambda: client.get_permissions(chat, entity))
        if perms is None:
            return None, None
        rank = getattr(getattr(perms, "participant", None), "rank", None)
        if getattr(perms, "is_creator", False):
            return "creator", rank
        if getattr(perms, "is_admin", False):
            return "admin", rank
        return None, None
    except FloodWaitError:
        raise
    except Exception:
        if entity.id in await fetch_admin_ids(client, chat):
            return "admin", None
        return None, None


async def resolve_user(client, target):
    try:
        entity = await retry(lambda: client.get_entity(target))
    except ValueError:
        await load_dialogs(client)  # подтянуть кэш диалогов и повторить
        entity = await retry(lambda: client.get_entity(target))
    if not isinstance(entity, User):
        raise RuntimeError("это не пользователь")
    return entity


def dm_state_name(v):
    m = re.fullmatch(r"(text[456])(?:x(\d+))?", v or "")
    if m:
        base = {"text4": "отказывался", "text5": "спрашивал", "text6": "спрашивал про контакт"}[m.group(1)]
        return base + (f" ×{m.group(2)}" if m.group(2) else "")
    return {"text1": "ждём ответ", "done": "диалог закрыт", "manual": "ведёте сами"}.get(v, v)


async def profile_text(client, entity):
    """Карточка человека: всё, что есть. Пустые поля («о себе», «отметки») просто не показываются."""
    full = None
    with suppress(Exception):
        full = (await retry(lambda: client(functions.users.GetFullUserRequest(entity)))).full_user
    g = lambda obj, attr: getattr(obj, attr, None)

    name = " ".join(x for x in (entity.first_name, entity.last_name) if x) or "без имени"
    extra = [f"@{u.username}" for u in (g(entity, "usernames") or [])
             if g(u, "active") is not False and u.username and u.username != entity.username]
    uname = (f"@{entity.username}" if entity.username else "без юзернейма") + (f" (ещё: {', '.join(extra)})" if extra else "")
    lines = [
        f"👤 <b>{esc(name)}</b>",
        f"{esc(uname)} · ID <code>{entity.id}</code>",
        "",
        f"📞 Телефон: {esc('+' + entity.phone) if g(entity, 'phone') else 'скрыт'}",
        f"🕓 Статус: {esc(status_text(g(entity, 'status')))}",
    ]
    if g(full, "about"):
        lines.append(f"💬 О себе: {esc(short(full.about, 300))}")
    bd = g(full, "birthday")
    if bd:
        lines.append(f"🎂 День рождения: {bd.day:02d}.{bd.month:02d}" + (f".{bd.year}" if g(bd, "year") else ""))
    if g(entity, "lang_code"):
        lines.append(f"🌐 Язык: {esc(entity.lang_code)}")
    counts = []
    with suppress(Exception):
        counts.append(f"🖼 Фото: {(await client.get_profile_photos(entity, limit=1)).total}")
    if g(full, "common_chats_count") is not None:
        counts.append(f"👥 Общих чатов: {full.common_chats_count}")
    if counts:
        lines.append(" · ".join(counts))
    flags = [t for t, on in (
        ("⭐ премиум", g(entity, "premium")), ("🤖 бот", g(entity, "bot")), ("✅ верифицирован", g(entity, "verified")),
        ("⚠️ скам", g(entity, "scam")), ("🎭 фейк", g(entity, "fake")), ("🗑 удалён", g(entity, "deleted")),
        ("🚫 ограничен", g(entity, "restricted")), ("📇 в контактах", g(entity, "contact")),
    ) if on]
    if flags:
        lines.append("🏷 " + " · ".join(flags))
    reasons = [g(r, "text") or g(r, "reason") or "" for r in (g(entity, "restriction_reason") or [])]
    if any(reasons):
        lines.append("🚫 Ограничения: " + esc(short("; ".join(r for r in reasons if r), 200)))
    if g(full, "blocked"):
        lines.append("⛔ Заблокирован этим аккаунтом")
    return "\n".join(lines)


def split_text(text, limit=3800):
    """Режет длинный текст по строкам, чтобы влезло в сообщение Telegram."""
    chunks, cur = [], ""
    for line in text.split("\n"):
        if cur and len(cur) + len(line) + 1 > limit:
            chunks.append(cur)
            cur = ""
        cur += ("\n" if cur else "") + line
    return chunks + ([cur] if cur else [])


@router.message(Command("info"))
@admin
@operation("Поиск человека")
async def cmd_info(message: Message):
    args = args_of(message)
    if not args:
        await notify(message, "Использование: /info 123456789 или /info @username")
        return
    raw = args[0].strip()
    target = int(raw) if re.fullmatch(r"-?\d+", raw) else re.sub(r"^(https?://)?t\.me/|^@", "", raw)

    cfg = load_config()
    slots = [sl for sl in slots_sorted(cfg) if session_exists(sl)]
    if not slots:
        await notify(message, "❌ Нет авторизованных аккаунтов.")
        return
    tags = {str(r).lower(): "📥 источник" for r in cfg["chat_source_groups"]}
    tags.update({str(r).lower(): "🚫 исключение" for r in cfg["chat_exclude_groups"]})

    box = {"profile": None, "entity": None}
    chats, per_acc, claimed = {}, {}, set()     # chats: id -> {"chat", "kind", "rank"}; админку каждого чата проверяем один раз
    sem = asyncio.Semaphore(4)

    async def check_admin(client, ch, entity, cid):
        async with sem:
            chats[cid]["kind"], chats[cid]["rank"] = await admin_status(client, ch, entity)

    async def work(slot):
        try:
            async with account_client(slot) as client:
                entity = await resolve_user(client, target)
                if box["entity"] is None:                         # карточку строит первый, кто нашёл человека
                    box["entity"] = entity
                    box["profile"] = await profile_text(client, entity)
                found = await common_chats(client, entity)
                jobs = []
                for ch in found:
                    cid = full_chat_id(ch)
                    chats.setdefault(cid, {"chat": ch, "kind": None, "rank": None})
                    if cid not in claimed:
                        claimed.add(cid)
                        jobs.append(check_admin(client, ch, entity, cid))
                await asyncio.gather(*jobs)
                per_acc[slot] = [full_chat_id(ch) for ch in found]
        except FloodWaitError as e:
            per_acc[slot] = f"⏳ FloodWait {e.seconds} сек"
        except ValueError:
            per_acc[slot] = "не видит этого человека"
        except Exception as e:
            per_acc[slot] = f"⚠️ {esc(err_text(e))}"

    async with working(message, "🔎 Ищу…"):
        await asyncio.gather(*(work(sl) for sl in slots))

    entity = box["entity"]
    parts = [box["profile"] or f"👤 {esc(raw)}\nНи один аккаунт не смог найти человека."]
    if entity is not None:
        done = (entity.username or "").lower() in read_processed()
        dialogs = ", ".join(f"акк {sl}: {dm_state_name(v)}" for sl, v in dm_any(entity.id))
        parts.append(f"📨 Из бота: {'писали ✅' if done else 'не писали'}" + (f" · диалог: {esc(dialogs)}" if dialogs else ""))

    def tag_of(cid):
        ch = chats[cid]["chat"]
        return tags.get(str(cid)) or tags.get(f"@{getattr(ch, 'username', '') or ''}".lower()) or ""

    def chat_name(cid):
        return esc(short(getattr(chats[cid]["chat"], "title", None) or "без названия", 45))

    admin_lines = []
    for cid, r in chats.items():
        if r["kind"]:
            role = "👑 владелец" if r["kind"] == "creator" else "🛡 админ"
            admin_lines.append(f"• {chat_name(cid)} <code>{cid}</code> — {role}" + (f" «{esc(r['rank'])}»" if r["rank"] else "")
                               + (f" · {tag_of(cid)}" if tag_of(cid) else ""))
    if admin_lines:
        parts.append(f"👑 <b>Админ в {len(admin_lines)} из {len(chats)} общих чатов</b>\n" + "\n".join(admin_lines))

    for slot in slots:                                  # по аккаунтам: только названия и ID (админы уже выше)
        head = f"<b>{title(slot)}</b>{role_mark(cfg, slot)}"
        res = per_acc.get(slot)
        if isinstance(res, str):
            parts.append(f"{head}: {res}")
        elif not res:
            parts.append(f"{head}: общих чатов нет")
        else:
            rows = [f"• {chat_name(cid)} <code>{cid}</code>" + (f" · {tag_of(cid)}" if tag_of(cid) else "") for cid in res]
            parts.append(f"{head} · общих чатов <b>{len(res)}</b>\n" + "\n".join(rows))

    for chunk in split_text("\n\n".join(parts)):
        await notify(message, chunk)


# ---------------------------------------------------------
# /stats — статистика за день по аккаунтам
# ---------------------------------------------------------

_stats = {"data": None}


def stats_db():
    if _stats["data"] is None:
        _stats["data"] = load_json(STATS_FILE, {})
    return _stats["data"]


def stats_add(slot, kind, n=1):
    """kind: sent — первое сообщение (рассылка), reply — ответ в диалоге, failed — ошибка отправки, accepted — принят в группу."""
    data = stats_db()
    day = datetime.now(MSK).strftime("%Y-%m-%d")
    row = data.setdefault(day, {}).setdefault(str(slot), {})
    row[kind] = row.get(kind, 0) + n
    for old in sorted(data)[:-60]:      # храним последние 60 дней
        del data[old]
    save_json(STATS_FILE, data)


@router.message(Command("stats"))
@admin
async def cmd_stats(message: Message):
    args = args_of(message)
    day = datetime.now(MSK).date()
    label = "сегодня"
    if args and args[0].lower() in ("вчера", "yesterday"):
        day, label = day - timedelta(days=1), "вчера"
    elif args and re.fullmatch(r"\d{4}-\d{2}-\d{2}", args[0]):
        with suppress(ValueError):
            day, label = datetime.strptime(args[0], "%Y-%m-%d").date(), args[0]
    rows = stats_db().get(day.strftime("%Y-%m-%d"), {})

    cfg = load_config()
    slots = [s for s in slots_sorted(cfg) if session_exists(s) or str(s) in rows]
    total = {"sent": 0, "reply": 0, "failed": 0, "accepted": 0}
    lines = []
    for slot in slots:
        r = rows.get(str(slot), {})
        for k in total:
            total[k] += r.get(k, 0)
        lines.append(f"<b>{title(slot)}</b>{role_mark(cfg, slot)}\n"
                     f"✉️ Написано: <b>{r.get('sent', 0)}</b> · 💬 ответов: {r.get('reply', 0)}"
                     + (f" · 🚪 принято: {r['accepted']}" if r.get("accepted") else "")
                     + (f" · ❌ ошибок: {r['failed']}" if r.get("failed") else ""))
    body = "\n\n".join(lines) if lines else "<i>нет аккаунтов</i>"
    await notify(
        message,
        f"📊 <b>Статистика · {label}, {day.strftime('%d.%m')} (МСК)</b>\n\n{body}\n\n"
        f"👥 <b>Написано людям всего: {total['sent']}</b>\n"
        f"💬 Ответов в диалогах: {total['reply']}" + (f"\n🚪 Принято в группу: {total['accepted']}" if total["accepted"] else "")
        + (f"\n❌ Ошибок: {total['failed']}" if total["failed"] else ""))


# ---------------------------------------------------------
# /admin_list и /scan_group
# ---------------------------------------------------------

@router.message(Command("admin_list"))
@admin
@operation("Список админов")
async def cmd_admin_list(message: Message):
    args = args_of(message)
    if not args:
        await notify(message, "Использование: /admin_list -1001234567890 (ID, @юзернейм или ссылка на чат)")
        return
    ref = normalize_ref(args[0])

    async def job(client, entity, slot):
        return entity, await fetch_admins(client, entity, strict=True)

    async with working(message, "⏳ Ищу…"):
        slot, (entity, admins) = await with_chat_access(ref, job)
    label, _ = chat_names(entity, ref)
    admins.sort(key=lambda a: (not a["creator"], a["username"] or "~", a["id"]))
    bots = sum(1 for a in admins if a["bot"])
    head = f"👑 <b>Админы</b> · {esc(label)}\nчерез {title(slot)} · всего <b>{len(admins)}</b>" + (f" (ботов {bots})" if bots else "")
    if not admins:
        await notify(message, head + "\n\nСписок админов пуст или недоступен.")
        return

    def icon(a):
        return "👑" if a["creator"] else "🤖" if a["bot"] else "🛡"

    if len(admins) <= 40:
        lines = [f"{icon(a)} {('@' + a['username']) if a['username'] else '—'} | <code>{a['id']}</code>"
                 + (f" | {esc(a['name'])}" if a["name"] else "") + (f" · «{esc(a['rank'])}»" if a["rank"] else "")
                 for a in admins]
        await notify(message, head + "\n\n" + "\n".join(lines))
        return
    data = "\n".join(f"{('@' + a['username']) if a['username'] else '—'} | {a['id']} | {a['name']} | {a['rank'] or ''}" for a in admins)
    await message.answer_document(BufferedInputFile(data.encode("utf-8"), filename=f"admins_{ref}.txt"),
                                  caption=head, parse_mode="HTML")


@router.message(Command("scan_group"))
@admin
@operation("Скан группы")
async def cmd_scan_group(message: Message):
    args = args_of(message)
    if not args:
        await notify(message, "Использование: /scan_group -1001234567890 [сколько сообщений]\nБез числа берётся значение из /scan.")
        return
    cfg = load_config()
    ref = normalize_ref(args[0])
    limit = int(args[1]) if len(args) > 1 and args[1].isdigit() and int(args[1]) > 0 else int(cfg["scan_messages_limit"])

    async def job(client, entity, slot):
        admin_ids_chat = await fetch_admin_ids(client, entity)
        users, stats = {}, {"admins": 0, "nouser": 0}

        def handle(msg, sender):
            if not isinstance(sender, User) or sender.id in users or sender.bot or sender.deleted:
                return
            if sender.id in admin_ids_chat:
                stats["admins"] += 1
                return
            if not sender.username:
                stats["nouser"] += 1
                return
            users[sender.id] = sender.username

        seen, partial = await scan_messages(client, entity, limit, handle)
        return entity, users, stats, seen, partial

    async with working(message, "⏳ Ищу…"):
        slot, (entity, users, stats, seen, partial) = await with_chat_access(ref, job)
    label, _ = chat_names(entity, ref)
    if not users:
        await notify(message, f"📋 <b>{esc(label)}</b>\nПросмотрено {seen} сообщений — подходящих людей не нашёл.")
        return
    rows = sorted(users.items(), key=lambda x: x[1].lower())
    data = "\n".join(f"@{uname} | {uid}" for uid, uname in rows)
    caption = (f"📋 <b>{esc(label)}</b>\nчерез {title(slot)} · просмотрено {seen} сообщ."
               + (" ⚠️ неполно (сбой Telegram)" if partial else "") + "\n"
               f"людей: <b>{len(rows)}</b> · админов пропущено: {stats['admins']} · без юзернейма: {stats['nouser']}")
    await message.answer_document(BufferedInputFile(data.encode("utf-8"), filename=f"scan_{ref}.txt"),
                                  caption=caption, parse_mode="HTML")


# ---------------------------------------------------------
# Запуск
# ---------------------------------------------------------

# ВАЖНО: обработчик обычного текста должен регистрироваться ПОСЛЕДНИМ — иначе он перехватывает
# все сообщения раньше команд, объявленных ниже него, и те «не реагируют».
@router.message(F.text, ~F.text.startswith("/"))
async def handle_input(message: Message):
    uid = message.from_user.id
    if uid not in admin_ids or message.text.startswith("/"):
        return
    text = message.text.strip()

    if uid in login:
        await handle_login(message, uid, text)
        return

    # ответ человеку из бота: реплаем на уведомление или после кнопки «✍️ Ответить»
    target = None
    reply_to = getattr(message, "reply_to_message", None)
    if reply_to is not None:
        target = reply_map.get(f"{message.chat.id}:{reply_to.message_id}")
    state = waiting.get(uid)
    if target is None and state and state.startswith("dmreply:"):
        target = state.split(":", 1)[1]
        waiting.pop(uid, None)
    if target:
        slot, user_id = target.split(":")
        try:
            await dm_send(slot, user_id, text)
        except Exception as e:
            await notify(message, f"❌ Не отправилось ({title(slot)}): {esc(type(e).__name__)}: {esc(e)}")
            return
        stats_add(slot, "reply")
        set_dm_state(slot, user_id, "manual")   # дальше диалог ведёте вы: автоответов не будет
        await notify(message, f"✅ Отправлено · {title(slot)} → <code>{user_id}</code>")
        return

    if not state:
        return
    cfg = load_config()

    if state.startswith("add:"):            # чат в список — строго по одному
        if await add_chat(message, state.split(":", 1)[1], text):
            waiting.pop(uid, None)
        return

    if state.startswith("account:"):
        slot = state.split(":", 1)[1]
        parts = text.split()
        err, warn = save_account(cfg, slot, *parts) if len(parts) == 3 else ("❌ Формат: api_id api_hash +79991234567", None)
        if err:
            await notify(message, err)
            return
        waiting.pop(uid, None)
        await notify(message, f"✅ {title(slot)} сохранён" + (f"\n{warn}" if warn else ""))
        await begin_login(message, uid, slot)
        return

    key, kind, _ = SETTINGS[state.split(":", 1)[1]]
    try:
        cfg[key] = parse_setting(kind, text, cfg[key])
    except ValueError:
        await notify(message, "❌ Неверное значение, попробуйте ещё раз или /cancel.")
        return
    save_config(cfg)
    waiting.pop(uid, None)
    await notify(message, f"✅ Сохранено:\n<code>{esc(short(cfg[key], 300)) if cfg[key] != '' else 'пусто'}</code>")


# ---------------------------------------------------------
# Автопринятие в группу
# ---------------------------------------------------------
# 1) Группа берётся из ссылки-приглашения в /text2.
# 2) Принимает любой подключённый аккаунт, у которого в этой группе есть права (админ с правом добавлять участников).
# 3) Принимаем только тех, кому мы писали и кто согласился: либо согласие записано ботом, либо оно найдено в истории
#    диалога (так подхватываются и те, кто согласился раньше). Если человек потом передумал — заявку всё равно принимаем.
#    Чужие заявки (нет диалога / не соглашался) не трогаем.

INVITE_RE = re.compile(r"(?:t\.me|telegram\.me|telegram\.dog)/(?:\+|joinchat/)([\w-]{10,})", re.I)
PUBLIC_RE = re.compile(r"(?:t\.me|telegram\.me)/([A-Za-z]\w{4,31})(?![\w/])", re.I)
AGREED_TTL = 30 * 86400
accept_cache = {"key": None, "ts": 0, "routes": [], "reason": None}
accept_checked = {}      # uid -> время последней проверки истории (чтобы не читать диалог каждые 20 секунд)
accept_warned = {}


_agreed = {"data": None}
_accept_wakeup = {"ev": None}


def accept_event():
    if _accept_wakeup["ev"] is None:
        _accept_wakeup["ev"] = asyncio.Event()
    return _accept_wakeup["ev"]


def wake_accept():
    """Разбудить автопринятие сейчас (новая заявка или новое согласие), а не ждать следующего тика."""
    with suppress(Exception):
        accept_event().set()


def agreed_db():
    if _agreed["data"] is None:
        _agreed["data"] = load_json(AGREED_FILE, {})
    return _agreed["data"]


def agreed_add(slot, user, meta):
    data = agreed_db()
    if (data.get(str(user.id)) or {}).get("accepted"):
        return                                      # этого уже приняли
    now = time.time()
    for k in [k for k, v in data.items() if now - v.get("ts", 0) >= AGREED_TTL]:
        del data[k]
    data[str(user.id)] = {"slot": str(slot), "ts": now, "name": getattr(user, "username", None) or meta.get("name") or "",
                          "chat": meta.get("chat") or "", "chat_id": meta.get("ref")}
    save_json(AGREED_FILE, data)
    wake_accept()                                   # человек мог уже подать заявку — принимаем без задержки


def agreed_waiting():
    now = time.time()
    return {int(k): v for k, v in agreed_db().items()
            if not v.get("accepted") and now - v.get("ts", 0) < AGREED_TTL}


def agreed_mark_accepted(uid, rec):
    data = agreed_db()
    row = data.get(str(uid)) or dict(rec, ts=time.time())
    row.update(accepted=True, accepted_at=time.time())
    data[str(uid)] = row
    save_json(AGREED_FILE, data)


def already_accepted(uid):
    return bool((agreed_db().get(str(uid)) or {}).get("accepted"))


async def accept_warn(key, text):
    """Предупреждение админам — не чаще раза в час."""
    await warn_once("accept:" + key, text, 3600)


async def invite_targets(client, text):
    """Группы из ссылок в тексте, которые видит этот аккаунт (то есть он в них состоит)."""
    found = []
    for h in INVITE_RE.findall(text):
        with suppress(Exception):
            chat = getattr(await client(functions.messages.CheckChatInviteRequest(h)), "chat", None)
            if chat is not None:
                found.append(chat)
    if not found:                                   # публичная группа: t.me/username
        for name in PUBLIC_RE.findall(text):
            with suppress(Exception):
                ent = await client.get_entity(name)
                if not isinstance(ent, User):
                    found.append(ent)
    return found


async def can_accept(client, entity):
    """Есть ли у аккаунта право принимать заявки в этой группе."""
    try:
        p = await client.get_permissions(entity, "me")
    except Exception:
        return False
    return bool(getattr(p, "is_creator", False) or (getattr(p, "is_admin", False) and getattr(p, "invite_users", False)))


async def accept_routes(cfg):
    """[(slot, группа)] — какой аккаунт какую группу принимает. -> (routes, причина_если_пусто).
    Причины: 'group' — группа не найдена, 'rights' — нет прав, 'offline' — аккаунт не подключился (не кэшируется,
    раньше из-за этого приходило ложное «не нашёл группу» и принятие замирало на 2 минуты)."""
    text = cfg.get("text_agree1") or ""
    ttl = 600 if accept_cache["routes"] else 120
    if accept_cache["key"] == text and time.time() - accept_cache["ts"] < ttl:
        return accept_cache["routes"], accept_cache["reason"]
    pref = int(cfg.get("accept_user_id") or 0)
    slots = [s for s in slots_sorted(cfg) if session_exists(s)]
    slots.sort(key=lambda s: 0 if pref and int((acc_info.get(s) or {}).get("id") or 0) == pref else 1)
    routes, seen, saw_group, offline = [], set(), False, False
    for slot in slots:
        try:
            client = await get_client(slot)
            if client is None:
                continue                            # сессия мёртвая — это не «временно офлайн»
            targets = await invite_targets(client, text)
            found = []
            for entity in targets:
                saw_group = True
                cid = tg_utils.get_peer_id(entity)
                if cid not in seen and await can_accept(client, entity):
                    seen.add(cid)
                    found.append((slot, entity))
        except Exception as e:
            log.warning("accept_routes: аккаунт %s недоступен: %s: %s", slot, type(e).__name__, e)
            offline = True
            continue
        routes += found
    if not routes and offline:
        return [], "offline"
    reason = None if routes else ("rights" if saw_group else "group")
    accept_cache.update(key=text, ts=time.time(), routes=routes, reason=reason)
    return routes, reason


async def pending_requests(client, entity):
    """Все ожидающие заявки в группу: ([importer], {id: User})."""
    users, importers, offset_date, offset_user = {}, [], None, InputUserEmpty()
    for _ in range(5):
        res = await client(functions.messages.GetChatInviteImportersRequest(
            peer=entity, limit=100, offset_date=offset_date, offset_user=offset_user, requested=True))
        users.update({u.id: u for u in res.users})
        importers += res.importers
        if len(res.importers) < 100:
            break
        last = res.importers[-1]
        offset_date, offset_user = last.date, users.get(last.user_id) or InputUserEmpty()
    return importers, users


async def agreed_in_history(uid):
    """Мы писали этому человеку и он согласился (ищем в истории диалога)? -> запись или None."""
    states = dm_any(uid)
    if not states or time.time() - accept_checked.get(uid, 0) < 60:
        return None
    accept_checked[uid] = time.time()
    if len(accept_checked) > 5000:
        accept_checked.clear()
    for slot, _state in states:
        try:
            client = await get_client(slot)
        except Exception:
            continue
        if client is None:
            continue
        try:
            peer = await client.get_input_entity(int(uid))
            async for msg in client.iter_messages(peer, limit=40):
                text = (msg.raw_text or "").strip()
                if msg.out or not text:
                    continue
                if detect_trigger(text) == "agreement" or is_opener(text):
                    meta = dm_meta_get(slot, uid)
                    return {"slot": str(slot), "name": meta.get("name") or "", "chat": meta.get("chat") or "",
                            "chat_id": meta.get("ref"), "ts": time.time()}
        except Exception:
            continue
    return None


async def accept_cycle(cfg):
    routes, reason = await accept_routes(cfg)
    if not routes:
        if reason == "rights":
            await accept_warn("rights", "⚠️ <b>Автопринятие</b>: ни у одного подключённого аккаунта нет прав принимать заявки "
                                        "в этой группе (нужен админ с правом «Добавление участников»)")
        elif reason == "group":
            await accept_warn("group", "⚠️ <b>Автопринятие</b>: не нашёл группу. Вставьте ссылку-приглашение в /text2, "
                                       "а один из аккаунтов должен состоять в этой группе")
        return
    agreed_now = agreed_waiting()

    for slot, entity in routes:
        try:
            client = await get_client(slot)
        except Exception as e:
            log.warning("автопринятие: аккаунт %s недоступен: %s: %s", slot, type(e).__name__, e)
            accept_cache["ts"] = 0
            continue
        if client is None:
            accept_cache["ts"] = 0
            continue
        try:
            importers, users = await pending_requests(client, entity)
        except FloodWaitError as e:
            await asyncio.sleep(min(e.seconds + 1, 120))
            return
        except Exception as e:
            log.warning("автопринятие: не получил заявки (акк %s): %s: %s", slot, type(e).__name__, e)
            accept_cache["ts"] = 0                  # права могли пропасть — пересчитаем маршруты
            continue
        for imp in importers:
            uid = imp.user_id
            user = users.get(uid)
            if user is None or getattr(user, "bot", False) or getattr(user, "deleted", False) or already_accepted(uid):
                continue
            rec = agreed_now.get(uid) or await agreed_in_history(uid)
            if rec is None or dm_get(rec["slot"], uid) is None:
                continue                            # чужая заявка: диалога нет или согласия не было — не трогаем
            try:
                await client(functions.messages.HideChatJoinRequestRequest(peer=entity, user_id=user, approved=True))
            except FloodWaitError as e:
                await asyncio.sleep(min(e.seconds + 1, 120))
                return
            except Exception as e:
                if type(e).__name__ == "ChatAdminRequiredError":
                    accept_cache["ts"] = 0
                    return
                log.info("автопринятие: заявка %s пропущена (%s)", uid, type(e).__name__)
                continue                             # заявку отозвали / уже в группе — пропускаем
            agreed_mark_accepted(uid, rec)
            agreed_now.pop(uid, None)
            stats_add(rec["slot"], "accepted")
            log.info("принят в группу: %s (акк %s)", uid, slot)
            who = f"@{user.username}" if getattr(user, "username", None) else (f"@{rec['name']}" if rec.get("name") else "без юзернейма")
            origin = esc(rec.get("chat") or "—") + (f" (<code>{rec['chat_id']}</code>)" if rec.get("chat_id") else "")
            await notify_admins(
                f"✅ <b>Принят в группу</b> · {esc(short(getattr(entity, 'title', None) or '', 40))}\n"
                f"👤 {esc(who)} · <code>{uid}</code>\n"
                f"📲 Писал: {title(rec['slot'])}\n"
                f"📍 Из чата: {origin}\n"
                f"🛡 Принял: {title(slot)}", key=(rec["slot"], uid))
            await asyncio.sleep(0.5)


async def accept_loop():
    """Проверка заявок: каждые ACCEPT_POLL сек И сразу по событию (новая заявка / новое согласие).
    Каждый цикл ограничен по времени — зависший запрос больше не останавливает автопринятие навсегда."""
    ev = accept_event()
    while True:
        try:
            cfg = load_config()
            if cfg.get("auto_accept"):
                await asyncio.wait_for(accept_cycle(cfg), 240)
        except asyncio.CancelledError:
            raise
        except asyncio.TimeoutError:
            log.warning("автопринятие: цикл завис дольше 4 минут — перезапускаю")
            accept_cache["ts"] = 0
        except Exception:
            log.exception("автопринятие: ошибка цикла")
        try:
            await asyncio.wait_for(ev.wait(), ACCEPT_POLL)
        except asyncio.TimeoutError:
            pass
        ev.clear()
        await asyncio.sleep(1)                       # пачка событий = один цикл


def accept_status(cfg):
    pref = int(cfg.get("accept_user_id") or 0)
    return (f"🚪 Автопринятие <b>{'включено' if cfg.get('auto_accept') else 'выключено'}</b>\n\n"
            "📍 Группа: из ссылки в /text2\n"
            "🛡 Принимает: любой подключённый аккаунт с правами админа в группе"
            + (f" (сначала ID <code>{pref}</code>)" if pref else "") + "\n"
            f"⏳ Согласились и ждут: {len(agreed_waiting())}\n\n"
            "<i>Принимаются только те, кому мы писали и кто согласился (проверяется по диалогу). Чужие заявки не трогаются.</i>")


@router.message(Command("accept"))
@admin
async def cmd_accept(message: Message):
    cfg = load_config()
    cfg["auto_accept"] = not cfg.get("auto_accept")
    save_config(cfg)
    accept_cache["ts"] = 0
    wake_accept()
    await notify(message, accept_status(cfg))


dp.include_router(router)

BOT_COMMANDS = [
    ("start", "Аккаунты и статус"), ("command", "Список команд"), ("run", "Запустить рассылку"),
    ("stop", "Остановить"), ("source", "Чаты-источники"), ("exclude", "Чаты-исключения"),
    ("info", "Информация о человеке"), ("stats", "Статистика за день"), ("accept", "Автопринятие вкл/выкл"), ("spam", "Проверить спам-блок"), ("cancel", "Отмена"),
]


async def supervise(name, factory):
    """Фоновая задача, которая сама перезапускается, если упала."""
    while True:
        try:
            await factory()
            return
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("%s: сбой, перезапуск через 5 с", name)
            await asyncio.sleep(5)


async def main():
    setup_logging()
    cfg = load_config()
    token = cfg.get("bot_token") or os.environ.get("BOT_TOKEN")
    if not token:
        raise ValueError("Укажите bot_token в config.json или переменную окружения BOT_TOKEN")
    # защита от двух одновременно запущенных копий (из-за них бывают дубли сообщений и «через раз» команды)
    lock = socket.socket()
    try:
        lock.bind(("127.0.0.1", int(cfg.get("instance_port", 47213))))
    except OSError:
        print("❌ Бот уже запущен в другом окне/процессе. Закройте его и запустите заново.")
        return
    bot = Bot(token=token)
    bot_ref["bot"] = bot
    load_state()
    with suppress(Exception):
        await bot.set_my_commands([BotCommand(command=c, description=d) for c, d in BOT_COMMANDS])
    accept_event()
    # аккаунты подключаются в фоне и параллельно: бот отвечает на команды сразу, а не после подключения всех аккаунтов
    tasks = [
        asyncio.create_task(connect_all()),
        asyncio.create_task(supervise("автопринятие", accept_loop)),
        asyncio.create_task(supervise("сторож соединений", watchdog_loop)),
    ]
    print("🤖 Бот запущен. Аккаунты подключаются в фоне…")
    try:
        await dp.start_polling(bot)
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        for uid in list(login):
            await close_login(uid)
        await drop_all()
        flush_saves()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())

