import logging
import requests
from io import BytesIO, BufferedIOBase


logger = logging.getLogger(__name__)
logger.setLevel(logging.NOTSET)
logger.propagate = False


MAX_MESSAGE_LEN = 4096


def escape_html(text):
    return text.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')

# class EMOJI_COLOR:
#     WHITE_CIRCLE = '\U000026AA'
#     BLUE_CIRCLE = '\U0001F535'
#     RED_CIRCLE = '\U00001F7E2'
#     GREEN_CIRCLE = '\U0001F7E2'
#     YELLOW_CIRCLE = '\U0001F7E1'
#     PURPLE_CIRCLE = '\U0001F7E3'
#     BROWN_CIRCLE = '\U0001F7E4'
#     ORANGE_CIRCLE = 'U\0001F7E0'

class EMOJI:
    NOTSET = '\U000026AA' # WHITE_CIRCLE
    DEBUG = '\U0001F7E2' # GREEN_CIRCLE
    INFO = '\U0001F535' # BLUE_CIRCLE
    WARNING = '\U0001F7E1' # YELLOW_CIRCLE
    ERROR = '\U0001F534' # RED_CIRCLE
    CRITICAL = '\U0001F7E4' # BROWN_CIRCLE


class EMOJI_ICO:
    NOTSET = "📢"
    DEBUG = "🤖"
    INFO = "💬"
    WARNING = "⚠️"
    ERROR = "❗"
    CRITICAL = "💀"


class TelegramFormatter(logging.Formatter):
    # fmt = "%(asctime)s %(levelname)s\n[%(name)s:%(funcName)s]\n%(message)s"
    fmt = '%(levelname)s %(message)s'
    datefmt = '%Y-%m-%d %H:%M:%S'
    parse_mode = None

    def __init__(self, fmt=None, datefmt=None, *args, **kwargs):
        super(TelegramFormatter, self).__init__(fmt or self.fmt, datefmt or self.datefmt, *args, **kwargs)


class MarkdownFormatter(TelegramFormatter):
    fmt = '`%(asctime)s` *%(levelname)s*\n[%(name)s:%(funcName)s]\n%(message)s'
    parse_mode = 'Markdown'

    def formatException(self, *args, **kwargs):
        string = super(MarkdownFormatter, self).formatException(*args, **kwargs)
        return '```\n%s\n```' % string
    
class HtmlFormatter(TelegramFormatter):
    # fmt = '<code>%(asctime)s</code> <b>%(levelname)s</b>\nFrom %(name)s:%(funcName)s\n%(message)s'
    fmt = '<b>%(levelname)s</b> <code>%(funcName)s (%(filename)s)</code>\n%(message)s'
    parse_mode = 'HTML'

    def __init__(self, *args, **kwargs):
        self.use_emoji = kwargs.pop('use_emoji', True)
        super(HtmlFormatter, self).__init__(*args, **kwargs)

    def format(self, record):
        if record.funcName:
            record.funcName = escape_html(str(record.funcName))
        if record.filename:
            record.filename = escape_html(str(record.filename))
        if record.module:
            record.module = escape_html(str(record.module))
        if record.name:
            record.name = escape_html(str(record.name))
        if record.msg:
            record.msg = escape_html(record.getMessage())
        if self.use_emoji:
            record.levelname = f"{getattr(EMOJI, record.levelname.upper(), 'NOTSET')}{record.levelname}" 
        super(HtmlFormatter, self).format(record)
        if hasattr(self, '_style'):
            return self._style.format(record)
        else:
            return self._fmt % record.__dict__

    def formatException(self, *args, **kwargs):
        string = super(HtmlFormatter, self).formatException(*args, **kwargs)
        return '<pre>%s</pre>' % escape_html(string)

    def formatStack(self, *args, **kwargs):
        string = super(HtmlFormatter, self).formatStack(*args, **kwargs)
        return '<pre>%s</pre>' % escape_html(string)
    

class TelegramHandler(logging.Handler):
    API_ENDPOINT = 'https://api.telegram.org'
    last_response = None

    def __init__(self, token, chat_id=None, level=logging.NOTSET, timeout=10, disable_notification=False,
                 disable_web_page_preview=False, proxies=None):
        self.token = token
        self.disable_web_page_preview = disable_web_page_preview
        self.disable_notification = disable_notification
        self.timeout = timeout
        self.proxies = proxies
        self.chat_id = chat_id or self.get_chat_id()
        if not self.chat_id:
            level = logging.NOTSET
            logger.error('Did not get chat id. Setting handler logging level to NOTSET.')
        logger.info('Chat id: %s', self.chat_id)

        super(TelegramHandler, self).__init__(level=level)

        self.setFormatter(HtmlFormatter())

        self.data = {
            'chat_id': self.chat_id,
            'disable_web_page_preview': self.disable_web_page_preview,
            'disable_notification': self.disable_notification,
        }
        if getattr(self.formatter, 'parse_mode', None):
            self.data['parse_mode'] = self.formatter.parse_mode


    @classmethod
    def format_url(cls, token, method):
        return '%s/bot%s/%s' % (cls.API_ENDPOINT, token, method)

    def get_chat_id(self):
        response = self.request('getUpdates')
        if not response or not response.get('ok', False):
            logger.error('Telegram response is not ok: %s', str(response))
            return
        try:
            return response['result'][-1]['message']['chat']['id']
        except:
            logger.exception('Something went terribly wrong while obtaining chat id')
            logger.debug(response)

    def request(self, method, **kwargs):
        url = self.format_url(self.token, method)

        kwargs.setdefault('timeout', self.timeout)
        kwargs.setdefault('proxies', self.proxies)
        response = None
        try:
            response = requests.post(url, **kwargs)
            self.last_response = response
            response.raise_for_status()
            return response.json()
        except:
            logger.exception('Error while making POST to %s', url)
            logger.debug(str(kwargs))
            if response is not None:
                logger.debug(response.content)

        return response

    def send_message(self, text, **kwargs):
        if len(text) >= MAX_MESSAGE_LEN:
            return self.send_document(text[:1000], document=BytesIO(text.encode()))
        
        data = {'text': text}
        data.update(self.data | kwargs)
        return self.request('sendMessage', json=data)

    def send_document(self, text, document, **kwargs):
        data = {'caption': text}
        data.update(self.data | kwargs)
        document = document if isinstance(document, BufferedIOBase) else open(document, 'rb')
        return self.request('sendDocument', data=data, files={'document': document})
    
    def set_webhook(self, url, file, **kwargs):
        # url = url.lower()
        url.replace('http://', 'https://', 1)
        url = f"https://{url}" if not url.startswith('https://') else url
        data = {"url": url}
        data.update(self.data | kwargs)
        with  open(file, 'rb') as document:
            self.request('setwebhook', data=data, files={'certificate': document})
        return self.request('getWebhookInfo')
    
    def delete_webhook(self, **kwargs):
        return self.request('deleteWebhook')
    
    def emit(self, record):
        text = self.format(record)
        response = self.send_message(text)
        if response and not response.get('ok', False):
            logger.warning('Telegram responded with ok=false status! {}'.format(response))
