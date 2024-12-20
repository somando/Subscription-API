import os

CHANNEL_ACCESS_TOKEN = os.environ.get("LINE_CHANNEL_ACCESS_TOKEN")

DYNAMODB_TABLE_NAME = "SubscriptionLINEBot"

class LINE_ENDPOINT():
    PUSH_MESSAGE = "https://api.line.me/v2/bot/message/push"
    REPLY_MESSAGE = "https://api.line.me/v2/bot/message/reply"
    LOADING = "https://api.line.me/v2/bot/chat/loading/start"

HEADERS_JSON = {
    'Content-Type': 'application/json',
    'Authorization': 'Bearer ' + CHANNEL_ACCESS_TOKEN
}

# リクエストタイプ
MESSAGE = 'message'
TEXT = 'text'
FOLLOW = 'follow'
UNFOLLOW = 'unfollow'

MESSAGE_TRIGGER_SYMBOL = '>'

class ALT_TEXT():
    user_item = "ユーザーのアイテム"
