import requests, json

import const

def sendMessage(user_id, objects):

    data = {
        "to": user_id,
        "messages": objects
    }

    data = json.dumps(data)

    requests.post(
        const.LINE_ENDPOINT.PUSH_MESSAGE,
        data = data, 
        headers = const.HEADERS_JSON
    )

def sendReply(replyToken, objects):

    data = {
        "replyToken": replyToken,
        "messages": objects
    }
    
    print(data)

    data = json.dumps(data)

    requests.post(
        const.LINE_ENDPOINT.REPLY_MESSAGE,
        data = data, 
        headers = const.HEADERS_JSON
    )

def sendLoadingAnimation(chat_id, sec=60):
    
    data = {
        "chatId": chat_id,
        "loadingSeconds": sec
    }
    
    data = json.dumps(data)
    
    requests.post(
        const.LINE_ENDPOINT.LOADING, 
        data=data, 
        headers=const.HEADERS_JSON
    )