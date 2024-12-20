import flexMessage

def messageText(message, quick_reply=False):
    
    ret = {
        "type": "text",
        "text": message,
    }
    
    if quick_reply:
        
        ret["quickReply"] = {}
    
    return ret


def messageQuickReply(message, datas):
    
    res = messageText(message, True)
    
    quick_reply = []
    
    for data in datas:
        
        quick_reply.append({
            'type': 'action',
            'action': {
                'type': 'message',
                'label': data["label"],
                'text': data["text"]
            }
        })
    
    res["quickReply"]["items"] = quick_reply
    
    return res


def flexCarousel(alt_text):
    
    return {
        "type": "flex",
        "altText": alt_text,
        "contents": {
            "type": "carousel",
            "contents": []
        }
    } 


def userData(datas, alt_text, count=0, day=""):
    
    res = flexCarousel(alt_text)
    
    i = 0
    
    for j in range(count, len(datas)):
        
        res["contents"]["contents"].append(flexMessage.item(datas[j], day))
        
        i += 1
        if i >= 12:
            break
    
    return res


def messageConfirm(message, button1, button2, quick_reply=False):
    
    ret = {
        "type": "template",
        "altText": message,
        "template": {
            "type": "confirm",
            "text": message,
            "actions": [
                {
                    "type": "message",
                    "label": button1[0],
                    "text": button1[-1]
                },
                {
                    "type": "message",
                    "label": button2[0],
                    "text": button2[-1]
                }
            ]
        }
    }
    
    if quick_reply:
        
        ret["quickReply"] = {}
    
    return ret


def quickReply(message, datas):
    
    res = messageText(message, True)
    
    quick_reply = []
    
    for data in datas:
        
        quick_reply.append({
            'type': 'action',
            'action': {
                'type': 'message',
                'label': data["label"],
                'text': data["text"]
            }
        })
    
    res["quickReply"]["items"] = quick_reply
    
    return res

