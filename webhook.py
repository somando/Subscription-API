import boto3, uuid
from boto3.dynamodb.conditions import Key, Attr
from decimal import Decimal
from datetime import datetime, timedelta

import const, api, format

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(const.DYNAMODB_TABLE_NAME)


def scan_table():
    response = table.scan()
    data = response.get('Items', [])
    
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response.get('Items', []))
    
    return data


def newItem(user_id, item_data):
    
    id = str(uuid.uuid4())
    
    item = {
        'id': id,
        'user': user_id,
        'name': item_data["name"],
        'price': Decimal(item_data["price"]),
        'next_date': item_data["next_date"],
        'interval': Decimal(item_data["interval"]),
        'unit': item_data["unit"],
        'payment_method': item_data["payment_method"],
        'pause': False,
    }
    
    if "memo" in item_data:
        item["memo"] = item_data["memo"]
    
    table.put_item(
        Item=item
    )
    
    return id


def getUserItem(user_id, item_id=None):
    if item_id is not None:
        response = table.query(
            KeyConditionExpression=Key('id').eq(item_id),
            FilterExpression=Attr('user').eq(user_id)
        )
        items = response.get('Items', [])
    else:
        response = table.query(
            IndexName='UserIndex',
            KeyConditionExpression=Key('user').eq(user_id),
            ScanIndexForward=True
        )
        items = response.get('Items', [])
        
        while 'LastEvaluatedKey' in response:
            response = table.query(
                IndexName='UserIndex',
                KeyConditionExpression=Key('user').eq(user_id),
                ScanIndexForward=True,
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            items.extend(response.get('Items', []))
    
    return items


def getUserItemDate(user_id, date):
    response = table.query(
        IndexName='UserIndex',
        KeyConditionExpression=Key('user').eq(user_id) & Key('next_date').eq(date),
        FilterExpression=Attr('pause').eq(False)
    )
    items = response.get('Items', [])
    
    while 'LastEvaluatedKey' in response:
        response = table.query(
            IndexName='UserIndex',
            KeyConditionExpression=Key('user').eq(user_id) & Key('next_date').eq(date),
            FilterExpression=Attr('pause').eq(False),
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        items.extend(response.get('Items', []))
    
    return items


def createItem(user_id, data):
    
    id = newItem(user_id, data)
    
    return id


def deleteItem(user_id, item_id):
    
    for item in getUserItem(user_id, item_id):
        table.delete_item(
            Key={
                'id': item['id']
            }
        )


def deleteItemCheck(user_id, item_id):
    
    for item in getUserItem(user_id, item_id):
        table.delete_item(
            Key={
                'id': item['id']
            }
        )


def deleteUser(user_id):

    for item in getUserItem(user_id):
        table.delete_item(
            Key={
                'id': item['id']
            }
        )


def textCommand(user_id, message, reply_token):
    
    if const.MESSAGE_TRIGGER_SYMBOL + " get" in message:
        
        message_split = message.split(" ")
        
        if len(message_split) >= 3:
            
            count = int(message_split[2])
        
        else:
            
            count = 0
        
        if len(message_split) == 4:
            
            day = message_split[3]
            date = datetime.now() + timedelta(hours=9)
            if day == "advance":
                date += timedelta(days=3)
            date_string = date.strftime('%Y-%m-%d')
            datas = getUserItemDate(user_id, date_string)
        
        else:
            
            day = ""
            datas = getUserItem(user_id)
        
        if len(datas) == 0:
            
            api.sendReply(reply_token, [format.messageText("アイテムが見つかりません。")])
        
        elif len(datas) - count - 12 > 0:
            
            api.sendReply(reply_token, [format.messageText("アイテムが見つかりました。"), format.userData(datas, const.ALT_TEXT.user_item, count, day), format.quickReply("現在表示されていないアイテムがあります。\n続きを表示する場合は下記クイックリプライをご利用ください。", [{"label": "続きを表示", "text": "> get " + str(count + 12)}])])
        
        else:
            
            api.sendReply(reply_token, [format.messageText("アイテムが見つかりました。"), format.userData(datas, const.ALT_TEXT.user_item, count, day)])
    
    elif const.MESSAGE_TRIGGER_SYMBOL + " create" in message:
        
        message_list = message.split(" ")
        
        data = {
            "name": message_list[2],
            "price": int(message_list[3]),
            "next_date": message_list[4],
            "interval": int(message_list[5]),
            "unit": message_list[6],
            "payment_method": message_list[7]
        }
        
        if len(message_list) == 9:
            data["memo"] = message_list[8]
        
        id = createItem(user_id, data)
        
        items = getUserItem(user_id, id)
        
        api.sendReply(reply_token, [format.messageText("アイテムを作成しました。"), format.userData(items, const.ALT_TEXT.user_item)])
    
    elif const.MESSAGE_TRIGGER_SYMBOL + " delete confirm" in message:
        
        message_list = message.split(" ")
        
        item_id = message_list[3]
        
        if len(getUserItem(user_id, item_id)) == 0:
            
            api.sendReply(reply_token, [format.messageText("アイテムが見つかりません。")])
        
        else:
            
            api.sendReply(reply_token, [format.messageConfirm("アイテムを削除しますか？", ["削除", "> delete yes " + item_id], ["キャンセル", "> cancel"])])
    
    elif const.MESSAGE_TRIGGER_SYMBOL + " delete yes" in message:
        
        message_list = message.split(" ")
        
        item_id = message_list[3]
        
        if len(message_list) != 4:
            
            api.sendReply(reply_token, [format.messageText("コマンドが正しくありません。")])
        
        elif len(getUserItem(user_id, item_id)) == 0:
            
            api.sendReply(reply_token, [format.messageText("アイテムが見つかりません。")])
        
        else:
            
            deleteItem(user_id, item_id)
            
            api.sendReply(reply_token, [format.messageText("アイテムを削除しました。")])
    
    elif const.MESSAGE_TRIGGER_SYMBOL + " cancel" in message:
        
        api.sendReply(reply_token, [format.messageText("キャンセルしました。")])
    
    else:
        
        api.sendReply(reply_token, [format.messageText("コマンドが見つかりません。")])


def textMessage(reply_token):
    
    api.sendReply(reply_token, [format.messageText("メッセージは受け取れません。")])  


def textAction(user_id, message, reply_token):
    
    if message[0] == const.MESSAGE_TRIGGER_SYMBOL:
        
        textCommand(user_id, message, reply_token)
    
    else:
        
        textMessage(reply_token)


def route(data):
    
    for event in data["events"]:
        
        request_type = event["type"]
        user_id = event["source"]["userId"]
        
        if request_type == const.UNFOLLOW:
            
            pass
            # deleteUser(user_id)
        
        else:
            
            reply_token = event["replyToken"]
            
            api.sendLoadingAnimation(user_id)
            
            if request_type == const.MESSAGE:
                
                message_type = event["message"]["type"]
                
                if message_type == const.TEXT:
                    
                    message = event["message"]["text"]
                    textAction(user_id, message, reply_token)
