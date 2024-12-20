import json

import webhook

def lambda_handler(event, context):
    
    path = event.get('path', '/')
    
    if path == "/subscriptionLINEBot":

        body = json.loads(event['body'])

        webhook.route(body)

        return {
            'statusCode': 200,
            'body': json.dumps('webhook')
        }
