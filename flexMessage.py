def unitConvert(unit):
    if unit == "day":
        return "日"
    elif unit == "week":
        return "週"
    elif unit == "month":
        return "月"
    elif unit == "year":
        return "年"
    else:
        return "(不明)"


def today_advance(text):
    return {
        "type": "text",
        "text": text + "更新",
        "weight": "bold",
        "size": "sm",
        "wrap": True
    }


def item(item, day):
    data = {
      "type": "bubble",
      "body": {
        "type": "box",
        "layout": "vertical",
        "contents": [
          {
            "type": "text",
            "text": item["name"],
            "weight": "bold",
            "size": "xl",
            "wrap": True,
            "margin": "md"
          },
          {
            "type": "text",
            "text": f'¥{item["price"]}',
            "weight": "regular",
            "size": "lg",
            "wrap": True,
            "margin": "md"
          },
          {
            "type": "box",
            "layout": "vertical",
            "margin": "lg",
            "spacing": "sm",
            "contents": [
              {
                "type": "box",
                "layout": "baseline",
                "spacing": "sm",
                "contents": [
                  {
                    "type": "text",
                    "color": "#aaaaaa",
                    "size": "sm",
                    "flex": 1,
                    "text": "更新日"
                  },
                  {
                    "type": "text",
                    "text": item["next_date"],
                    "wrap": True,
                    "color": "#666666",
                    "size": "sm",
                    "flex": 4
                  }
                ]
              },
              {
                "type": "box",
                "layout": "baseline",
                "spacing": "sm",
                "contents": [
                  {
                    "type": "text",
                    "color": "#aaaaaa",
                    "size": "sm",
                    "flex": 1,
                    "text": "頻度"
                  },
                  {
                    "type": "text",
                    "text": f'{item["interval"]}ヶ{unitConvert(item['unit'])}毎',
                    "wrap": True,
                    "color": "#666666",
                    "size": "sm",
                    "flex": 4
                  }
                ]
              },
              {
                "type": "box",
                "layout": "baseline",
                "spacing": "sm",
                "contents": [
                  {
                    "type": "text",
                    "text": "状況",
                    "color": "#aaaaaa",
                    "size": "sm",
                    "flex": 1
                  },
                  {
                    "type": "text",
                    "text": "停止中" if item["pause"] else "契約中",
                    "wrap": True,
                    "color": "#666666",
                    "size": "sm",
                    "flex": 4
                  }
                ]
              },
              {
                "type": "box",
                "layout": "baseline",
                "spacing": "sm",
                "contents": [
                  {
                    "type": "text",
                    "text": "請求先",
                    "color": "#aaaaaa",
                    "size": "sm",
                    "flex": 1
                  },
                  {
                    "type": "text",
                    "text": item["payment_method"],
                    "wrap": True,
                    "color": "#666666",
                    "size": "sm",
                    "flex": 4
                  }
                ]
              }
            ]
          }
        ]
      },
      "footer": {
        "type": "box",
        "layout": "vertical",
        "spacing": "sm",
        "contents": [
          {
            "type": "button",
            "style": "link",
            "height": "sm",
            "action": {
              "type": "uri",
              "label": "編集",
              "uri": "https://liff.line.me/2006629352-naO0NMkB/items/" + item["id"]
            }
          },
          {
            "type": "button",
            "style": "link",
            "height": "sm",
            "action": {
              "type": "message",
              "label": "削除",
              "text": "> delete confirm " + item["id"]
            },
            "color": "#ED1A3D"
          }
        ],
        "flex": 0
      },
    }
    
    if day == "today":
      data["body"]["contents"].insert(0, today_advance("本日"))
    elif day == "advance":
      data["body"]["contents"].insert(0, today_advance("3日後"))
    
    if "memo" in item:
        data["body"]["contents"][2]["contents"].append({
          "type": "box",
          "layout": "baseline",
          "spacing": "sm",
          "contents": [
            {
              "type": "text",
              "text": "メモ",
              "color": "#aaaaaa",
              "size": "sm",
              "flex": 1
            },
            {
              "type": "text",
              "text": item["memo"],
              "wrap": True,
              "color": "#666666",
              "size": "sm",
              "flex": 4
            }
          ]
        })
    
    return data
