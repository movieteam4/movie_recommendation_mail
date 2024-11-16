from flask import Flask
from flask_mail import Mail, Message
import mysql.connector
# from apscheduler.schedulers.background import BackgroundScheduler
import os
from call_dataframe import call_dataframe, week_ranking

data = week_ranking(call_dataframe()) # 加載電影數據

app = Flask(__name__) # 初始化Flask應用

#Flask-Mail配置
app.config.update(
    MAIL_SERVER="smtp.gmail.com",
    MAIL_PORT=465,
    MAIL_USE_SSL=True,
    MAIL_USE_TLS=False,
    MAIL_USERNAME="a2b3c7g@gmail.com",
    MAIL_PASSWORD="lzanqhmfgforbapo",
    MAIL_DEFAULT_SENDER="a2b3c7g@gmail.com"
)

mail = Mail(app) #初始化Flask-Mail

#資料庫配置
db_config = {
    'host': 'u3r5w4ayhxzdrw87.cbetxkdyhwsb.us-east-1.rds.amazonaws.com',
    'user': 'dhv81sqnky35oozt',
    'password': 'rrdv8ehsrp8pdzqn',
    'database': 'xltc236odfo1enc9',
}

#中英文電影類型對照表
genre_translation = {
    "Action": "動作",
    "Adventure": "冒險",
    "Animation": "動畫",
    "Art House": "藝術",
    "Comedy": "喜劇",
    "Crime": "犯罪",
    "Documentary": "紀錄片",
    "Drama": "劇情",
    "Family": "家庭",
    "Horror": "恐怖",
    "Children": "兒童",
    "Kung Fu": "功夫",
    "Musical": "音樂",
    "Romance": "愛情",
    "Science Fiction": "科幻",
    "Thriller": "驚悚",
}

def translate_genres(genres, to_language="zh"): #根據用戶選擇的語言進行電影類型翻譯
    translated_genres = []
    for genre in genres:
        if to_language == "zh":
            translated_genres.append(genre_translation.get(genre, genre))
        else:
            translated_genres.append(
                next((k for k, v in genre_translation.items() if v == genre), genre)
            )
    return translated_genres

def get_favorite_genres(email): #從資料庫中根據用戶電子郵件獲取喜好電影類型
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        query = "SELECT favorite_genres FROM verifiedAccount WHERE mail = %s;"
        cursor.execute(query, (email,))
        result = cursor.fetchone()
        
        if result and result[0]:
            favorite_genres = [genre.strip().strip("'") for genre in result[0].strip("[]").split(',')]
            return translate_genres(favorite_genres, to_language="zh")
        else:
            print("No favorite genres found for this user.")
            return []

    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return []

    finally:
        cursor.close()
        connection.close()
        
def get_all_users(): #獲取所有用戶的電子郵件地址
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        query = "SELECT mail FROM verifiedAccount;"
        cursor.execute(query)
        results = cursor.fetchall()
        
        return [result[0] for result in results if result[0]]

    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return []

    finally:
        cursor.close()
        connection.close()

def get_movies_by_genres(email):  #根據用戶喜好類型推薦電影
    favorite_genres = get_favorite_genres(email)
    
    if not favorite_genres:
        return []

    filtered_movies = data[data['類型'].isin(favorite_genres)] # 篩選與用戶喜好相同類型的電影
        
    recommended_movies = filtered_movies.nlargest(3, '當周票房數')
    return recommended_movies['中文片名']

def send_email_with_flask_mail(recipient_email, subject, movies): #發送推薦電影的郵件給用戶
    with app.app_context():
        movie_list_items = "".join(f"<li>{movie}</li>" for movie in movies)
        movie_buttons = "".join(
            f'<a href="https://taiwan-movies-36c4c3ac2ec6.herokuapp.com/Taiwan_movies_all/more_detail/?m={movie}" '
            f'class="button">{movie}</a>' for movie in movies
        )

        html_content = f"""
<html>
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <style>
            body {{
                font-family: Arial, sans-serif;
                background-color: #f4f4f4;
                margin: 0;
                padding: 0;
            }}
            .container {{
                width: 90%;
                max-width: 600px;
                margin: auto;
                background: #ffffff;
                padding: 20px;
                border-radius: 8px;
                box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);
            }}
            h1 {{ color: #333333; }}
            ul {{ list-style-type: disc; padding-left: 20px; }}
            .button {{
                display: inline-block;
                padding: 10px 20px;
                background-color: #aaa;
                color: white;
                text-decoration: none;
                border-radius: 5px;
                margin: 5px;
                text-align: center;
                width: 100%;
            }}
            .footer {{
                text-align: center;
                margin-top: 20px;
                color: #666;
                font-size: 0.9em;
            }}
            @media (min-width: 600px) {{
                .button {{
                    width: auto;
                }}
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>電影推薦信</h1>
            <p>親愛的用戶，</p>
            <p>根據您喜好的類型，我們為您推薦了以下電影：</p>
            <ul>{movie_list_items}</ul>
            <p>希望您會喜歡！</p>
            <div>{movie_buttons}</div>
        </div>
        <div class="footer">
            <p>這是一封自動發送的電子郵件，請勿回覆。</p>
        </div>
    </body>
</html>
""".encode("utf-8").decode("utf-8")


        msg = Message(
            subject=subject,
            sender="a2b3c7g@gmail.com",
            recipients=[recipient_email]
        )
        msg.body = "您的電影推薦清單"
        msg.html = html_content
        msg.extra_headers = {'Content-Type': 'text/html; charset=UTF-8'}

        try:
            mail.send(msg)
            print(f"Email sent to {recipient_email}")
        except Exception as e:
            print(f"Failed to send email: {e}")

def scheduled_job():
    users = get_all_users()
    for user_email in users:
        favorite_genres = get_favorite_genres(user_email)
        if favorite_genres:
            recommended_movies = get_movies_by_genres(user_email)
            if not recommended_movies.empty:
                subject = "Team4電影推薦(請勿直接回覆)"
                send_email_with_flask_mail(user_email, subject, recommended_movies)
        else:
            print(f"No recommendations available for {user_email}.")
            
# scheduler = BackgroundScheduler() #初始化排程器

# def shutdown(): #設置排程器運行狀態和排程時間
#     if not scheduler.running:
#         try:
#             # scheduler.add_job(scheduled_job, 'cron', day_of_week='mon', hour=9)
#             scheduler.add_job(scheduled_job, 'interval', minutes=1)
#             scheduler.start()     
#         except Exception as e:
#             print(f"error{e}")
#             scheduler.shutdown()
#         print("Scheduler started")

# shutdown()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)