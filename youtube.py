import googleapiclient.discovery
import streamlit as st
import mysql.connector as mysql
import pandas as pd
from datetime import datetime, timedelta
import json


def api_connect(apikey):
    try:
        youtube_api = googleapiclient.discovery.build('youtube', 'v3', developerKey=apikey)
        return youtube_api
    except Exception as e:
        st.error(f"Error connecting to YouTube API: {e}")
        return None


# API key to connect to the YouTube API
api_key = 'AIzaSyD2onBKce9PI8_Y3jo0Szm5PRNVjZc_lqA'
# To establish the connection to the API
youtube = api_connect(api_key)


# SQL connection
def get_db_connection():
    return mysql.connect(host="localhost", user="root", password="Viyan@30", database="youtube", port="3306")


def create_tables(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS channel_info (
            channel_name VARCHAR(255),
            channel_id VARCHAR(255) PRIMARY KEY,
            subscribe BIGINT,
            views BIGINT,
            total_videos INT,
            channel_description TEXT,
            playlist_id VARCHAR(255)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS video_details (
            channel_name VARCHAR(255),
            channel_id VARCHAR(255),
            video_id VARCHAR(255) PRIMARY KEY,
            title TEXT,
            tags TEXT,
            thumbnail TEXT,
            description TEXT,
            published_date DATETIME,
            duration TIME,
            views BIGINT,
            likes INT,
            dislikes INT,
            comments INT
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS comment_details (
            comment_id VARCHAR(255) PRIMARY KEY,
            video_id VARCHAR(255),
            comment_text TEXT,
            author VARCHAR(255),
            published_date DATETIME
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS playlist_details (
            playlist_id VARCHAR(255),
            title VARCHAR(255),
            channel_id VARCHAR(255),
            published_date DATETIME,
            video_count INT
        )
    """)


def api_data_receive(youtube_api, channel_id, part='snippet'):
    try:
        channel_response = youtube_api.channels().list(id=channel_id, part=part).execute()
        return channel_response['items'][0]
    except Exception as e:
        st.error(f"Error retrieving channel data: {e}")
        return None


def channel_data(channel_id):
    with get_db_connection() as conn:
        cursor = conn.cursor()
        create_tables(cursor)

        try:
            request = youtube.channels().list(
                part="snippet,contentDetails,statistics",
                id=channel_id
            )
            response = request.execute()
            if 'items' not in response or not response['items']:
                st.error("No channel data found for the given channel ID")
                return

            detail_list = []
            for item in response.get('items', []):
                details = {
                    'Channel_Name': item['snippet']['title'],
                    'Channel_Id': item['id'],
                    'Subscribers': item['statistics'].get('subscriberCount', 0),
                    'Views': item['statistics'].get('viewCount', 0),
                    'Total_Videos': item['statistics'].get('videoCount', 0),
                    'Channel_Description': item['snippet']['description'],
                    'Playlist_Id': item['contentDetails']['relatedPlaylists']['uploads']
                }
                detail_list.append(details)
                cursor.execute("SELECT count(*) FROM channel_info WHERE channel_id = %s", (details['Channel_Id'],))
                count = cursor.fetchone()[0]
                if count == 0:
                    cursor.execute(
                        "INSERT INTO channel_info (channel_name, channel_id, subscribe, views, total_videos, channel_description, playlist_id) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                        (details['Channel_Name'], details['Channel_Id'], details['Subscribers'], details['Views'],
                         details['Total_Videos'], details['Channel_Description'], details['Playlist_Id']))

                else:
                    cursor.execute("UPDATE channel_info SET channel_name = %s, subscribe = %s, views = %s, total_videos = %s, channel_description = %s, playlist_id = %s WHERE channel_id = %s ",
                                   (details['Channel_Name'], details['Subscribers'],details['Views'],details['Total_Videos'],
                                    details['Channel_Description'], details['Playlist_Id'],details['Channel_Id']))
                    st.write('Channel already exists and values are fetched and updated')
            conn.commit()
            return detail_list
        except Exception as e:
            st.error(f"Error fetching channel data: {e}")
            return []


def parse_duration(duration_str):
    try:
        duration_seconds = int(duration_str[2:-1])
        return duration_seconds
    except ValueError:
        return None


def get_video_details(video_ids):
    video_list = []
    with get_db_connection() as conn:
        cursor = conn.cursor()
        create_tables(cursor)

        for video_id in video_ids:
            try:
                request = youtube.videos().list(
                    part="snippet,contentDetails,statistics",
                    id=video_id
                )

                response = request.execute()
                for item in response['items']:
                    data = {
                        'channel_Name': item['snippet']['channelTitle'],
                        'Channel_Id': item['snippet']['channelId'],
                        'Video_Id': item['id'],
                        'Title': item['snippet']['title'],
                        'Tags': json.dumps(item.get('tags')),
                        'Thumbnail': json.dumps(item['snippet']['thumbnails']),
                        'Description': item['snippet'].get('description', ''),
                        'Publish_Date': item['snippet']['publishedAt'],
                        'Duration': item['contentDetails']['duration'],
                        'Views': item['statistics'].get('viewCount', 0),
                        'Likes': item['statistics'].get('likeCount', 0),
                        'Dislikes': item['statistics'].get('dislikeCount'),
                        'Comments': item['statistics'].get('commentCount', 0)
                    }

                    video_list.append(data)

                    duration_seconds = parse_duration(data['Duration'])
                    duration = timedelta(seconds=duration_seconds) if duration_seconds is not None else timedelta(
                        seconds=0)
                    current_datetime = datetime.now()
                    sql_duration = (current_datetime + duration).strftime('%Y-%m-%d %H:%M:%S')

                    parsed_datetime = datetime.fromisoformat(data['Publish_Date'].replace('Z', '+00:00'))
                    mysql_published_date = parsed_datetime.strftime('%Y-%m-%d %H:%M:%S')
                    cursor.execute("SELECT count(*) FROM video_details WHERE video_id=%s ",(data['Video_Id'],))
                    count = cursor.fetchone()[0]
                    if count == 0:
                        cursor.execute(
                            "INSERT INTO video_details (channel_name, channel_id, video_id, title, tags, thumbnail, description, published_date, duration, views, likes, dislikes, comments) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                            (data['channel_Name'], data['Channel_Id'], data['Video_Id'], data['Title'], data['Tags'],
                             data['Thumbnail'],data['Description'], mysql_published_date, sql_duration, data['Views'], data['Likes'],
                             data['Dislikes'],data['Comments']))
                    else:
                        cursor.execute("UPDATE video_details set channel_name = %s, channel_id = %s, title = %s, tags = %s, thumbnail = %s, description = %s, published_date = %s, duration = %s, views = %s, likes = %s, dislikes = %s, comments = %s WHERE video_id = %s",
                                       (data['channel_Name'], data['Channel_Id'], data['Title'], data['Tags'],
                                                data['Thumbnail'],data['Description'], mysql_published_date, sql_duration, data['Views'], data['Likes'],
                                                data['Dislikes'],data['Comments'],data['Video_Id']))
                conn.commit()
            except Exception as e:
                st.error(f"Error fetching video details for video ID {video_id}: {e}")

    return video_list


def get_video_data(video_id):
    video_ids = []
    try:
        response = youtube.channels().list(id=video_id, part='contentDetails').execute()

        if 'items' not in response or not response['items']:
            st.error("No data found for the given video ID")
            return video_ids

        playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        next_page_token = None

        while True:
            request = youtube.playlistItems().list(
                part='snippet',
                playlistId=playlist_id,
                maxResults=50,
                pageToken=next_page_token
            ).execute()

            for item in request['items']:
                video_ids.append(item['snippet']['resourceId']['videoId'])

            next_page_token = request.get('nextPageToken')
            if next_page_token is None:
                break
    except Exception as e:
        st.error(f"Error fetching video data: {e}")

    return video_ids


def get_comment_details(video_ids):
    comment_list = []
    with get_db_connection() as conn:
        cursor = conn.cursor()
        create_tables(cursor)

        for video_id in video_ids:
            try:
                request = youtube.commentThreads().list(
                    part="snippet",
                    videoId=video_id,
                    maxResults=50
                )
                response = request.execute()

                for item in response['items']:
                    comment_details = {
                        'Comment_ID': item['snippet']['topLevelComment']['id'],
                        'Video_Id': item['snippet']['topLevelComment']['snippet']['videoId'],
                        'Comment_Text': item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        'Author_Name': item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        'Published_Date': item['snippet']['topLevelComment']['snippet']['publishedAt']
                    }

                    comment_list.append(comment_details)

                    parsed_datetime = datetime.fromisoformat(comment_details['Published_Date'].replace('Z', '+00:00'))
                    mysql_published_date = parsed_datetime.strftime('%Y-%m-%d %H:%M:%S')
                    cursor.execute("SELECT count(*) FROM comment_details WHERE comment_id=%s",(comment_details['Comment_ID'],))
                    count = cursor.fetchone()[0]
                    if count == 0:
                        cursor.execute("INSERT INTO comment_details (comment_id, video_id, comment_text, author, published_date) VALUES (%s, %s, %s, %s, %s)",
                        (comment_details['Comment_ID'], comment_details['Video_Id'], comment_details['Comment_Text'],
                         comment_details['Author_Name'], mysql_published_date))
                    else:
                        cursor.execute("UPDATE comment_details SET video_id = %s, comment_text = %s, author = %s, published_date = %s WHERE comment_id = % s",
                                        comment_details['Video_Id'], comment_details['Comment_Text'],
                                        comment_details['Author_Name'], mysql_published_date, comment_details['Comment_ID'] )
                conn.commit()
            except Exception as e:
                st.error(f"Error fetching comments for video ID {video_id}: {e}")

    return comment_list


def get_playlist_details(channel_id):
    playlist_datas = []
    with get_db_connection() as conn:
        cursor = conn.cursor()
        create_tables(cursor)

        next_page_token = None
        while True:
            try:
                request = youtube.playlists().list(
                    part="snippet,contentDetails",
                    channelId=channel_id,
                    maxResults=50,
                    pageToken=next_page_token
                )
                response = request.execute()

                if 'items' not in response or not response['items']:
                    st.error("No playlist data found for the given channel ID")
                    break

                for item in response['items']:
                    playlist_item = {
                        'Playlist_Id': item['id'],
                        'Title': item['snippet']['title'],
                        'Channel_Id': item['snippet']['channelId'],
                        'Published_Date': item['snippet']['publishedAt'],
                        'Video_Count': item['contentDetails']['itemCount']
                    }

                    playlist_datas.append(playlist_item)

                    parsed_datetime = datetime.fromisoformat(playlist_item['Published_Date'].replace('Z', '+00:00'))
                    mysql_published_date = parsed_datetime.strftime('%Y-%m-%d %H:%M:%S')
                    cursor.execute("SELECT count(*) from playlist_details WHERE playlist_id = %s",(playlist_item['Playlist_Id'],))
                    count = cursor.fetchone()[0]
                    if count == 0:
                        cursor.execute(
                            "INSERT INTO playlist_details (playlist_id, title, channel_id, published_date, video_count) VALUES (%s, %s, %s, %s, %s)",
                            (playlist_item['Playlist_Id'], playlist_item['Title'], playlist_item['Channel_Id'],
                             mysql_published_date, playlist_item['Video_Count']))
                    else:
                        cursor.execute("UPDATE playlist_details SET title = %s, channel_id = %s, published_date = %s, video_count = %s WHERE playlist_id = %s",
                                       (playlist_item['Title'], playlist_item['Channel_Id'],mysql_published_date,
                                        playlist_item['Video_Count'],playlist_item['Playlist_Id']))
                conn.commit()
                next_page_token = response.get('nextPageToken')
                if next_page_token is None:
                    break
            except Exception as e:
                st.error(f"Error fetching playlist data: {e}")
                break

    return playlist_datas


def fetch_all_data(channel_id):
    channel_info = channel_data(channel_id)
    video_ids = get_video_data(channel_id)
    video_details = get_video_details(video_ids)
    comment_details = get_comment_details(video_ids)
    playlist_details = get_playlist_details(channel_id)

    # Convert dict to DataFrames
    channel_df = pd.DataFrame([channel_info])
    video_df = pd.DataFrame(video_ids)
    playlist_df = pd.DataFrame(playlist_details)
    video_detail_df = pd.DataFrame(video_details)
    comment_df = pd.DataFrame(comment_details)

    return {
        "channel_details": channel_df,
        "video_details": video_df,
        "comment_details": comment_df,
        "playlist_details": playlist_df,
        "video_data": video_detail_df
    }


def main():

    st.sidebar.header(':red[MENU]')
    option = st.sidebar.radio(":black[Select option]", ['Data Collection', 'Data Analysis'])
    with st.sidebar:
        st.write('-----')
    if option == "Data Collection":
        st.header("Data Collection",divider='red')
        st.markdown('''
                - Enter channel ID in the input field.
                - Clicking the 'Get Channel Details' button will display an overview of youtube channel.
                ''')
        st.markdown('''
                :red[note:] ***you can get the channel ID :***
                open youtube - go to any channel - go to about - share channel - copy the channel ID''')
        st.write("-----")
        channel_id = st.text_input("Enter Channel ID")

        if st.button("Get Channel Details"):
            fetch_all_data(channel_id)
            st.write("Channel data's are fetched and stored successfully")

    elif option == "Data Analysis":
        st.header("Data Analysis", divider='red')

        questions = [
            "1. What are the names of all the videos and their corresponding channels?",
            "2. Which channels have the most number of videos, and how many videos do they have?",
            "3. What are the top 10 most viewed videos and their respective channels?",
            "4. How many comments were made on each video, and what are their corresponding video names?",
            "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
            "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
            "7. What is the total number of views for each channel, and what are their corresponding channel names?",
            "8. What are the names of all the channels that have published videos in the year 2022?",
            "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
            "10. Which videos have the highest number of comments, and what are their corresponding channel names?"
        ]

        selected_questions = st.multiselect("Select questions to execute", questions)
        if st.button("Run Selected Queries"):

            for selected_question in selected_questions:
                with get_db_connection() as conn:
                    cursor = conn.cursor()
                    if selected_question == questions[0]:
                        cursor.execute("SELECT channel_name,title FROM video_details")
                        data = cursor.fetchall()
                        df = pd.DataFrame(data, columns=['Channel Name', 'Title'])
                        st.write(df)


                    elif selected_question == questions[1]:
                        cursor.execute(
                            "SELECT channel_name, COUNT(*) as video_count FROM video_details GROUP BY channel_name ORDER BY video_count DESC")
                        data = cursor.fetchall()
                        df = pd.DataFrame(data, columns=['Channel Name', 'Counts'])
                        st.write(df)


                    elif selected_question == questions[2]:
                        cursor.execute("SELECT channel_name,title,views FROM video_details ORDER BY views DESC LIMIT 10")
                        data = cursor.fetchall()
                        df = pd.DataFrame(data, columns=['Channel Name', 'Title', 'Views'])
                        st.write(df)

                    elif selected_question == questions[3]:
                        cursor.execute("SELECT title,comments FROM video_details")
                        data = cursor.fetchall()
                        df = df = pd.DataFrame(data, columns=['Title', 'Comments'])
                        st.write(df)

                    elif selected_question == questions[4]:
                        cursor.execute(
                            "SELECT channel_name,MAX(likes) as max_likes FROM video_details GROUP BY channel_name")
                        data = cursor.fetchall()
                        df = pd.DataFrame(data, columns=['Channel_Name', 'Likes'])
                        st.write(df)

                    elif selected_question == questions[5]:
                        cursor.execute(
                            "SELECT title, SUM(likes) as total_likes, SUM(dislikes) as total_dislikes FROM video_details GROUP BY title")
                        data = cursor.fetchall()
                        df = pd.DataFrame(data, columns=['Title', 'Likes', 'Dislikes'])
                        st.write(df)

                    elif selected_question == questions[6]:
                        cursor.execute(
                            "SELECT channel_name, SUM(views) as total_views FROM video_details GROUP BY channel_name")
                        data = cursor.fetchall()
                        df = pd.DataFrame(data, columns=['Channel_Name', 'Views'])
                        st.write(df)

                    elif selected_question == questions[7]:
                        cursor.execute("SELECT DISTINCT channel_name FROM video_details WHERE YEAR(published_date) = 2022;")
                        data = cursor.fetchall()
                        df = pd.DataFrame(data, columns=['Channel_Name'])
                        st.write(df)

                    elif selected_question == questions[8]:
                        cursor.execute(
                            "SELECT channel_name, AVG(duration) AS avg_duration FROM video_details GROUP BY channel_name ")
                        data = cursor.fetchall()
                        df = pd.DataFrame(data, columns=['Channel_Name', 'Avg_Duration'])
                        st.write(df)

                    elif selected_question == questions[9]:
                        cursor.execute("""SELECT title, channel_name, SUM(comments) as comments
                                FROM video_details 
                                GROUP BY title, channel_name 
                                ORDER BY comments DESC 
                                LIMIT 1
                            """)
                        data = cursor.fetchall()
                        df = pd.DataFrame(data, columns=['Title', 'Channel_Name', 'Comments'])
                        st.write(df)
    else:
        pass

if __name__ == "__main__":
    main()