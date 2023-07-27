import streamlit as st
from streamlit_option_menu import option_menu
from googleapiclient.discovery import build
import pymongo
import pandas as pd
import mysql.connector

st.set_page_config(layout="wide")
api_key = "AIzaSyALbrRwmvZikXjsFhdA6UtEGA8_AqUCsBI"
you_tube = build("youtube", "v3", developerKey=api_key)

# create a client instance of MongoDB
client = pymongo.MongoClient('mongodb://localhost:27017/')
# create a database or use existing one
mydb = client['Hii']
# create a collection
channel_list = mydb['Youtube_data']
# define the data to insert
final_output_data = {
            'Channel_Name': 'c_id',
           "Channel_data":'final_output'
           }

# insert or update data in the collection
upload = channel_list.replace_one({'_id': 'channel_id'}, final_output_data, upsert=True)

# print the result of the insertion operation
st.write(f"Updated document id: {upload.upserted_id if upload.upserted_id else upload.modified_count}")

# Close the connection
# client.close()

# Connect to the MySQL server
connect = mysql.connector.connect(
        host = "127.0.0.1",
        user = "root",
        password = "Pallavi@1997")

        # Create a new database and use
mycursor = connect.cursor()
mycursor.execute("CREATE DATABASE IF NOT EXISTS hi_db")

mycursor.execute("use hi_db")



def channel(c_id):  # Returns scrapped channel info
    channel_data = you_tube.channels().list(
        part="snippet, contentDetails, statistics",
        id=c_id).execute()
    print(channel_data)

    channel_info = dict(channel_id=c_id,
                        channel_name=channel_data["items"][0]["snippet"]["title"],
                        subscriber_count=channel_data["items"][0]["statistics"]["subscriberCount"],
                        channel_view_count=channel_data["items"][0]["statistics"]["viewCount"],
                        channel_des=channel_data["items"][0]["snippet"]["description"],
                        playlist_id=channel_data['items'][0]['contentDetails']['relatedPlaylists']['uploads'])

    return channel_info


def get_video_stats(playlist_id):  # Returns video_ids
    all_video_id = []
    next_page_token = None
    while True:
        request = you_tube.playlistItems().list(
            part='contentDetails',
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token)
        response = request.execute()

        for i in range(len(response['items'])):
            all_video_id.append(response['items'][i]['contentDetails']['videoId'])
        next_page_token = response.get('nextPageToken')
        if next_page_token is None:
            break
    return all_video_id


def video_2(playlist_id):  # Returns scrapped video_info
    video_ids_list = get_video_stats(playlist_id)
    video_info = []
    for sublist in video_ids_list:
        request = you_tube.videos().list(
            part="snippet,contentDetails,statistics",
            id=sublist)
        response = request.execute()
        for video in response["items"]:
            video_a = dict(channel_id=video['snippet']['channelId'], video_id=video["id"],
                           video_title=video["snippet"]["title"],
                           video_description=video["snippet"]["description"],
                           video_duration=video["contentDetails"]["duration"],
                           video_caption=video["contentDetails"].get('caption', 'false'),
                           view_count=video["statistics"].get("viewCount", 0),
                           like_count=video["statistics"].get("likeCount", 0),
                           dislike_count=video["statistics"].get("dislikeCount", 0),
                           favorite_count=video["statistics"].get("favoriteCount", 0),
                           comment_count=video["statistics"].get("commentCount", 0),
                           published_at=video["snippet"]["publishedAt"])
            video_info.append(video_a)
    return video_info


def video_comments(playlist_id):  # Returns comment info
    video_ids_list = get_video_stats(playlist_id)
    comment_info = []
    for ele in video_ids_list:
        next_page_token_comment = None
        try:
            while True:
                request = you_tube.commentThreads().list(
                    part="snippet",
                    videoId=ele,
                    maxResults=50,
                    pageToken=next_page_token_comment)
                response = request.execute()
                if response["items"]:
                    for j in response["items"]:
                        comment_1 = dict(video_id=j["snippet"]["topLevelComment"]["snippet"]["videoId"],
                                         comment_id=j["id"],
                                         published_at=j['snippet']['topLevelComment']['snippet']['publishedAt'],
                                         author_display_name=j['snippet']['topLevelComment']['snippet'][
                                             'authorDisplayName'],
                                         display_text=j['snippet']['topLevelComment']['snippet']['textDisplay'])
                        comment_info.append(comment_1)
                next_page_token_comment = response.get(next_page_token_comment)
                if next_page_token_comment is None:
                    break
        except:
            pass

    return comment_info

with st.sidebar:
    opt = option_menu(
        menu_title="Menu",
        options=["About", "Upload", "Migration", "Insights"],
        icons=["house", "upload", "table", "check2-square"],
        menu_icon="cast",
        default_index=0
    )
if opt == "About":
    st.title(":red[YouTube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit]")
    st.write("The YouTube Data Scraper and Analysis web page is a powerful tool designed to provide valuable "
             "insights into YouTube channels based on user-provided channel IDs. By utilizing the YouTube API, "
             "the application extracts crucial details such as the channel name, subscriber count, like count, "
             "total videos, and comprehensive statistics for each video, including view count, likes, dislikes, "
             "and comments.")
    st.write("The webpage goes beyond data extraction and offers a range of analysis and visualization options "
             "using the collected data. Users can delve into the information to gain valuable insights into a "
             "channel's performance, audience engagement, and overall growth trajectory. The flexibility of Streamlit "
             "enables customization and expansion of the application to cater to specific analysis requirements, "
             "allowing users to uncover meaningful patterns and make data-driven decisions")
    st.write("For businesses, the YouTube Data Scraper and Analysis web application serves as a robust solution to "
             "extract, store, and analyze YouTube channel data. By harnessing the power of the scraped data, "
             "businesses can gain actionable insights, optimize content strategies, and make informed decisions to "
             "drive success on the platform. With its user-friendly features and the capability to perform sentiment "
             "analysis using machine learning techniques, the application provides a comprehensive toolkit for "
             "businesses to thrive and effectively connect with their audience on YouTube.")
    st.write("In summary, the YouTube Data Scraper and Analysis web application streamlines the process of gathering "
             "and analyzing data from YouTube channels. By leveraging the YouTube API, MongoDB, and MySQL, "
             "the application empowers users with a powerful toolset to extract valuable insights and facilitate "
             "data-driven decision-making in the dynamic landscape of YouTube content creation and consumption.")


elif opt == "Upload":
    st.title("YouTube Data Harvesting and Warehousing with MongoDB, SQL")
    col1, col2 = st.columns([6, 2])
    with col1:
        channel_id = st.text_input("Enter the channel id")
        page_names = ["Enter", "Preview", "Upload"]
        page = st.radio("select", page_names, horizontal=True, label_visibility="hidden")

        if channel_id and page == "Enter":
            st.write("You can now preview the json file or upload the file in MongoDB")

        if page == "Preview":
            channel_stats = channel(channel_id)
            video_stats = video_2(channel_stats['playlist_id'])
            comment_stats = video_comments(channel_stats['playlist_id'])
            preview_file = dict(channel_info=channel_stats, video_info=video_stats, comment_info=comment_stats)
            st.write(preview_file)

        if page == "Upload":
            channel_stats = channel(channel_id)
            video_stats = video_2(channel_stats['playlist_id'])
            comment_stats = video_comments(channel_stats['playlist_id'])
            file_upload = dict(channel_info=channel_stats, video_info=video_stats, comment_info=comment_stats)
            channel_name = file_upload["channel_info"]["channel_name"]
            col = mydb[channel_name]
            col.insert_one(file_upload)


elif opt == "Migration":
    st.title("YouTube Data Harvesting and Warehousing with MongoDB, SQL")  # Have to get the list channel_name here
    channel_id = st.text_input("Enter the channel id")
    available_channel_names = [channel['Channel_Name'] for channel in channel_list.find()]
    id_selected = st.selectbox("Select a channel name to migrate the data from mongodb to sql",
                               options=available_channel_names)
    migrate = st.button("Migrate")

    if migrate:
        channel_dict = channel(channel_id)
        channel_df = pd.DataFrame([channel_dict])
        channel_df["subscriber_count"] = channel_df["subscriber_count"].astype("int64")
        channel_df["channel_view_count"] = channel_df["channel_view_count"].astype("int64")
        #print(channel_df)
        mycursor.execute(
            "CREATE TABLE IF NOT EXISTS CHANNEL(CHANNEL_ID VARCHAR(50) PRIMARY KEY, CHANNEL_NAME VARCHAR(75), "
            "SUBSCRIBER_COUNT INT, CHANNEL_VIEW_COUNT INT, CHANNEL_DESCRIPTION TEXT)")
        connect.commit()
        a = "INSERT INTO CHANNEL(CHANNEL_ID, CHANNEL_NAME, SUBSCRIBER_COUNT, CHANNEL_VIEW_COUNT, CHANNEL_DESCRIPTION) "\
            "VALUES(%s, %s, %s, %s, %s)"
        for index, value in channel_df.iterrows():
            val = value["channel_id"]
            mycursor.execute(f"SELECT * FROM CHANNEL WHERE CHANNEL_ID = '{val}'")
            w = mycursor.fetchall()
            if len(w) > 0:
                mycursor.execute(f"DELETE FROM CHANNEL WHERE CHANNEL_ID = '{val}'")
                connect.commit()
            result_1 = (
                value["channel_id"], value["channel_name"], value["subscriber_count"], value["channel_view_count"],
                value["channel_des"])
            mycursor.execute(a, result_1)
        connect.commit()

        video_dict = video_2(channel_dict['playlist_id'])
        video_df = pd.DataFrame(video_dict)
        video_df["view_count"] = video_df["view_count"].astype("int64")
        video_df["like_count"] = video_df["like_count"].astype("int64")
        video_df["dislike_count"] = video_df["dislike_count"].astype("int64") 
        video_df["favorite_count"] = video_df["favorite_count"].astype("int64")
        video_df["video_duration"] = pd.to_timedelta(video_df["video_duration"])
        video_df["video_duration"] = video_df["video_duration"].dt.total_seconds().astype("int64")
        video_df['published_at'] = pd.to_datetime(video_df['published_at']).dt.date.astype("datetime64[ns]")
        #print(video_df.head(2))
        #print(video_df.info())
        mycursor.execute(
            "CREATE TABLE IF NOT EXISTS VIDEO(CHANNEL_ID VARCHAR(75), VIDEO_ID VARCHAR(100) PRIMARY KEY, VIDEO_TITLE "
            "TEXT, VIDEO_DESCRIPTION TEXT, VIDEO_DURATION INT, VIDEO_CAPTION VARCHAR(10), "
            "VIEW_COUNT INT, LIKE_COUNT INT, DISLIKE_COUNT INT, FAVOURITE_COUNT INT, COMMENT_COUNT INT, "
            "PUBLISHED_AT TIMESTAMP)")
        connect.commit()
        c = "INSERT INTO VIDEO(CHANNEL_ID, VIDEO_ID, VIDEO_TITLE, VIDEO_DESCRIPTION, VIDEO_DURATION, VIDEO_CAPTION, " \
            "VIEW_COUNT, LIKE_COUNT, DISLIKE_COUNT, FAVOURITE_COUNT, COMMENT_COUNT, PUBLISHED_AT) VALUES(%s, %s, %s, " \
            "%s, " \
            "%s, %s, %s, %s, %s, %s, %s, %s)"
        for index, value_2 in video_df.iterrows():
            val_2 = value_2["video_id"]
            mycursor.execute(f"SELECT * FROM VIDEO WHERE VIDEO_ID = '{val_2}'")
            y = mycursor.fetchall()
            if len(y) > 0:
                mycursor.execute(f"DELETE FROM VIDEO WHERE VIDEO_ID = '{val_2}'")
                connect.commit()
            result_3 = (
                value_2["channel_id"], value_2["video_id"], value_2["video_title"], value_2["video_description"],
                value_2["video_duration"], value_2["video_caption"], value_2["view_count"], value_2["like_count"],
                value_2["dislike_count"], value_2["favorite_count"], value_2["comment_count"], value_2["published_at"])
            mycursor.execute(c, result_3)
        connect.commit()

        comments_dict = video_comments(channel_dict['playlist_id'])
        comments_df = pd.DataFrame(comments_dict)
        comments_df['published_at'] = pd.to_datetime(comments_df['published_at']).dt.date
        #print(comments_df.info())
        #print(comments_df.head(2))
        mycursor.execute("CREATE TABLE IF NOT EXISTS COMMENTS(VIDEO_ID VARCHAR(75), COMMENT_ID VARCHAR(75) PRIMARY KEY, "
                    "PUBLISHED_AT VARCHAR(30), COMMENT_GIVEN TEXT)")
        connect.commit()
        d = "INSERT INTO COMMENTS(VIDEO_ID, COMMENT_ID, PUBLISHED_AT, COMMENT_GIVEN) VALUES(%s, %s, " \
            "%s, %s, %s)"
        for index, value_3 in comments_df.iterrows():
            val_3 = value_3["comment_id"]
            mycursor.execute(f"SELECT * FROM COMMENTS WHERE COMMENT_ID = '{val_3}'")
            z = mycursor.fetchall()
            if len(z) > 0:
                mycursor.execute(f"DELETE FROM COMMENTS WHERE COMMENT_ID = '{val_3}'")
                connect.commit()
            result_4 = (
                value_3["video_id"], value_3["comment_id"], value_3["published_at"], value_3["comment_given"])
            mycursor.execute(d, result_4)
        connect.commit()


else:
    st.title("YouTube Data Harvesting and Warehousing with MongoDB, SQL")
    st.subheader("Simplify and analyze the migrated data from the selected ten questions to gain valuable insights")

    question_list = ["Select", "What are the names of all the videos and their corresponding channels?",
                     "Which channels have the most number of videos, and how many videos do they have?",
                     "What are the top 10 most viewed videos and their respective channels?",
                     "How many comments were made on each video, and what are their corresponding video names?",
                     "Which videos have the highest number of likes, and what are their corresponding channel names?",
                     "What is the total number of likes and dislikes for each video, and what are their corresponding "
                     "video names?",
                     "What is the total number of views for each channel, and what are their corresponding channel "
                     "names?",
                     "What are the names of all the channels that have published videos in the year 2022?",
                     "What is the average duration of all videos in each channel, and what are their corresponding "
                     "channel names?",
                     "Which videos have the highest number of comments, and what are their corresponding channel names?"
                     ]
    question_selected = st.selectbox("Select the question from below options", options=question_list)

    if question_selected == "Select":
        st.write("   ")

    if question_selected == "What are the names of all the videos and their corresponding channels?":
        mycursor.execute("SELECT CHANNEL.CHANNEL_NAME, VIDEO.VIDEO_TITLE FROM CHANNEL JOIN VIDEO ON VIDEO.CHANNEL_ID = "
                    "CHANNEL.CHANNEL_ID")
        fetch = mycursor.fetchall()
        df = pd.DataFrame(fetch, columns=['Channel Name', 'Video title'])
        st.table(df)

    if question_selected == "Which channels have the most number of videos, and how many videos do they have?":
        mycursor.execute("SELECT CHANNEL.CHANNEL_NAME , COUNT(VIDEO_ID) AS VIDEO_COUNT FROM VIDEO JOIN CHANNEL ON "
                    "CHANNEL.CHANNEL_ID = VIDEO.CHANNEL_ID GROUP BY CHANNEL.CHANNEL_ID, VIDEO.CHANNEL_ID ORDER BY "
                    "VIDEO_COUNT DESC LIMIT 3")
        fetch = mycursor.fetchall()
        df = pd.DataFrame(fetch, columns=['Channel Name', 'Number of videos'])
        st.table(df)

    if question_selected == "What are the top 10 most viewed videos and their respective channels?":
        mycursor.execute("SELECT CHANNEL.CHANNEL_NAME, VIDEO.VIEW_COUNT FROM CHANNEL JOIN VIDEO ON VIDEO.CHANNEL_ID = "
                    "CHANNEL.CHANNEL_ID ORDER BY VIEW_COUNT DESC LIMIT 10")
        fetch = mycursor.fetchall()
        df = pd.DataFrame(fetch, columns=['Channel Name', 'Top view count'])
        st.table(df)

    if question_selected == "How many comments were made on each video, and what are their corresponding video names?":
        mycursor.execute("SELECT VIDEO.VIDEO_TITLE, COUNT(COMMENT_ID) AS TOTAL_COMMENT FROM COMMENTS JOIN VIDEO ON "
                    "VIDEO.VIDEO_ID = COMMENTS.VIDEO_ID GROUP BY VIDEO_TITLE ORDER BY TOTAL_COMMENT DESC")
        fetch = mycursor.fetchall()
        df = pd.DataFrame(fetch, columns=['Video Title', 'Comments count'])
        st.table(df)

    if question_selected == "Which videos have the highest number of likes, and what are their corresponding channel " \
                            "names?":
        mycursor.execute("SELECT CHANNEL.CHANNEL_NAME, VIDEO.VIDEO_TITLE, VIDEO.LIKE_COUNT FROM VIDEO JOIN CHANNEL ON "
                    "VIDEO.CHANNEL_ID = CHANNEL.CHANNEL_ID ORDER BY VIDEO.LIKE_COUNT DESC LIMIT 10")
        fetch = mycursor.fetchall()
        df = pd.DataFrame(fetch, columns=['Channel Name', 'Video Title', 'Like Count'])
        st.table(df)

    if question_selected == "What is the total number of likes and dislikes for each video, and what are their " \
                            "corresponding video names?":
        mycursor.execute("SELECT VIDEO_TITLE, LIKE_COUNT, DISLIKE_COUNT FROM VIDEO")
        fetch = mycursor.fetchall()
        df = pd.DataFrame(fetch, columns=['Video Title', 'Like Count', 'Dislike Count'])
        st.table(df)

    if question_selected == "What is the total number of views for each channel, and what are their corresponding " \
                            "channel names?":
        mycursor.execute("SELECT CHANNEL_NAME, CHANNEL_VIEW_COUNT FROM CHANNEL ORDER BY CHANNEL_VIEW_COUNT DESC")
        fetch = mycursor.fetchall()
        df = pd.DataFrame(fetch, columns=['Channel Name', 'View Count'])
        st.table(df)

    if question_selected == "What are the names of all the channels that have published videos in the year 2022?":
        mycursor.execute("SELECT CHANNEL_NAME FROM CHANNEL JOIN VIDEO ON CHANNEL.CHANNEL_ID = VIDEO.CHANNEL_ID WHERE "
                    "EXTRACT(YEAR FROM VIDEO.PUBLISHED_AT) = 2022 GROUP BY CHANNEL.CHANNEL_NAME")
        fetch = mycursor.fetchall()
        df = pd.DataFrame(fetch, columns=['Channels published video in the year 2022'])
        st.table(df)

if question_selected == "What is the average duration of all videos in each channel, and what are their " \
                           "corresponding channel names?":
        mycursor.execute("SELECT CHANNEL.CHANNEL_NAME, AVG(VIDEO.VIDEO_DURATION) FROM VIDEO JOIN "
                    "CHANNEL ON CHANNEL.CHANNEL_ID = VIDEO.CHANNEL_ID GROUP BY CHANNEL.CHANNEL_ID")
        fetch = mycursor.fetchall()
        df = pd.DataFrame(fetch, columns=['Channel Name', 'Average video duration in Seconds'])
        st.table(df)


if question_selected == "Which videos have the highest number of comments, and what are their corresponding " \
                            "channel names?":
        mycursor.execute("SELECT CHANNEL.CHANNEL_NAME, VIDEO.VIDEO_TITLE, VIDEO.COMMENT_COUNT FROM VIDEO JOIN CHANNEL ON "
                    "CHANNEL.CHANNEL_ID = VIDEO.CHANNEL_ID ORDER BY VIDEO.COMMENT_COUNT DESC LIMIT 10")
        fetch = mycursor.fetchall()
        df = pd.DataFrame(fetch, columns=['Channel Name', 'Video Title', 'Comment count'])
        st.table(df)

# Close the cursor and database connection
mycursor.close()
connect.close()
