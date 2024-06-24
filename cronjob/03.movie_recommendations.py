import pandas as pd
from pymongo import MongoClient

#필요한 csv 파일
user_train_df = pd.read_csv('./user_train_df.csv')
movie_train_df = pd.read_csv('./movie_train_df.csv', index_col= 'VOD_ID')
movie_train_df.columns = movie_train_df.columns.astype(int)
popular_vod_df = pd.read_csv('./popular_hellody_df.csv')
popular_vod_list = popular_vod_df.VOD_ID.to_list()

#추천 함수
def get_recommendations(user_id, user_train_df, movie_train_df, popular_vod_list, top_n=20):
    # 사용자가 데이터프레임에 존재하는지 확인
    if user_id not in user_train_df['user_id'].values:
        print(f"User ID {user_id} not found in user_train_df")
        return []

    # 속한 클러스터 찾기
    user_cluster = user_train_df.loc[user_train_df['user_id'] == user_id, 'cluster'].iloc[0]
    user_watched_list = list(set(user_train_df.loc[user_train_df['user_id'] == user_id, 'vod_id']))
    cluster_watched_list = list(set(user_train_df[user_train_df['cluster'] == user_cluster]['vod_id']) - set(user_watched_list))

    # 사용자가 본 영화와 군집이 본 영화 간의 코사인 유사도 데이터프레임 생성 
    # 군집별 추천 영화 확인해봐야 함
    watched_sim_df = pd.DataFrame(index=user_watched_list, columns=cluster_watched_list)

    for user_movie_id in user_watched_list:
        for cluster_movie_id in cluster_watched_list:
            try:
                watched_sim_df.loc[user_movie_id, cluster_movie_id] = movie_train_df.loc[user_movie_id, cluster_movie_id]
            except KeyError:
                print(f"KeyError: {user_movie_id}, {cluster_movie_id}")
                watched_sim_df.loc[user_movie_id, cluster_movie_id] = 0.0

    watched_sim_df = watched_sim_df.astype(float)

    top_similar_movies_dict = {}
    for user_movie_id in user_watched_list:
        top_similar_movies_dict[user_movie_id] = watched_sim_df.loc[user_movie_id].nlargest(20).index.tolist()

    # 리스트로 변환하고 중복 제거
    top_similar_movies_list = [movie_id for movie_ids in top_similar_movies_dict.values() for movie_id in movie_ids]
    top_similar_movies_set = set(top_similar_movies_list)

    # top_n 개수 만큼 추천 영화 리스트 생성
    recommended_movies = list(top_similar_movies_set)[:top_n]

    # 추천 영화 개수가 top_n보다 적을 경우 인기 VOD로 채우기
    if len(recommended_movies) < top_n:
        additional_movies = [movie for movie in popular_vod_list if movie not in recommended_movies and movie not in user_watched_list]
        recommended_movies.extend(additional_movies[:top_n - len(recommended_movies)])

    return recommended_movies

# 추천 생성
recommend_for_id = {}
for user_id in user_train_df['user_id'].values:
    recommend_for_id[user_id] = get_recommendations(user_id, user_train_df, movie_train_df, popular_vod_list)

# print(recommend_for_id)

#MongoDB 추천 vod_list 업데이트
client = MongoClient("mongodb://wang:0131@3.37.201.211:27017")

db = client['hellody']
movies = db['MOVIES']
recommend_list = db['recommend_list']

# MongoDB 추천 VOD 리스트 업데이트
for user_id, vod_id_list in recommend_for_id.items():
    result = []
    for vod_id in vod_id_list :
        movie = movies.find_one(
            {'$and' : [
                {'VOD_ID' : int(vod_id)},
                {'MOVIE_RATING' : {'ne' : '18'}},
                {'MOVIE_RATING' : {'ne' : 18}},
                {'MOVIE_RATING' : {'$ne' : None}}
            ]},
            { "_id": 0, "VOD_ID":1,"TITLE": 1, "POSTER": 1 }
        )
        result.append(movie)
    # print(result)

    recommend_list.update_one(
              { "user_id": int(user_id) },
              { 
                "$set": {
                    "vod_history": result
          }
      },
      upsert=True  # 존재하지 않는 경우 새로 삽입
    )

print("MongoDB 업데이트 완료")

# # # mySQL에서 vod_id, poster 가져오기
# # # mySQL 연결
# # try:
# #     conn = mysql.connector.connect(
# #         host='hellovision.c3gk86ic62pt.ap-northeast-2.rds.amazonaws.com',  # MySQL 서버 주소
# #         user='root',   # 사용자 이름
# #         password='12340131',   # 사용자 비밀번호
# #         database='hellovision'  # 데이터베이스 이름
# #     )
# #     print("MySQL 연결 성공")
# # except mysql.connector.Error as err:
# #     print(f"MySQL 연결 에러: {err}")
# #     conn = None

# # client = MongoClient("mongodb://3.37.201.211:27017")
# # db = client['hellody']
# # # MongoDB 추천 VOD 리스트 업데이트
# # for user_id, movie_id_list in recommend_for_id.items():
# #     for movie_id in movie_id_list:
# #         if conn:
# #             try:
# #                 # 커서 생성
# #                 cursor = conn.cursor()
                
# #                 # SQL 쿼리 작성
# #                 query = "SELECT TITLE,POSTER,VOD_ID FROM MOVIES WHERE (MOVIE_ID = {})".format(movie_id)
                
# #                 # SQL 쿼리 실행
# #                 cursor.execute(query)
                
# #                 # 결과 가져오기
# #                 results = cursor.fetchall()  # 결과 리스트
# #                 print("Results:", results) # 튜플(title, poster, vod_id) 의 리스트
# #             except mysql.connector.Error as err:
# #                 print(f"SQL 쿼리 실행 에러: {err}")
# #             except ValueError as val_err:
# #                 print(f"Value Error: {val_err}")
# #             finally:
# #                 if cursor:
# #                     cursor.close()
# #                 if conn:
# #                     conn.close()
# #         else:
# #             print("데이터베이스에 연결되지 않았습니다.")