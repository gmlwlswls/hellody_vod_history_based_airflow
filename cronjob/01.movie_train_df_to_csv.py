# import
import pandas as pd
from pymongo import MongoClient
from konlpy.tag import Okt
from sklearn.metrics.pairwise import linear_kernel
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.feature_extraction.text import CountVectorizer
from dotenv import load_dotenv
import os
import boto3
import chardet
import io

load_dotenv()
url = "mongodb://wang:0131@3.37.201.211:27017"
client = MongoClient(url)

db = client['hellody']
MOVIES = db['MOVIES']

# AWS key
S3_ACCESS_KEY = os.getenv("AWS_S3_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("AWS_S3_SECRET_KEY")

# S3 클라이언트 생성
s3 = boto3.client("s3", aws_access_key_id=S3_ACCESS_KEY, aws_secret_access_key=S3_SECRET_KEY)

# S3 버킷과 파일 경로 설정
bucket = 'spotifymodel'
s3_dir = 'watch-data/watchdata.csv'

# S3에서 파일을 읽어와서 pandas 데이터프레임으로 변환
def read_s3_csv_to_dataframe(bucket, s3_dir):
    # S3에서 파일 가져오기
    response = s3.get_object(Bucket=bucket, Key=s3_dir)
    content = response['Body'].read()
    
    # 파일의 인코딩 감지
    result = chardet.detect(content)
    encoding = result['encoding']
    
    # 데이터프레임으로 읽기
    df = pd.read_csv(io.StringIO(content.decode(encoding)))
    
    return df

# 데이터프레임 읽기
df = read_s3_csv_to_dataframe(bucket, s3_dir)
movie_history_ids = df['vod_id'].drop_duplicates().to_list()

movie_feature_data = []
for movie_id in movie_history_ids:
    movie_list = MOVIES.find_one(
        {'MOVIE_ID': movie_id},
        {'_id': 0, 'VOD_ID' : 1, 'TITLE': 1, 'GENRE' : 1, 'MOVIE_OVERVIEW' : 1, 'CAST': 1, 'CREW': 1}
    )
    if movie_list is not None:
        movie_feature_data.append(movie_list)

movie_feature_df = pd.DataFrame(movie_feature_data)
movie_feature_df = movie_feature_df[['VOD_ID', 'TITLE', 'GENRE', 'MOVIE_OVERVIEW', 'CAST', 'CREW']]

#title,genre : 한글 / 'names_actor', 'names_crew' : 한글+영어, 'job_crew' , 'keyword': 영어,
names_feature = ['CAST', 'CREW'] 

# 배우/감독
# 문자열 소문자/ 공백 제거 > "Johnny Depp"과 "Johnny Galecki"의 'Johnny'를 동일하게 취급하지 않게 함
def clean_data(x):
    if isinstance(x, list):
        return [str.lower(i.replace(" ", "")) for i in x]
    else:
        #Check if director exists. If not, return empty string
        if isinstance(x, str):
            return str.lower(x.replace(" ", ""))
        else:
            return ''
        
#조사, 접속사 제거  
okt = Okt()
def tokenize(text):
    return okt.nouns(text)       

for feature in names_feature:
    movie_feature_df.loc[:, feature] = movie_feature_df[feature].apply(clean_data)

tfidf = TfidfVectorizer(tokenizer=tokenize)
tfidf_matrix = tfidf.fit_transform(movie_feature_df['MOVIE_OVERVIEW'])
overview_cosine_sim = linear_kernel(tfidf_matrix, tfidf_matrix)

# 각 장르의 등장 횟수를 특성으로 하는 희소 행렬
# 만약, (1,4) 2 이면 영화1의 장르4가 2번 등장
count_vectorizer = CountVectorizer()
genre_matrix = count_vectorizer.fit_transform(movie_feature_df['GENRE'])
crew_matrix = count_vectorizer.fit_transform(movie_feature_df['CREW'])
cast_matrix = count_vectorizer.fit_transform(movie_feature_df['CAST'])

# 영화 간 장르 유사도
genre_cosine_sim = cosine_similarity(genre_matrix, genre_matrix)
crew_cosine_sim = cosine_similarity(crew_matrix, crew_matrix)
cast_cosine_sim = cosine_similarity(cast_matrix, cast_matrix)

# 종합 유사도 계산
combined_cosine_sim = genre_cosine_sim + crew_cosine_sim + cast_cosine_sim + overview_cosine_sim
combined_cosine_sim_df = pd.DataFrame(combined_cosine_sim, index = movie_feature_df['VOD_ID'], columns= movie_feature_df['VOD_ID'])
print(combined_cosine_sim_df)

combined_cosine_sim_df.to_csv('./movie_train_df.csv', index= True)


# # 메타 데이터
# # 임의로 mysql에서 가져오기(원래는 mongoDB)
# # MySQL 데이터베이스에 연결
# try:
#     conn = mysql.connector.connect(
#         host='hellovision.c3gk86ic62pt.ap-northeast-2.rds.amazonaws.com',  # MySQL 서버 주소
#         user='root',   # 사용자 이름
#         password='12340131',   # 사용자 비밀번호
#         database='hellovision'  # 데이터베이스 이름
#     )
#     print("MySQL 연결 성공")
# except mysql.connector.Error as err:
#     print(f"MySQL 연결 에러: {err}")
#     conn = None

# if conn:
#     try:
#         # 커서 생성
#         cursor = conn.cursor()
#         vod_history_ids = user_train_df['vod_ID'].drop_duplicates().to_list()

#         if not vod_history_ids:
#             raise ValueError("vod_history_ids 리스트가 비어 있습니다.")

#         # SQL 쿼리 작성
#         query = "SELECT * FROM MOVIES WHERE MOVIE_ID IN (%s)" % ','.join(['%s'] * len(vod_history_ids))
#         print("SQL Query:", query)

#         # SQL 쿼리 실행
#         cursor.execute(query, vod_history_ids)

#         # 결과 가져오기
#         results = cursor.fetchall()  # 결과 리스트
#         print("Results:", results)

#         if not results:
#             raise ValueError("쿼리 결과가 없습니다.")

#         # 데이터프레임으로 변환
#         column_names = [i[0] for i in cursor.description]  # 컬럼명
#         movies_df = pd.DataFrame(results, columns=column_names)

#         # 결과 출력
#         feature_df = movies_df[['MOVIE_ID', 'TITLE', 'GENRE', 'MOVIE_OVERVIEW', 'CAST', 'CREW']]
#         print("Feature DataFrame:")
#         print(feature_df)

#     except mysql.connector.Error as err:
#         print(f"SQL 쿼리 실행 에러: {err}")
#     except ValueError as val_err:
#         print(f"Value Error: {val_err}")
#     finally:
#         if cursor:
#             cursor.close()
#         if conn:
#             conn.close()
# else:
#     print("데이터베이스에 연결되지 않았습니다.")

# #배우 3명
# for i in range(len(feature_df['CAST'])):
#     feature_df['CAST'] = feature_df['CAST'].apply(lambda x: x.split(',')[:3] if isinstance(x, str) else x)
#     feature_df['CAST'] = feature_df['CAST'].apply(lambda x: ','.join(x))