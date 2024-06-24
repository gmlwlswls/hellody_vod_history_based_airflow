# import
import pandas as pd
import numpy as np
from pymongo import MongoClient
from sklearn.cluster import KMeans
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

# df['timestamp'] = pd.to_datetime(df['timestamp'], yearfirst= True, format= '%Y%m%d%H%M%S')
# df['month'] = df['timestamp'].dt.month

# user_train_df = df[df['month'] != 3] #780
# user_test_df = df[df['month'] == 3] #335 ** index

movie_history_ids = df['vod_id'].to_list() #1165
movie_to_vod_id = []
for movie_id in movie_history_ids:
    movie_list = MOVIES.find_one(
        {'MOVIE_ID': movie_id},
        {'_id': 0, 'VOD_ID' : 1}
    )
    if movie_list is not None:
        movie_to_vod_id.append(movie_list)

movie_to_vod_id_df = pd.DataFrame(movie_to_vod_id)
movie_to_vod_id_list = movie_to_vod_id_df['VOD_ID'].to_list()

df['movie_id_to_vod_id'] = movie_to_vod_id_list
del df['vod_id']
df = df[['user_id', 'movie_id_to_vod_id', 'timestamp']]
df.columns = ['user_id', 'vod_id', 'timestamp']

# 사용자-영화 행렬
# 204사용자 * 290영화
user_movie_df = pd.crosstab(index=df['user_id'], columns=df['vod_id'], dropna=False, values=1, aggfunc='sum')
user_movie_df.fillna(0, inplace=True)
user_movie_df = user_movie_df.astype(int)
user_movie_matrix = user_movie_df.to_numpy()

def jaccard_similarity(row1, row2):
    intersection = np.logical_and(row1, row2).sum()
    union = np.logical_or(row1, row2).sum()
    if union == 0:
        return 0
    else:
        return intersection / union

num_users = user_movie_matrix.shape[0]
user_sim_matrix = np.zeros((num_users, num_users))

for i in range(num_users):
    for j in range(num_users):
        user_sim_matrix[i, j] = jaccard_similarity(user_movie_matrix[i], user_movie_matrix[j])

# 유사도가 1이면 완전히 동일한 시청 리스트
# 유사도가 0이면 교집합이 없는 시청 리스트

num_clusters = 5
kmeans = KMeans(n_clusters=num_clusters, random_state=42)
user_clusters = kmeans.fit_predict(user_sim_matrix)

# 사용자 클러스터 정보 추가
user_clusters_series = pd.Series(user_clusters, index=user_movie_df.index)
# user_train_df에 'cluster' 열 추가
df['cluster'] = df['user_id'].map(user_clusters_series)

df.to_csv('./user_train_df.csv', index = False)
