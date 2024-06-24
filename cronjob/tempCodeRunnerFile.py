
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
