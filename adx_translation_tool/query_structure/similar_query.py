import json
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np


class SimilarQuery:
    def __init__(self, ground_truths_path):
        self.ground_truths = self._load_ground_truths(ground_truths_path)
        self.vectorizer = TfidfVectorizer(
            tokenizer=lambda x: x.split(), stop_words='english')

    def _load_ground_truths(self, ground_truths_path):
        with open(ground_truths_path, 'r') as f:
            return json.load(f)

    def get_top_k_similar_queries(self, kusto_query, k=1):
        similarities = [
            (i, self.compute_similarity(kusto_query, gt['kql']))
            for i, gt in enumerate(self.ground_truths)
        ]
        top_k_indices = sorted(
            similarities, key=lambda x: x[1], reverse=True)[:k]
        return [self.ground_truths[i] for i, _ in top_k_indices]

    def group_by_tfidf(self, kusto_queries, similarity_threshold=0.7):
        tf_idf_matrix = self._compute_tf_idf_matrix(kusto_queries)
        cosine_sim_matrix = cosine_similarity(tf_idf_matrix)
        labels = [-1] * len(kusto_queries)
        current_group_id = 0

        for i in range(len(kusto_queries)):
            if labels[i] != -1:
                continue

            similar_queries = np.where(
                cosine_sim_matrix[i] >= similarity_threshold)[0]

            if len(similar_queries) > 1:
                for idx in similar_queries:
                    labels[idx] = current_group_id
                current_group_id += 1

        return labels

    def compute_similarity(self, q1, q2):
        tf_idf_matrix = self._compute_tf_idf_matrix([q1, q2])
        return cosine_similarity(tf_idf_matrix)[0, 1]

    def _compute_tf_idf_matrix(self, queries):
        return self.vectorizer.fit_transform(queries)
