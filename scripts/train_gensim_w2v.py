#!/usr/bin/env python3
import sys
import time
import argparse
import logging
from gensim.models import word2vec

def main(data_path, model_path):
    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
    logging.info(f"data_path : {data_path}")
    logging.info(f"model_path : {model_path}")
    sentences = word2vec.LineSentence(data_path)
    model = word2vec.Word2Vec(sentences, iter=8, size=128, alpha=0.2, sg=0, window=15, min_count=3, workers=30,)
    model.wv.save_word2vec_format(model_path)

if __name__ == "__main__":
    parser = argparse.ArgumentParser("python3 train_gensim_w2v.py")
    parser.add_argument("data", type=str, help="Train data")
    parser.add_argument("model", type=str, help="Save model path")
    args = parser.parse_args()
    b_time = time.time()
    print('[Start] Prepare to train')
    main(args.data, args.model)
    print('Finished! Cost time : ', time.time() - b_time)