#!/usr/bin/env python3
import sys
import time
import argparse
import logging
from gensim.models import word2vec

def main(data, model):
    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
    sentences = word2vec.LineSentence(data)
    model = word2vec.Word2Vec(sentences, iter=8, size=128, alpha=0.2, sg=0, window=15, min_count=3, workers=30,)
    model.wv.save_word2vec_format(model)

if __name__ == "__main__":
    parser = argparse.ArgumentParser("python3 train_gensim_w2v.py")
    parser.add_argument("data", type=str, help="Train data")
    parser.add_argument("model", type=str, help="Save model path")
    args = parser.parse_args()
    b_time = time.time()
    print('[Start] Prepare to train')
    main(args.data, args.model)
    print('Finished! Cost time : ', time.time() - b_time)