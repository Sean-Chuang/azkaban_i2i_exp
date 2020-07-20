#!/bin/bash
set -eu
basedir=$(cd $(dirname "$0")/.. && pwd)

cd ~
if [ ! -d smart-ad-dmp ]; then
    git clone --recursive git@github.com:smartnews/smart-ad-dmp.git
    cd
fi

# setup env
curl https://bootstrap.pypa.io/get-pip.py | python3.6 - --user
pip3 install tqdm \
			numpy \
			scikit-learn \
			gensim \
			pandas \
			pyhive \
			annoy

rm -rf /mnt1/train
mkdir -p /mnt1/train

# prepare training set
dt="$(date -d '1 day ago' '+%Y-%m-%d')"
echo ${dt}

cd ~/smart-ad-dmp/azkaban-flow/datafeed
s3_prefix="smartad-dmp/warehouse/user/seanchuang/i2i_offline_w2v_train_data/dt=${dt}"
bash bin/s3_sync_mnt "/mnt1/train" ${s3_prefix}
data="/mnt1/train/${s3_prefix}/merged.data"
du -khs ${data}

# train gensum w2v model
cd ~
model="/mnt1/train/behaviors.vec"
python3 azkaban_i2i_exp/scripts/train_w2v.py ${data} ${model}
du -khs ${model}

# build ANN / find similar topK items / insert DDB
catalog_table="hive.maeda.rakuten_rpp_datafeed"
topK=20
ddb_table="dev_dynamic_ads_similar_items"
label="rakuten_shopping"
backup_file="/mnt1/train/similar_items_res.csv"
python3 azkaban_i2i_exp/scripts/insert_similar_topK_ddb.py \
								${catalog_table} \
								${model} \
								${topK} \
								${ddb_table} \
								${label} \
								${backup_file}


# copy vectors to feature table
s3url="s3://smartad-dmp/warehouse/user/seanchuang/tmp_i2i_items_similar_backup/dt=${dt}/"
aws s3 cp ${backup_file} ${s3url}
