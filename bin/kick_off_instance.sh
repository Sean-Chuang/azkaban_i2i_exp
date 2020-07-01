#!/bin/bash

set -eu

usage() {
  echo "Usage: kick_off_instance.sh [options] {instance_type} {step_name} {instance_name} [{spot_price}]

Launch EC2 instances

Options:
  -t <key>=<value>  EC2 instance tags
  -h                This help screen"

  exit 0
}

tag_opts=("")
while getopts 'ht:' flag; do
    case ${flag} in
        h) usage ;;
        t) tag_opts+=(-t "${OPTARG}") ;;
    esac
done

shift $((OPTIND - 1))

if [ $# -lt 3 -o $# -gt 4 ]; then
    usage
fi

ec2tp="$1"
task="$2"
name="$3"

price=""
if [ $# -gt 3 ]; then
    price="$4"
fi

oridir=$(pwd)

tmphome='/mnt/tmp/'
mkdir -p $tmphome
wd=$(mktemp -d -p $tmphome)

cd $wd

git clone --recursive git@github.com:smartnews/smart-ad-dmp.git

cd smart-ad-dmp/emr-op

tags="${tag_opts[@]}"
if [ -z "$price" ]; then
    ec2/launch_node.sh $tags ${ec2tp} 1 ${name} ${task}
else
    ec2/launch_spot_node.sh $tags ${ec2tp} ${price} 1 ${name} ${task}
fi

cd $oridir && rm -rf $wd