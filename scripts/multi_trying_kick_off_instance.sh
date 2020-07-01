#!/bin/bash

set -eu
basedir=$(cd $(dirname "$0")/.. && pwd)

tag_opts=(-t "Owner=$(id -u -n)@smartnews.com")
while getopts 't:' flag; do
    case ${flag} in
        t) tag_opts+=(-t "${OPTARG}") ;;
    esac
done

shift $((OPTIND - 1))

ec2types=${1//,/$'\n'}
for ec2type in ${ec2types}
do
    cmd="bash $basedir/bin/kick_off_instance.sh ${tag_opts[@]} ${ec2type} $2 $3"
    echo "run command: ${cmd}"
    bash -c "${cmd}" && exit 0
done
