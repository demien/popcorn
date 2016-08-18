# create container
ABSOLUTE_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_PATH=$ABSOLUTE_PATH/../../..

arg_len="$#";
arg="$@";

if [ $arg_len == "0" ]; then
    docker run -i -t -p 5555:5555 -p 9090:9090 -p 6379:6379 --name popcorn -v $BASE_PATH/celery:/var/celery/ -v $BASE_PATH/popcorn:/var/popcorn/ -w /var/popcorn/examples demien/popcorn
elif [ $arg_len == "1" ]; then
    docker run -i -t --name $arg --link popcorn:popcorn -v $BASE_PATH/celery:/var/celery/ -v $BASE_PATH/popcorn:/var/popcorn/ -w /var/popcorn/examples demien/popcorn
else
    echo "invalid params"
fi
