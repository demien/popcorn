# attatch to popcorn container
# start container
arg_len="$#";
arg="$@";

if [ $arg_len == "0" ]; then
    docker exec -it popcorn /bin/bash
elif [ $arg_len == "1" ]; then
    docker exec -it $arg /bin/bash
else
    echo "invalid params"
fi
