# start container
arg_len="$#";
arg="$@";

if [ $arg_len == "0" ]; then
    docker start -i popcorn
elif [ $arg_len == "1" ]; then
    docker start -i $arg
else
    echo "invalid params"
fi
