# build image
ABSOLUTE_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
docker build -t demien/popcorn $ABSOLUTE_PATH/..
