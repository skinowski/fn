set -e
make -f ./Makefile
ARCH=$(uname -m)
docker build --build-arg ARCH=${ARCH} --build-arg HTTPS_PROXY --build-arg HTTP_PROXY -t fnproject/pause-${ARCH}:latest .
