#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
IMG=alexishuf/hdtss
PUSH=n
DO_JDK=y
DO_NATIVE=y
cd "$DIR"

function help() {
  echo -e "Usage: build-docker.sh [-hpNJ] [-i IMAGE_NAME] [-v VERSION] \n" \
          "\n" \
          "Options: \n"\
          "   -h Show this help message\n" \
          "   -p Push built images to docker hub\n" \
          "   -N Skip building the native-tagged images\n" \
          "   -J Skip building the jdk-tagged images (which include the \n" \
          "      :-\$VERSION and :latest tags)\n" \
          "   -i IMAGE_NAME\n" \
          "      Use IMAGE_NAME instead of alexishuf/hdtss as the image name\n" \
          "   -v VERSION\n" \
          "      Do not read <version> from the pom.xml file, use VERSION instead"
}


while getopts "hpNJ" o; do
  case $o in
  h)
    help && exit 0 ;;
  p)
    PUSH=y ;;
  N)
    DO_NATIVE=n ;;
  J)
    DO_JDK=n ;;
  i)
    IMG="$OPTARG" ;;
  v)
    VERSION="$OPTARG" ;;
  *)
    echo "Unexpected option $o" 1>&2; help 1>&2 ; exit 1 ;;
  esac
done

# Exit on first error
set -e

# Get version
if [ -z "$VERSION" ]; then
  VERSION=$(sed -nE 's@^  <version>(.*)</version>@\1@p' pom.xml)
  if ! echo "$VERSION" | grep -E '^[0-9]+\.[0-9+]'; then
    echo "Something went wrong when extracting the <version> from pom.xml"
    exit 1
  fi
fi

# Build JDK variant
if [ "$DO_JDK" == "y" ]; then
  docker build -f jdk.Dockerfile -t $IMG:$VERSION-jdk -t $IMG:jdk -t $IMG .
fi

# Build native image
test "$DO_NATIVE" == "y" && \
  docker build -f native.Dockerfile -t $IMG:$VERSION-native -t $IMG:native .

# Push images
if [ "$PUSH" == "y" ]; then
  if [ "$DO_JDK" == "y" ]; then
    docker push $IMG:$VERSION-jdk
    docker push $IMG:$VERSION
    docker push $IMG:jdk
    docker push $IMG
  fi
  if [ "$DO_NATIVE" == "y" ]; then
      docker push $IMG:$VERSION-native
      docker push $IMG:native
  fi
fi

