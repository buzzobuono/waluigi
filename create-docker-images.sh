VERSION=${VERSION:-${1:-latest}}

docker build -t "buzzobuono/waluigi:${VERSION}" --no-cache .
