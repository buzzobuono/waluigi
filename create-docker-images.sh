VERSION=${VERSION:-${1:-latest}}

docker build --build-arg VERSION=${VERSION} -t "buzzobuono/waluigi-boss:${VERSION}" -f Dockerfile.boss --no-cache .

docker build --build-arg VERSION=${VERSION} -t "buzzobuono/waluigi-worker:${VERSION}" -f Dockerfile.worker --no-cache .
