VERSION=${VERSION:-${1:-latest}}

docker build --build-arg VERSION=${VERSION} -t "buzzobuono/waluigi-bossd:${VERSION}" -f Dockerfile.bossd --no-cache .

docker build --build-arg VERSION=${VERSION} -t "buzzobuono/waluigi-workerd:${VERSION}" -f Dockerfile.workerd --no-cache .
