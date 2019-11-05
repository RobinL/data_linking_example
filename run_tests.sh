docker build -t dt -f Dockerfile_testrunner .
docker run --rm dt pytest -v -s
