FROM golang:1.11-alpine3.7 as builder

WORKDIR /go/src/github.com/kubeflow/pipelines
COPY . .

# Needed musl-dev for github.com/mattn/go-sqlite3
RUN apk update && apk upgrade && \
    apk add --no-cache bash git openssh gcc musl-dev

RUN GO111MODULE=on go build -o /bin/apiserver backend/src/apiserver/*.go

FROM python:3.5 as sample-compiler
RUN pip3 install PyYAML kubernetes

COPY . /pipelines

#We need to check that all samples have been compiled without error.
#For find program, the -exec argument is a filter predicate just like -name. It only affects whether the file is "found", not the find's exit code.
#One way to solve this problem is to check whether we have any python pipelines that cannot compile. Here the exit code is the number of such files:
#RUN  bash -e -c 'exit $(find . -maxdepth 2 -name "*.py" ! -exec dsl-compile --py {} --output {}.tar.gz \; -print | wc -l)'
#I think it's better to just use a shell loop though.
#RUN  for pipeline in $(find . -maxdepth 2 -name '*.py' -type f); do dsl-compile --py "$pipeline" --output "$pipeline.tar.gz"; done
#The "for" loop breaks on all whitespace, so we either need to override IFS or use the "read" command instead.
RUN  find /pipelines/samples -maxdepth 2 -name '*.py' -type f | while read pipeline; do PYTHONPATH=/pipelines/sdk/python "$pipeline"; done

FROM alpine:3.8

ARG COMMIT_SHA=unknown
ENV COMMIT_SHA=${COMMIT_SHA}

WORKDIR /bin

COPY --from=builder /bin/apiserver /bin/apiserver
COPY --from=builder /go/src/github.com/kubeflow/pipelines/third_party/license.txt /bin/license.txt
COPY backend/src/apiserver/config/ /config

COPY --from=sample-compiler /pipelines/samples /samples

# Adding CA certificate so API server can download pipeline through URL
RUN apk add ca-certificates

# Expose apiserver port
EXPOSE 8888

# Start the apiserver
CMD apiserver --config=/config --sampleconfig=/config/sample_config.json -alsologtostderr=true
