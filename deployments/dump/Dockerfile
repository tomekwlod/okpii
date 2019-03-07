# First step - just building the go app
FROM golang:1.11.5 as builder
ENV WORKDIR /go/src/app
WORKDIR ${WORKDIR}
COPY . .
RUN go get -u github.com/golang/dep/cmd/dep \
    && dep init && dep ensure \
    && cd ${WORKDIR}/cmd/dump \
    && CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o dump .

ENTRYPOINT [ "./cmd/dump/dump" ]
