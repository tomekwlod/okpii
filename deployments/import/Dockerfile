# First step - just building the go app
FROM golang:1.11.5 as ibuilder

ENV WORKDIR /go/src/app

WORKDIR ${WORKDIR}
COPY . .
RUN go get -u github.com/golang/dep/cmd/dep \
    && dep init && dep ensure \
    && cd ${WORKDIR}/cmd/import \
    && CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o import .

# # Second step - copying the files and running the exec
FROM alpine:3.8
RUN apk --no-cache add ca-certificates
WORKDIR /root/

COPY --from=ibuilder /go/src/app/cmd/import/import .

CMD [ "./import" ]
