# First step - just building the go app
FROM golang:1.11.5 as builder

ENV WORKDIR /go/src/app
WORKDIR ${WORKDIR}
COPY . .

RUN go get -u github.com/golang/dep/cmd/dep \
    && cd ${WORKDIR}/cmd/matching \
    && dep init && dep ensure \
    && CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o matching .

# # Second step - copying the files and running the exec
FROM alpine:3.8

RUN apk --no-cache add ca-certificates
ENV STATICPATH=static
WORKDIR /root/
COPY --from=builder /go/src/app/cmd/matching/matching .

# CMD [ "./matching" ]
