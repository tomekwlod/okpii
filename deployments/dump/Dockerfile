# First step - just building the go app
FROM golang:1.11.5 as builder

ENV WORKDIR /go/src/app
WORKDIR ${WORKDIR}
COPY . .

RUN git config --global url."git@github.com:".insteadOf "https://github.com/" \
    && go get -u github.com/golang/dep/cmd/dep \
    && dep init && dep ensure \
    && cd ${WORKDIR}/cmd/dump \
    && CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o dump .

# # Second step - copying the files and running the exec
FROM alpine:3.8

RUN apk --no-cache add ca-certificates
ENV STATICPATH=static
WORKDIR /root/
COPY --from=builder /go/src/app/cmd/dump/dump .

# CMD [ "./dump" ]
