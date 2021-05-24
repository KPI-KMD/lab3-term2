FROM golang:1.15 as build

RUN apt-get update && apt-get install -y ninja-build

# TODO: Змініть на власну реалізацію системи збірки. DONE
RUN go get -u github.com/andreichenko256/lab1-2ndTerm/build/cmd/bood
WORKDIR /go/src/practice-2
COPY . .

#RUN CGO_ENABLED=0 bood
RUN CGO_ENABLED=0 bood out/bin/server out/server/bood_test out/bin/lb out/server/bood_test


# ==== Final image ====
FROM alpine:3.11
WORKDIR /opt/practice-2
COPY entry.sh ./
COPY --from=build /go/src/practice-2/out/bin/* ./
ENTRYPOINT ["/opt/practice-2/entry.sh"]
CMD ["server"]

