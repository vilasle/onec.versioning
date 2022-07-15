FROM ubuntu

EXPOSE 5432

ENV TZ=Asia/Yekaterinburg

RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && \
    echo $TZ > /etc/timezone
