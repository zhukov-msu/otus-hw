FROM python/3.8-slim
LABEL authors="a.zhukov"

ENTRYPOINT ["top", "-b"]