FROM python:3
WORKDIR /usr/src/app
COPY . .
CMD ["app.py"]
ENTRYPOINT ["python3"]