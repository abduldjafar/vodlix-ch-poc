FROM python:3.8.13
COPY req.txt .
COPY main.py .
CMD [ "uvicorn", "main:app","--reload" ]

