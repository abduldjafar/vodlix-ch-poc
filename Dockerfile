FROM python:3.8.13
COPY req.txt .
COPY main.py .
RUN pip install -r req.txt
CMD [ "uvicorn", "main:app","--host", "0.0.0.0","--reload" ]

